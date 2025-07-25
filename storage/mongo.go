package storage

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoStorage struct {
	DB        *mongo.Database
	BeefStore beef.BeefStorage
	pub       publish.Publisher
}

func NewMongoStorage(connString string, dbName string, beefStore beef.BeefStorage, pub publish.Publisher) (*MongoStorage, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(connString))
	if err != nil {
		return nil, err
	}
	db := client.Database(dbName)

	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "outpoint", Value: 1},
			{Key: "topic", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	if _, err = db.Collection("outputs").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}

	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{Key: "txid", Value: 1},
		},
	}
	if _, err = db.Collection("outputs").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}

	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{Key: "topic", Value: 1},
			{Key: "blockHeight", Value: 1},
		},
	}
	if _, err = db.Collection("outputs").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}

	// Index for score-based queries in FindUTXOsForTopic
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{Key: "topic", Value: 1},
			{Key: "score", Value: 1},
			{Key: "spent", Value: 1},
		},
	}
	if _, err = db.Collection("outputs").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}

	// Index for interactions collection
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{Key: "host", Value: 1},
			{Key: "topic", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	if _, err = db.Collection("interactions").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}

	return &MongoStorage{
		DB:        db,
		BeefStore: beefStore,
		pub:       pub,
	}, nil
}

func (s *MongoStorage) InsertOutput(ctx context.Context, utxo *engine.Output) (err error) {
	if err := s.BeefStore.SaveBeef(ctx, &utxo.Outpoint.Txid, utxo.Beef); err != nil {
		return err
	}

	bo := NewBSONOutput(utxo)
	// Insert or update the output in the "outputs" collection
	if _, err = s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": utxo.Outpoint.String(), "topic": utxo.Topic},
		bson.M{"$set": bo},
		options.UpdateOne().SetUpsert(true),
	); err != nil {
		return err
	}

	if s.pub != nil {
		s.pub.Publish(ctx, utxo.Topic, base64.StdEncoding.EncodeToString(utxo.Beef))
	}

	return nil
}

func (s *MongoStorage) FindOutput(ctx context.Context, outpoint *transaction.Outpoint, topic *string, spent *bool, includeBEEF bool) (o *engine.Output, err error) {
	query := bson.M{"outpoint": outpoint.String()}
	if topic != nil {
		query["topic"] = *topic
	}
	if spent != nil {
		query["spent"] = *spent
	}

	bo := &BSONOutput{}
	if err := s.DB.Collection("outputs").FindOne(ctx, query).Decode(bo); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil // No output found
		}
		return nil, err // An error occurred
	}
	o = bo.ToEngineOutput()
	if includeBEEF {
		if o.Beef, err = s.BeefStore.LoadBeef(ctx, &outpoint.Txid); err != nil {
			return nil, err
		}
	}

	return o, nil
}

func (s *MongoStorage) FindOutputs(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
	ops := make([]string, 0, len(outpoints))
	for _, outpoint := range outpoints {
		ops = append(ops, outpoint.String())
	}
	query := bson.M{"topic": topic, "outpoint": bson.M{"$in": ops}}

	resultsByOutpoint := make(map[string]*engine.Output)
	if cursor, err := s.DB.Collection("outputs").Find(ctx, query); err != nil {
		return nil, err // An error occurred while querying the outputs
	} else {
		defer cursor.Close(ctx) // Ensure the cursor is closed after use
		for cursor.Next(ctx) {
			var result BSONOutput
			if err := cursor.Decode(&result); err != nil {
				return nil, err // An error occurred while decoding the output
			}
			output := result.ToEngineOutput()
			if includeBEEF {
				if output.Beef, err = s.BeefStore.LoadBeef(ctx, &output.Outpoint.Txid); err != nil {
					return nil, err
				}
			}
			resultsByOutpoint[result.Outpoint] = output // Store the output by its outpoint
			// outputs = append(outputs, output)
		}
		if err := cursor.Err(); err != nil {
			return nil, err // An error occurred while iterating through the cursor
		}
		var outputs []*engine.Output
		for _, outpoint := range outpoints {
			outputs = append(outputs, resultsByOutpoint[outpoint.String()])
		}
		return outputs, nil // Return the list of outputs found
	}
}

func (s *MongoStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
	query := bson.M{"txid": txid.String()}

	if cursor, err := s.DB.Collection("outputs").Find(ctx, query); err != nil {
		return nil, err // An error occurred while querying the outputs
	} else {
		defer cursor.Close(ctx) // Ensure the cursor is closed after use
		var outputs []*engine.Output
		for cursor.Next(ctx) {
			var result BSONOutput
			if err := cursor.Decode(&result); err != nil {
				return nil, err // An error occurred while decoding the output
			}
			output := result.ToEngineOutput()
			if includeBEEF {
				if output.Beef, err = s.BeefStore.LoadBeef(ctx, &output.Outpoint.Txid); err != nil {
					return nil, err
				}
			}
			outputs = append(outputs, output)
		}
		if err := cursor.Err(); err != nil {
			return nil, err // An error occurred while iterating through the cursor
		}
		return outputs, nil // Return the list of outputs found for the transaction
	}
}

func (s *MongoStorage) FindUTXOsForTopic(ctx context.Context, topic string, since float64, limit uint32, includeBEEF bool) ([]*engine.Output, error) {
	query := bson.M{
		"topic": topic,
		"score": bson.M{"$gte": since},
		"spent": false, // Ensure only unspent outputs are retrieved
	}
	findOpts := options.Find().SetSort(bson.M{"score": 1})
	if limit > 0 {
		findOpts.SetLimit(int64(limit))
	}
	if cursor, err := s.DB.Collection("outputs").Find(ctx, query, findOpts); err != nil {
		return nil, err // An error occurred while querying the outputs
	} else {
		defer cursor.Close(ctx) // Ensure the cursor is closed after use
		var outputs []*engine.Output
		for cursor.Next(ctx) {
			var result BSONOutput
			if err := cursor.Decode(&result); err != nil {
				return nil, err // An error occurred while decoding the output
			}
			output := result.ToEngineOutput()
			if includeBEEF {
				if output.Beef, err = s.BeefStore.LoadBeef(ctx, &output.Outpoint.Txid); err != nil {
					return nil, err
				}
			}
			outputs = append(outputs, output)
		}
		if err := cursor.Err(); err != nil {
			return nil, err // An error occurred while iterating through the cursor
		}
		return outputs, nil // Return the list of outputs found for the transaction
	}
}

func (s *MongoStorage) DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint, topic string) error {
	_, err := s.DB.Collection("outputs").DeleteOne(ctx, bson.M{
		"outpoint": outpoint.String(),
		"topic":    topic,
	})
	return err
}

func (s *MongoStorage) MarkUTXOAsSpent(ctx context.Context, outpoint *transaction.Outpoint, topic string, beef []byte) error {
	_, err := s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": outpoint.String(), "topic": topic},
		bson.M{"$set": bson.M{"spent": true}},
	)
	return err
}

func (s *MongoStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spendTxid *chainhash.Hash) error {
	ops := make([]string, 0, len(outpoints))
	for _, outpoint := range outpoints {
		ops = append(ops, outpoint.String())
	}
	_, err := s.DB.Collection("outputs").UpdateMany(ctx,
		bson.M{"outpoint": bson.M{"$in": ops}, "topic": topic},
		bson.M{"$set": bson.M{"spent": true}},
	)
	return err
}

func (s *MongoStorage) UpdateConsumedBy(ctx context.Context, outpoint *transaction.Outpoint, topic string, consumedBy []*transaction.Outpoint) error {
	ops := make([]string, 0, len(consumedBy))
	for _, outpoint := range consumedBy {
		ops = append(ops, outpoint.String())
	}
	_, err := s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": outpoint.String(), "topic": topic},
		bson.M{"$addToSet": bson.M{"consumedBy": bson.M{"$each": ops}}},
	)
	return err
}

func (s *MongoStorage) UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error {
	return s.BeefStore.SaveBeef(ctx, txid, beef)
}

func (s *MongoStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *transaction.Outpoint, topic string, blockHeight uint32, blockIndex uint64, ancelliaryBeef []byte) error {
	_, err := s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": outpoint.String(), "topic": topic},
		bson.M{"$set": bson.M{
			"blockHeight":   blockHeight,
			"blockIdx":      blockIndex,
			"ancillaryBeef": ancelliaryBeef,
		}},
	)
	return err
}

func (s *MongoStorage) InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error {
	_, err := s.DB.Collection("tx-topics").UpdateOne(ctx,
		bson.M{"_id": tx.Txid.String()},
		bson.M{
			"$addToSet":    bson.M{"topics": tx.Topic},
			"$setOnInsert": bson.M{"firstSeen": time.Now().UnixMilli()},
		},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

func (s *MongoStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	if err := s.DB.Collection("tx-topics").FindOne(ctx, bson.M{"_id": tx.Txid.String(), "topics": tx.Topic}).Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil // Transaction does not exist
		}
		return false, err // An error occurred
	}

	return true, nil
}

func (s *MongoStorage) UpdateLastInteraction(ctx context.Context, host string, topic string, since float64) error {
	_, err := s.DB.Collection("interactions").UpdateOne(ctx,
		bson.M{"host": host, "topic": topic},
		bson.M{"$set": bson.M{
			"host":      host,
			"topic":     topic,
			"score":     since,
			"updatedAt": time.Now(),
		}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

func (s *MongoStorage) GetLastInteraction(ctx context.Context, host string, topic string) (float64, error) {
	var result struct {
		Score float64 `bson:"score"`
	}
	err := s.DB.Collection("interactions").FindOne(ctx, bson.M{"host": host, "topic": topic}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil // No record exists, return 0 as specified
		}
		return 0, err
	}
	return result.Score, nil
}
