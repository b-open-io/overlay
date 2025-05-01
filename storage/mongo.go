package storage

import (
	"context"

	"github.com/4chain-ag/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoStorage struct {
	DB     *mongo.Database
	bucket *mongo.GridFSBucket
}

func NewMongoStorage(connString string, dbName string) (*MongoStorage, error) {
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

	return &MongoStorage{DB: db, bucket: db.GridFSBucket()}, nil
}

func (s *MongoStorage) InsertOutput(ctx context.Context, utxo *engine.Output) (err error) {
	// Delete existing GridFS file for this txid (if any)
	if _, err = s.DB.Collection("fs.files").DeleteOne(ctx, bson.M{"metadata.txid": utxo.Outpoint.Txid.String()}); err != nil {
		return err
	}

	// Upload the new Beef data to GridFS
	var beefFileID interface{}
	if len(utxo.Beef) > 0 {
		uploadStream, err := s.bucket.OpenUploadStreamWithID(
			ctx,
			utxo.Outpoint.Txid.String(), // Use txid as the file ID
			"beef",
			options.GridFSUpload().SetMetadata(bson.M{"txid": utxo.Outpoint.Txid.String()}),
		)
		if err != nil {
			return err
		}
		defer uploadStream.Close()

		if _, err = uploadStream.Write(utxo.Beef); err != nil {
			return err
		}
		beefFileID = uploadStream.FileID
	}

	bo := NewBSONOutput(utxo)
	bo.BeefFileId = beefFileID.(string) // Store the GridFS file ID in the BSONOutput
	// Insert or update the output in the "outputs" collection
	if _, err = s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": utxo.Outpoint.String(), "topic": utxo.Topic},
		bson.M{"$set": bo},
		options.UpdateOne().SetUpsert(true),
	); err != nil {
		return err
	}

	// // Update the "beefs" collection with the GridFS file ID
	// if _, err = s.DB.Collection("beefs").UpdateOne(ctx,
	// 	bson.M{"_id": utxo.Outpoint.Txid.String()},
	// 	bson.M{"$set": bson.M{"beefFileID": beefFileID}},
	// 	options.Update().SetUpsert(true),
	// ); err != nil {
	// 	return err
	// }

	return nil
}

func (s *MongoStorage) FindOutput(ctx context.Context, outpoint *overlay.Outpoint, topic *string, spent *bool, includeBEEF bool) (o *engine.Output, err error) {
	query := bson.M{"outpoint": outpoint.String()}
	if topic != nil {
		query["topic"] = *topic
	}
	if spent != nil {
		query["spent"] = *spent
	}

	// Aggregation pipeline
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: query}}, // Match the output
	}

	if includeBEEF {
		// Add a $lookup stage to join with the "beef" collection
		pipeline = append(pipeline, bson.D{
			{Key: "$lookup", Value: bson.M{
				"from":         "beef",
				"localField":   "txid",
				"foreignField": "_id", // Match on the unique _id field in the beef collection
				"as":           "beefData",
			}},
		})
		// Add a $set stage to directly assign the Beef field
		pipeline = append(pipeline, bson.D{
			{Key: "$set", Value: bson.M{
				"beef": bson.M{"$arrayElemAt": []interface{}{"$beefData.beef", 0}},
			}},
		})
	}

	// Execute the aggregation
	cursor, err := s.DB.Collection("outputs").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Decode the result
	if cursor.Next(ctx) {
		var result BSONOutput
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}

		// Convert BSONOutput to engine.Output
		o = result.ToEngineOutput()
	} else {
		return nil, nil // No output found
	}

	return o, nil
}

func (s *MongoStorage) FindOutputs(ctx context.Context, outpoints []*overlay.Outpoint, topic *string, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
	outputs := make([]*engine.Output, 0, len(outpoints))
	for _, outpoint := range outpoints {
		if output, err := s.FindOutput(ctx, outpoint, topic, spent, includeBEEF); err != nil {
			return nil, err
		} else {
			outputs = append(outputs, output)
		}
	}
	return outputs, nil
}

func (s *MongoStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
	query := bson.M{"txid": txid.String()}

	// Aggregation pipeline
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: query}}, // Match outputs with the given txid
	}

	if includeBEEF {
		// Add a $lookup stage to join with the "beef" collection
		pipeline = append(pipeline, bson.D{
			{Key: "$lookup", Value: bson.M{
				"from":         "beef",
				"localField":   "txid",
				"foreignField": "_id", // Match on the unique _id field in the beef collection
				"as":           "beefData",
			}},
		})
		// Add a $set stage to directly assign the Beef field
		pipeline = append(pipeline, bson.D{
			{Key: "$set", Value: bson.M{
				"beef": bson.M{"$arrayElemAt": []interface{}{"$beefData.beef", 0}},
			}},
		})
	}

	// Execute the aggregation
	cursor, err := s.DB.Collection("outputs").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Decode the results
	var outputs []*engine.Output
	for cursor.Next(ctx) {
		var result BSONOutput
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		outputs = append(outputs, result.ToEngineOutput())
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return outputs, nil
}

func (s *MongoStorage) FindUTXOsForTopic(ctx context.Context, topic string, since uint32, includeBEEF bool) ([]*engine.Output, error) {
	query := bson.M{
		"topic":       topic,
		"blockHeight": bson.M{"$gte": since},
		"spent":       false, // Ensure only unspent outputs are retrieved
	}

	// Aggregation pipeline
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: query}}, // Match UTXOs for the given topic and blockHeight
	}

	if includeBEEF {
		// Add a $lookup stage to join with the "beef" collection
		pipeline = append(pipeline, bson.D{
			{Key: "$lookup", Value: bson.M{
				"from":         "beef",
				"localField":   "txid",
				"foreignField": "_id", // Match on the unique _id field in the beef collection
				"as":           "beefData",
			}},
		})
		// Add a $set stage to directly assign the Beef field
		pipeline = append(pipeline, bson.D{
			{Key: "$set", Value: bson.M{
				"beef": bson.M{"$arrayElemAt": []interface{}{"$beefData.beef", 0}},
			}},
		})
	}

	// Execute the aggregation
	cursor, err := s.DB.Collection("outputs").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Decode the results
	var outputs []*engine.Output
	for cursor.Next(ctx) {
		var result BSONOutput
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		outputs = append(outputs, result.ToEngineOutput())
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return outputs, nil
}

func (s *MongoStorage) DeleteOutput(ctx context.Context, outpoint *overlay.Outpoint, topic string) error {
	_, err := s.DB.Collection("outputs").DeleteOne(ctx, bson.M{
		"outpoint": outpoint.String(),
		"topic":    topic,
	})
	return err
}

func (s *MongoStorage) MarkUTXOAsSpent(ctx context.Context, outpoint *overlay.Outpoint, topic string, spendTxid *chainhash.Hash) error {
	_, err := s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": outpoint.String(), "topic": topic},
		bson.M{"$set": bson.M{"spent": true}},
	)
	return err
}

func (s *MongoStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*overlay.Outpoint, topic string, spendTxid *chainhash.Hash) error {
	ops := make([]string, 0, len(outpoints))
	for _, outpoint := range outpoints {
		ops = append(ops, outpoint.String())
	}
	_, err := s.DB.Collection("outputs").UpdateMany(ctx,
		bson.M{"outpoint": bson.M{"$in": ops, "topic": topic}},
		bson.M{"$set": bson.M{"spent": true}},
	)
	return err
}

func (s *MongoStorage) UpdateConsumedBy(ctx context.Context, outpoint *overlay.Outpoint, topic string, consumedBy []*overlay.Outpoint) error {
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
	_, err := s.DB.Collection("beefs").UpdateOne(ctx,
		bson.M{"_id": txid.String()},
		bson.M{"$set": bson.M{"beef": beef}},
	)
	return err
}

func (s *MongoStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *overlay.Outpoint, topic string, blockHeight uint32, blockIndex uint64, ancelliaryBeef []byte) error {
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
	_, err := s.DB.Collection("beefs").UpdateOne(ctx,
		bson.M{"_id": tx.Txid.String()},
		bson.M{"$addToSet": bson.M{"topics": tx.Topic}},
	)
	return err
}

func (s *MongoStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	if err := s.DB.Collection("beefs").FindOne(ctx, bson.M{"_id": tx.Txid.String(), "topics": tx.Topic}).Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil // Transaction does not exist
		}
		return false, err // An error occurred
	}

	return true, nil
}
