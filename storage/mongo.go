package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

type MongoEventDataStorage struct {
	DB        *mongo.Database
	BeefStore beef.BeefStorage
	pub       publish.Publisher
}

// GetBeefStorage returns the underlying BEEF storage implementation
func (s *MongoEventDataStorage) GetBeefStorage() beef.BeefStorage {
	return s.BeefStore
}

func NewMongoEventDataStorage(connString string, beefStore beef.BeefStorage, pub publish.Publisher) (*MongoEventDataStorage, error) {
	// Parse the connection string to extract database name
	clientOpts := options.Client().ApplyURI(connString)
	
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, err
	}
	
	// Extract database name from the connection string
	// MongoDB connection strings can include the database as: mongodb://host/database
	dbName := "overlay" // default
	if cs, err := connstring.ParseAndValidate(connString); err == nil && cs.Database != "" {
		dbName = cs.Database
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

	return &MongoEventDataStorage{
		DB:        db,
		BeefStore: beefStore,
		pub:       pub,
	}, nil
}

func (s *MongoEventDataStorage) InsertOutput(ctx context.Context, utxo *engine.Output) (err error) {
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

func (s *MongoEventDataStorage) FindOutput(ctx context.Context, outpoint *transaction.Outpoint, topic *string, spent *bool, includeBEEF bool) (o *engine.Output, err error) {
	query := bson.M{"outpoint": outpoint.String()}
	if topic != nil {
		query["topic"] = *topic
	}
	if spent != nil {
		if *spent {
			// If looking for spent outputs, spend field must not be nil
			query["spend"] = bson.M{"$ne": nil}
		} else {
			// If looking for unspent outputs, spend field must be nil
			query["spend"] = nil
		}
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

func (s *MongoEventDataStorage) FindOutputs(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
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

func (s *MongoEventDataStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
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

func (s *MongoEventDataStorage) FindUTXOsForTopic(ctx context.Context, topic string, since float64, limit uint32, includeBEEF bool) ([]*engine.Output, error) {
	query := bson.M{
		"topic": topic,
		"score": bson.M{"$gte": since},
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

func (s *MongoEventDataStorage) DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint, topic string) error {
	_, err := s.DB.Collection("outputs").DeleteOne(ctx, bson.M{
		"outpoint": outpoint.String(),
		"topic":    topic,
	})
	return err
}

func (s *MongoEventDataStorage) MarkUTXOAsSpent(ctx context.Context, outpoint *transaction.Outpoint, topic string, beef []byte) error {
	// Parse the beef to get the spending txid
	_, _, spendTxid, err := transaction.ParseBeef(beef)
	if err != nil {
		return err
	}

	_, err = s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": outpoint.String(), "topic": topic},
		bson.M{"$set": bson.M{"spend": spendTxid.String()}},
	)
	return err
}

func (s *MongoEventDataStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spendTxid *chainhash.Hash) error {
	ops := make([]string, 0, len(outpoints))
	for _, outpoint := range outpoints {
		ops = append(ops, outpoint.String())
	}
	_, err := s.DB.Collection("outputs").UpdateMany(ctx,
		bson.M{"outpoint": bson.M{"$in": ops}, "topic": topic},
		bson.M{"$set": bson.M{"spend": spendTxid.String()}},
	)
	return err
}

func (s *MongoEventDataStorage) UpdateConsumedBy(ctx context.Context, outpoint *transaction.Outpoint, topic string, consumedBy []*transaction.Outpoint) error {
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

func (s *MongoEventDataStorage) UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error {
	return s.BeefStore.SaveBeef(ctx, txid, beef)
}

func (s *MongoEventDataStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *transaction.Outpoint, topic string, blockHeight uint32, blockIndex uint64, ancelliaryBeef []byte) error {
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

func (s *MongoEventDataStorage) InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error {
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

func (s *MongoEventDataStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	if err := s.DB.Collection("tx-topics").FindOne(ctx, bson.M{"_id": tx.Txid.String(), "topics": tx.Topic}).Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil // Transaction does not exist
		}
		return false, err // An error occurred
	}

	return true, nil
}

func (s *MongoEventDataStorage) UpdateLastInteraction(ctx context.Context, host string, topic string, since float64) error {
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

func (s *MongoEventDataStorage) GetLastInteraction(ctx context.Context, host string, topic string) (float64, error) {
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

// GetTransactionsByTopicAndHeight returns all transactions for a topic at a specific block height
func (s *MongoEventDataStorage) GetTransactionsByTopicAndHeight(ctx context.Context, topic string, height uint32) ([]*TransactionData, error) {
	collection := s.DB.Collection("outputs")

	// Find all outputs for this topic at the specified height
	pipeline := bson.A{
		// Match outputs for the topic at the specified block height
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "topic", Value: topic},
			{Key: "blockHeight", Value: height},
		}}},

		// Group by transaction ID to collect all outputs for each transaction
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$txid"},
			{Key: "outputs", Value: bson.D{{Key: "$push", Value: "$$ROOT"}}},
		}}},

		// Sort by transaction ID for consistent ordering
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var transactions []*TransactionData

	for cursor.Next(ctx) {
		var result struct {
			TxID    string       `bson:"_id"`
			Outputs []BSONOutput `bson:"outputs"`
		}

		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}

		txid, err := chainhash.NewHashFromHex(result.TxID)
		if err != nil {
			return nil, err
		}

		txData := &TransactionData{
			TxID:    *txid,
			Outputs: make([]*OutputData, 0, len(result.Outputs)),
		}

		// Convert outputs
		for _, output := range result.Outputs {
			// Parse outpoint to get vout
			outpoint, err := transaction.OutpointFromString(output.Outpoint)
			if err != nil {
				continue
			}

			outputData := &OutputData{
				Vout:     outpoint.Index,
				Data:     output.Data,
				Script:   output.Script,
				Satoshis: output.Satoshis,
			}
			txData.Outputs = append(txData.Outputs, outputData)
		}

		// Find inputs for this transaction using aggregation
		inputPipeline := bson.A{
			bson.D{{Key: "$match", Value: bson.D{
				{Key: "topic", Value: topic},
				{Key: "spend", Value: result.TxID},
			}}},
		}

		inputCursor, err := collection.Aggregate(ctx, inputPipeline)
		if err == nil {
			defer inputCursor.Close(ctx)

			txData.Inputs = make([]*OutputData, 0)
			for inputCursor.Next(ctx) {
				var inputOutput BSONOutput
				if err := inputCursor.Decode(&inputOutput); err != nil {
					continue
				}

				// Parse source txid
				sourceTxid, err := chainhash.NewHashFromHex(inputOutput.Txid)
				if err != nil {
					continue
				}

				// Parse outpoint to get vout
				inputOutpoint, err := transaction.OutpointFromString(inputOutput.Outpoint)
				if err != nil {
					continue
				}

				inputData := &OutputData{
					TxID:     sourceTxid,
					Vout:     inputOutpoint.Index,
					Data:     inputOutput.Data,
					Script:   inputOutput.Script,
					Satoshis: inputOutput.Satoshis,
				}
				txData.Inputs = append(txData.Inputs, inputData)
			}
		}

		transactions = append(transactions, txData)
	}

	return transactions, nil
}

// SaveEvents associates multiple events with a single output, storing arbitrary data
func (s *MongoEventDataStorage) SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, height uint32, idx uint64, data interface{}) error {
	if len(events) == 0 {
		return nil
	}

	var score float64
	if height > 0 {
		score = float64(height) + float64(idx)/1e9
	} else {
		score = float64(time.Now().Unix())
	}

	// Update the output document with events, score, and data
	update := bson.M{
		"$set": bson.M{
			"events": events,
			"score":  score,
		},
	}

	if data != nil {
		update["$set"].(bson.M)["data"] = data
	}

	_, err := s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": outpoint.String()},
		update,
	)
	return err
}

// FindEvents returns all events associated with a given outpoint
func (s *MongoEventDataStorage) FindEvents(ctx context.Context, outpoint *transaction.Outpoint) ([]string, error) {
	var result struct {
		Events []string `bson:"events"`
	}

	err := s.DB.Collection("outputs").FindOne(ctx,
		bson.M{"outpoint": outpoint.String()},
		options.FindOne().SetProjection(bson.M{"events": 1}),
	).Decode(&result)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return []string{}, nil
		}
		return nil, err
	}

	if result.Events == nil {
		return []string{}, nil
	}

	return result.Events, nil
}

// LookupOutpoints returns outpoints matching the given query criteria using aggregations for performance
func (s *MongoEventDataStorage) LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error) {
	withData := len(includeData) > 0 && includeData[0]

	// Build match criteria
	match := bson.M{}

	// Handle event filtering
	if question.Event != "" {
		match["events"] = question.Event
	} else if len(question.Events) > 0 {
		if question.JoinType == nil || *question.JoinType == JoinTypeUnion {
			// Union: match any of the events
			match["events"] = bson.M{"$in": question.Events}
		} else if *question.JoinType == JoinTypeIntersect {
			// Intersection: must have all events
			match["events"] = bson.M{"$all": question.Events}
		}
		// Note: Difference join type would need more complex aggregation
	}

	// Handle score range
	if question.From > 0 || question.Until > 0 {
		scoreMatch := bson.M{}
		if question.From > 0 {
			scoreMatch["$gte"] = question.From
		}
		if question.Until > 0 {
			scoreMatch["$lte"] = question.Until
		}
		match["score"] = scoreMatch
	}

	// Handle unspent filter
	if question.UnspentOnly {
		match["spend"] = nil
	}

	// Build aggregation pipeline
	pipeline := bson.A{
		bson.D{{Key: "$match", Value: match}},
	}

	// Sort by score
	sortOrder := 1
	if question.Reverse {
		sortOrder = -1
	}
	pipeline = append(pipeline, bson.D{{Key: "$sort", Value: bson.D{{Key: "score", Value: sortOrder}}}})

	// Limit results
	if question.Limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: question.Limit}})
	}

	// Project only needed fields
	projection := bson.M{
		"outpoint": 1,
		"score":    1,
	}
	if withData {
		projection["data"] = 1
	}
	pipeline = append(pipeline, bson.D{{Key: "$project", Value: projection}})

	cursor, err := s.DB.Collection("outputs").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []*OutpointResult
	for cursor.Next(ctx) {
		var doc struct {
			Outpoint string      `bson:"outpoint"`
			Score    float64     `bson:"score"`
			Data     interface{} `bson:"data,omitempty"`
		}

		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		outpoint, err := transaction.OutpointFromString(doc.Outpoint)
		if err != nil {
			continue
		}

		result := &OutpointResult{
			Outpoint: outpoint,
			Score:    doc.Score,
		}

		if withData && doc.Data != nil {
			// Convert BSON data to clean types like GetOutputData does
			jsonBytes, err := json.Marshal(doc.Data)
			if err == nil {
				var cleanData interface{}
				if err := json.Unmarshal(jsonBytes, &cleanData); err == nil {
					result.Data = cleanData
				}
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// GetOutputData retrieves the data associated with a specific output
func (s *MongoEventDataStorage) GetOutputData(ctx context.Context, outpoint *transaction.Outpoint) (interface{}, error) {
	var result struct {
		Data interface{} `bson:"data"`
	}

	err := s.DB.Collection("outputs").FindOne(ctx,
		bson.M{"outpoint": outpoint.String()},
		options.FindOne().SetProjection(bson.M{"data": 1}),
	).Decode(&result)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("outpoint not found")
		}
		return nil, err
	}

	if result.Data == nil {
		return nil, nil
	}

	// Convert through JSON to get clean types like Redis implementation
	jsonBytes, err := json.Marshal(result.Data)
	if err != nil {
		return nil, err
	}

	var cleanData interface{}
	if err := json.Unmarshal(jsonBytes, &cleanData); err != nil {
		return nil, err
	}

	return cleanData, nil
}
