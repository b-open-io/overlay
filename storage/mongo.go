package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
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
	BaseEventDataStorage
	DB *mongo.Database
}

// GetBeefStorage is inherited from BaseEventDataStorage

func NewMongoEventDataStorage(connString string, beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubsub pubsub.PubSub) (*MongoEventDataStorage, error) {
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

	// Compound index for sets SIsMember queries (_id + members)
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
			{Key: "members", Value: 1},
		},
	}
	if _, err = db.Collection("sets").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}

	// Unique index for sorted_sets key+member combination
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{Key: "key", Value: 1},
			{Key: "member", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	if _, err = db.Collection("sorted_sets").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}

	// Index for score-based range queries on sorted sets
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{Key: "key", Value: 1},
			{Key: "score", Value: 1},
		},
	}
	if _, err = db.Collection("sorted_sets").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}

	// Index for topic + events + score + spent queries (with topic filtering)
	indexModel = mongo.IndexModel{
		Keys: bson.D{
			{Key: "topic", Value: 1},
			{Key: "events", Value: 1},
			{Key: "score", Value: 1},
			{Key: "spent", Value: 1},
		},
	}
	if _, err = db.Collection("outputs").Indexes().CreateOne(context.TODO(), indexModel); err != nil {
		return nil, err
	}


	return &MongoEventDataStorage{
		BaseEventDataStorage: NewBaseEventDataStorage(beefStore, queueStorage, pubsub),
		DB:                   db,
	}, nil
}

func (s *MongoEventDataStorage) InsertOutput(ctx context.Context, utxo *engine.Output) (err error) {
	if err := s.beefStore.SaveBeef(ctx, &utxo.Outpoint.Txid, utxo.Beef); err != nil {
		return err
	}

	bo := NewBSONOutput(utxo)
	// Insert or update the output in the "outputs" collection
	update := bson.M{
		"$set":      bo,
		"$addToSet": bson.M{"events": utxo.Topic}, // Add topic as an event
	}
	
	if _, err = s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": utxo.Outpoint.String(), "topic": utxo.Topic},
		update,
		options.UpdateOne().SetUpsert(true),
	); err != nil {
		return err
	}

	// Manually publish topic event to pubsub since we're not using SaveEvents
	if s.pubsub != nil {
		if err := s.pubsub.Publish(ctx, utxo.Topic, utxo.Outpoint.String()); err != nil {
			log.Printf("Failed to publish topic event: %v", err)
		}
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
		if o.Beef, err = s.beefStore.LoadBeef(ctx, &outpoint.Txid); err != nil {
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
				if output.Beef, err = s.beefStore.LoadBeef(ctx, &output.Outpoint.Txid); err != nil {
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
				if output.Beef, err = s.beefStore.LoadBeef(ctx, &output.Outpoint.Txid); err != nil {
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
				if output.Beef, err = s.beefStore.LoadBeef(ctx, &output.Outpoint.Txid); err != nil {
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
	return s.beefStore.SaveBeef(ctx, txid, beef)
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
	score := float64(time.Now().UnixMilli())
	
	_, err := s.DB.Collection("tx-topics").UpdateOne(ctx,
		bson.M{"_id": tx.Txid.String()},
		bson.M{
			"$addToSet":    bson.M{"topics": tx.Topic},
			"$setOnInsert": bson.M{"firstSeen": score},
		},
		options.UpdateOne().SetUpsert(true),
	)
	
	if err != nil {
		return err
	}
	
	// Publish transaction to topic via PubSub (if available)
	if s.pubsub != nil {
		// For topic events (tm_*), publish the txid with the score
		if err := s.pubsub.Publish(ctx, tx.Topic, tx.Txid.String(), score); err != nil {
			// Log error but don't fail the transaction insertion
			log.Printf("Failed to publish transaction to topic %s: %v", tx.Topic, err)
		}
	}
	
	return nil
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

// GetTransactionByTopic returns a single transaction for a topic by txid
func (s *MongoEventDataStorage) GetTransactionByTopic(ctx context.Context, topic string, txid *chainhash.Hash, includeBeef ...bool) (*TransactionData, error) {
	collection := s.DB.Collection("outputs")

	// Find all outputs for this specific transaction and topic
	pipeline := bson.A{
		// Match outputs for the topic and transaction
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "topic", Value: topic},
			{Key: "txid", Value: txid.String()},
		}}},
		
		// Group by transaction ID and collect outputs
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$txid"},
			{Key: "outputs", Value: bson.D{{Key: "$push", Value: bson.D{
				{Key: "outpoint", Value: "$outpoint"},
				{Key: "vout", Value: "$vout"},
				{Key: "script", Value: "$script"},
				{Key: "satoshis", Value: "$satoshis"},
				{Key: "spend", Value: "$spend"},
				{Key: "data", Value: "$data"},
			}}}},
		}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var result struct {
		TxID    string `bson:"_id"`
		Outputs []struct {
			Outpoint string      `bson:"outpoint"`
			Vout     uint32      `bson:"vout"`
			Script   []byte      `bson:"script"`
			Satoshis uint64      `bson:"satoshis"`
			Spend    string      `bson:"spend,omitempty"`
			Data     interface{} `bson:"data,omitempty"`
		} `bson:"outputs"`
	}

	if !cursor.Next(ctx) {
		return nil, fmt.Errorf("transaction not found")
	}

	if err := cursor.Decode(&result); err != nil {
		return nil, err
	}

	// Build TransactionData
	txData := &TransactionData{
		TxID:    *txid,
		Outputs: make([]*OutputData, 0, len(result.Outputs)),
	}

	// Convert outputs
	for _, output := range result.Outputs {
		// Parse spend if exists
		var spend *chainhash.Hash
		if output.Spend != "" {
			if spendHash, err := chainhash.NewHashFromHex(output.Spend); err == nil {
				spend = spendHash
			}
		}

		outputData := &OutputData{
			TxID:     txid,
			Vout:     output.Vout,
			Data:     output.Data,
			Script:   output.Script,
			Satoshis: output.Satoshis,
			Spend:    spend,
		}

		txData.Outputs = append(txData.Outputs, outputData)
	}

	// Get inputs for this transaction
	inputPipeline := bson.A{
		bson.D{{Key: "$match", Value: bson.D{
			{Key: "spend", Value: txid.String()},
		}}},
	}

	inputCursor, err := collection.Aggregate(ctx, inputPipeline)
	if err != nil {
		return nil, err
	}
	defer inputCursor.Close(ctx)

	txData.Inputs = make([]*OutputData, 0)

	for inputCursor.Next(ctx) {
		var input struct {
			Outpoint string      `bson:"outpoint"`
			TxID     string      `bson:"txid"`
			Vout     uint32      `bson:"vout"`
			Script   []byte      `bson:"script"`
			Satoshis uint64      `bson:"satoshis"`
			Data     interface{} `bson:"data,omitempty"`
		}

		if err := inputCursor.Decode(&input); err != nil {
			continue
		}

		// Parse source txid
		sourceTxid, err := chainhash.NewHashFromHex(input.TxID)
		if err != nil {
			continue
		}

		inputData := &OutputData{
			TxID:     sourceTxid,
			Vout:     input.Vout,
			Data:     input.Data,
			Script:   input.Script,
			Satoshis: input.Satoshis,
		}

		txData.Inputs = append(txData.Inputs, inputData)
	}

	// Load BEEF if requested
	if len(includeBeef) > 0 && includeBeef[0] {
		beef, err := s.LoadBeefByTxidAndTopic(ctx, txid, topic)
		if err != nil {
			// Log but don't fail - BEEF is optional
		} else {
			txData.Beef = beef
		}
	}

	return txData, nil
}

// SaveEvents associates multiple events with a single output, storing arbitrary data
func (s *MongoEventDataStorage) SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, topic string, height uint32, idx uint64, data interface{}) error {
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
		"$addToSet": bson.M{
			"events": bson.M{"$each": events},
		},
		"$set": bson.M{
			"score": score,
		},
	}

	if data != nil {
		update["$set"].(bson.M)["data"] = data
	}

	_, err := s.DB.Collection("outputs").UpdateOne(ctx,
		bson.M{"outpoint": outpoint.String(), "topic": topic},
		update,
	)
	if err != nil {
		return err
	}

	// Publish events to pubsub for real-time notifications
	if s.pubsub != nil {
		outpointStr := outpoint.String()
		for _, event := range events {
			if err := s.pubsub.Publish(ctx, event, outpointStr); err != nil {
				// Log error but don't fail the operation - publishing is best-effort
				continue
			}
		}
	}

	return nil
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

	// Handle topic filtering
	if question.Topic != "" {
		match["topic"] = question.Topic
	}

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

// LoadBeefByTxidAndTopic loads merged BEEF for a transaction within a topic context
func (s *MongoEventDataStorage) LoadBeefByTxidAndTopic(ctx context.Context, txid *chainhash.Hash, topic string) ([]byte, error) {
	// Find any output for this txid in the specified topic
	var result struct {
		AncillaryBeef []byte `bson:"ancillaryBeef"`
	}
	
	err := s.DB.Collection("outputs").FindOne(ctx, bson.M{
		"txid":  txid.String(),
		"topic": topic,
	}).Decode(&result)
	
	if err != nil {
		return nil, fmt.Errorf("transaction %s not found in topic %s: %w", txid.String(), topic, err)
	}
	
	// Get BEEF from beef storage
	beefBytes, err := s.beefStore.LoadBeef(ctx, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to load BEEF: %w", err)
	}
	
	// Parse the main BEEF
	beef, _, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse main BEEF: %w", err)
	}
	
	// Merge AncillaryBeef if present (field is optional)
	if len(result.AncillaryBeef) > 0 {
		if err := beef.MergeBeefBytes(result.AncillaryBeef); err != nil {
			return nil, fmt.Errorf("failed to merge AncillaryBeef: %w", err)
		}
	}
	
	// Get atomic BEEF bytes for the specific transaction
	completeBeef, err := beef.AtomicBytes(txid)
	if err != nil {
		return nil, fmt.Errorf("failed to generate atomic BEEF: %w", err)
	}
	
	return completeBeef, nil
}

// FindOutputData returns outputs matching the given query criteria as OutputData objects
func (s *MongoEventDataStorage) FindOutputData(ctx context.Context, question *EventQuestion) ([]*OutputData, error) {
	// Build match pipeline
	matchStage := bson.M{}

	// Add topic filtering
	if question.Topic != "" {
		matchStage["topic"] = question.Topic
	}

	// Add event filtering
	if question.Event != "" {
		matchStage["events"] = question.Event
	} else if len(question.Events) > 0 {
		if question.JoinType != nil && *question.JoinType == JoinTypeIntersect {
			// For intersection, all events must be present
			matchStage["events"] = bson.M{"$all": question.Events}
		} else {
			// Default to union
			matchStage["events"] = bson.M{"$in": question.Events}
		}
	}

	// Add unspent filter
	if question.UnspentOnly {
		// If looking for unspent outputs, spend field must be nil
		matchStage["spend"] = nil
	}

	// Add score range filtering
	scoreFilter := bson.M{}
	if question.From > 0 {
		if question.Reverse {
			scoreFilter["$lte"] = question.From
		} else {
			scoreFilter["$gte"] = question.From
		}
	}
	if question.Until > 0 {
		if question.Reverse {
			scoreFilter["$gte"] = question.Until
		} else {
			scoreFilter["$lte"] = question.Until
		}
	}
	if len(scoreFilter) > 0 {
		matchStage["score"] = scoreFilter
	}

	// Build aggregation pipeline
	pipeline := []bson.M{
		{"$match": matchStage},
		{
			"$project": bson.M{
				"outpoint": 1,
				"vout":     1,
				"script":   1,
				"satoshis": 1,
				"data":     1,
				"score":    1,
				"spend":    1,
			},
		},
	}

	// Add sorting
	if question.Reverse {
		pipeline = append(pipeline, bson.M{"$sort": bson.M{"score": -1}})
	} else {
		pipeline = append(pipeline, bson.M{"$sort": bson.M{"score": 1}})
	}

	// Add limit
	if question.Limit > 0 {
		pipeline = append(pipeline, bson.M{"$limit": question.Limit})
	}

	cursor, err := s.DB.Collection("outputs").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []*OutputData
	for cursor.Next(ctx) {
		var doc struct {
			Outpoint string      `bson:"outpoint"`
			Vout     uint32      `bson:"vout"`
			Script   []byte      `bson:"script"`
			Satoshis uint64      `bson:"satoshis"`
			Data     interface{} `bson:"data"`
			Spend    *string     `bson:"spend"`
			Score    float64     `bson:"score"`
		}

		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		// Parse outpoint to get txid
		outpoint, err := transaction.OutpointFromString(doc.Outpoint)
		if err != nil {
			continue // Skip invalid outpoints
		}

		// Parse spending txid if present
		var spendTxid *chainhash.Hash
		if doc.Spend != nil && *doc.Spend != "" {
			if parsedSpendTxid, err := chainhash.NewHashFromHex(*doc.Spend); err == nil {
				spendTxid = parsedSpendTxid
			}
		}

		result := &OutputData{
			TxID:     &outpoint.Txid,
			Vout:     doc.Vout,
			Script:   doc.Script,
			Satoshis: doc.Satoshis,
			Spend:    spendTxid,
			Score:    doc.Score,
		}

		// Convert data through JSON to get clean types
		if doc.Data != nil {
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

	return results, cursor.Err()
}

// LookupEventScores returns lightweight event scores for simple queries
func (s *MongoEventDataStorage) LookupEventScores(ctx context.Context, topic string, event string, fromScore float64) ([]queue.ScoredMember, error) {
	// Query the outputs collection for this event without loading additional data
	filter := bson.M{
		"topic":  topic,
		"events": event,
		"score":  bson.M{"$gt": fromScore},
	}

	cursor, err := s.DB.Collection("outputs").Find(ctx, filter, 
		options.Find().SetSort(bson.M{"score": 1}).SetProjection(bson.M{"outpoint": 1, "score": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var members []queue.ScoredMember
	for cursor.Next(ctx) {
		var doc struct {
			Outpoint string  `bson:"outpoint"`
			Score    float64 `bson:"score"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		members = append(members, queue.ScoredMember{
			Member: doc.Outpoint,
			Score:  doc.Score,
		})
	}

	return members, cursor.Err()
}

// CountOutputs returns the total count of outputs in a given topic
func (s *MongoEventDataStorage) CountOutputs(ctx context.Context, topic string) (int64, error) {
	return s.DB.Collection("outputs").CountDocuments(ctx, bson.M{"topic": topic})
}

