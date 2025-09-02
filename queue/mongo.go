package queue

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

type MongoQueueStorage struct {
	db *mongo.Database
}

// Close disconnects from the MongoDB database
func (m *MongoQueueStorage) Close() error {
	if m.db != nil {
		return m.db.Client().Disconnect(context.Background())
	}
	return nil
}

func NewMongoQueueStorage(connString string) (*MongoQueueStorage, error) {
	// Parse the connection string to extract database name
	clientOpts := options.Client().ApplyURI(connString)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, err
	}

	// Extract database name from the connection string
	dbName := "overlay" // default
	if cs, err := connstring.ParseAndValidate(connString); err == nil && cs.Database != "" {
		dbName = cs.Database
	}

	db := client.Database(dbName)
	return &MongoQueueStorage{db: db}, nil
}

// Set Operations
func (s *MongoQueueStorage) SAdd(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	// Use $addToSet to add multiple members atomically, avoiding duplicates
	_, err := s.db.Collection("sets").UpdateOne(ctx,
		bson.M{"_id": key},
		bson.M{"$addToSet": bson.M{"members": bson.M{"$each": members}}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

func (s *MongoQueueStorage) SMembers(ctx context.Context, key string) ([]string, error) {
	var doc struct {
		Members []string `bson:"members"`
	}

	err := s.db.Collection("sets").FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return []string{}, nil // Return empty slice, not error
	}
	return doc.Members, err
}

func (s *MongoQueueStorage) SRem(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	_, err := s.db.Collection("sets").UpdateOne(ctx,
		bson.M{"_id": key},
		bson.M{"$pullAll": bson.M{"members": members}},
	)
	return err
}

func (s *MongoQueueStorage) SIsMember(ctx context.Context, key, member string) (bool, error) {
	count, err := s.db.Collection("sets").CountDocuments(ctx, bson.M{
		"_id":     key,
		"members": member,
	})
	return count > 0, err
}

// Hash Operations
func (s *MongoQueueStorage) HSet(ctx context.Context, key, field, value string) error {
	_, err := s.db.Collection("hashes").UpdateOne(ctx,
		bson.M{"_id": key},
		bson.M{"$set": bson.M{"fields." + field: value}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

func (s *MongoQueueStorage) HGet(ctx context.Context, key, field string) (string, error) {
	var doc bson.M
	err := s.db.Collection("hashes").FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return "", fmt.Errorf("redis: nil") // Mimic Redis nil error
	}
	if err != nil {
		return "", err
	}

	fields, ok := doc["fields"].(bson.M)
	if !ok {
		return "", fmt.Errorf("redis: nil")
	}

	value, exists := fields[field]
	if !exists {
		return "", fmt.Errorf("redis: nil")
	}

	return value.(string), nil
}

func (s *MongoQueueStorage) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	var doc struct {
		Fields map[string]string `bson:"fields"`
	}

	err := s.db.Collection("hashes").FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return make(map[string]string), nil // Return empty map, not error
	}
	if err != nil {
		return nil, err
	}

	if doc.Fields == nil {
		return make(map[string]string), nil
	}
	return doc.Fields, nil
}

func (s *MongoQueueStorage) HDel(ctx context.Context, key string, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}

	unsetFields := bson.M{}
	for _, field := range fields {
		unsetFields["fields."+field] = ""
	}

	_, err := s.db.Collection("hashes").UpdateOne(ctx,
		bson.M{"_id": key},
		bson.M{"$unset": unsetFields},
	)
	return err
}

// Sorted Set Operations
func (s *MongoQueueStorage) ZAdd(ctx context.Context, key string, members ...ScoredMember) error {
	if len(members) == 0 {
		return nil
	}

	var writes []mongo.WriteModel
	for _, member := range members {
		writes = append(writes, mongo.NewReplaceOneModel().
			SetFilter(bson.M{
				"key":    key,
				"member": member.Member,
			}).
			SetReplacement(bson.M{
				"key":    key,
				"member": member.Member,
				"score":  member.Score,
			}).
			SetUpsert(true))
	}

	_, err := s.db.Collection("sorted_sets").BulkWrite(ctx, writes)
	return err
}

func (s *MongoQueueStorage) ZRem(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	_, err := s.db.Collection("sorted_sets").DeleteMany(ctx, bson.M{
		"key":    key,
		"member": bson.M{"$in": members},
	})
	return err
}


func (s *MongoQueueStorage) ZRange(ctx context.Context, key string, scoreRange ScoreRange) ([]ScoredMember, error) {
	filter := bson.M{"key": key}
	
	// Add score range conditions if specified
	if scoreRange.Min != nil || scoreRange.Max != nil {
		scoreFilter := bson.M{}
		if scoreRange.Min != nil {
			scoreFilter["$gte"] = *scoreRange.Min
		}
		if scoreRange.Max != nil {
			scoreFilter["$lte"] = *scoreRange.Max
		}
		filter["score"] = scoreFilter
	}
	
	opts := options.Find().SetSort(bson.M{"score": 1})
	
	if scoreRange.Offset > 0 {
		opts.SetSkip(scoreRange.Offset)
	}
	
	if scoreRange.Count > 0 {
		opts.SetLimit(scoreRange.Count)
	}

	cursor, err := s.db.Collection("sorted_sets").Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var members []ScoredMember
	for cursor.Next(ctx) {
		var doc struct {
			Member string  `bson:"member"`
			Score  float64 `bson:"score"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		members = append(members, ScoredMember{
			Member: doc.Member,
			Score:  doc.Score,
		})
	}
	return members, cursor.Err()
}

func (s *MongoQueueStorage) ZScore(ctx context.Context, key, member string) (float64, error) {
	var doc struct {
		Score float64 `bson:"score"`
	}

	err := s.db.Collection("sorted_sets").FindOne(ctx, bson.M{
		"key":    key,
		"member": member,
	}).Decode(&doc)

	if err == mongo.ErrNoDocuments {
		return 0, fmt.Errorf("redis: nil") // Mimic Redis nil error
	}
	return doc.Score, err
}

func (s *MongoQueueStorage) ZCard(ctx context.Context, key string) (int64, error) {
	return s.db.Collection("sorted_sets").CountDocuments(ctx, bson.M{"key": key})
}

// Fee balance management
func (s *MongoQueueStorage) ZIncrBy(ctx context.Context, key, member string, increment float64) (float64, error) {
	collection := s.db.Collection("sorted_sets")

	filter := bson.M{"key": key, "member": member}
	update := bson.M{"$inc": bson.M{"score": increment}}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

	var result struct {
		Score float64 `bson:"score"`
	}
	err := collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	return result.Score, err
}

func (s *MongoQueueStorage) ZSum(ctx context.Context, key string) (float64, error) {
	pipeline := []bson.M{
		{"$match": bson.M{"key": key}},
		{"$group": bson.M{
			"_id": nil,
			"sum": bson.M{"$sum": "$score"},
		}},
	}

	cursor, err := s.db.Collection("sorted_sets").Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	if cursor.Next(ctx) {
		var result struct {
			Sum float64 `bson:"sum"`
		}
		if err := cursor.Decode(&result); err != nil {
			return 0, err
		}
		return result.Sum, nil
	}

	// No documents found, return 0
	return 0, nil
}