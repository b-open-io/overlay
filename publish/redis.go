package publish

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisPublish struct {
	DB          *redis.Client
	BufferLimit int64 // Maximum number of events to buffer per topic
}

// NewRedisPublish creates a Redis publisher
func NewRedisPublish(connString string) (*RedisPublish, error) {
	log.Println("Connecting to Redis Publisher...", connString)
	opts, err := redis.ParseURL(connString)
	if err != nil {
		return nil, err
	}
	
	return &RedisPublish{
		DB:          redis.NewClient(opts),
		BufferLimit: 1000, // Default: keep 1000 recent events per topic
	}, nil
}

// Publish sends a message to a topic and automatically buffers it for reconnection
func (r *RedisPublish) Publish(ctx context.Context, topic string, data string) error {
	// Generate nanosecond timestamp score for event ordering
	score := float64(time.Now().UnixNano())
	
	// Use pipeline for atomic operations
	_, err := r.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		// Buffer the event in a capped sorted set
		bufferKey := fmt.Sprintf("events:%s", topic)
		
		// Add event to sorted set with nanosecond timestamp score
		if err := p.ZAdd(ctx, bufferKey, redis.Z{
			Score:  score,
			Member: data, // data should be the outpoint string
		}).Err(); err != nil {
			return err
		}
		
		// Maintain cap by removing oldest events (keep latest BufferLimit events)
		if err := p.ZRemRangeByRank(ctx, bufferKey, 0, -r.BufferLimit-1).Err(); err != nil {
			return err
		}
		
		// Publish the formatted message for SSE consumption (score:outpoint)
		message := fmt.Sprintf("%.0f:%s", score, data)
		return p.Publish(ctx, topic, message).Err()
	})
	
	return err
}

// GetRecentEvents retrieves buffered events since a given score
func (r *RedisPublish) GetRecentEvents(ctx context.Context, topic string, since float64) ([]EventData, error) {
	bufferKey := fmt.Sprintf("events:%s", topic)
	
	// Get all events with score greater than since
	results, err := r.DB.ZRangeByScoreWithScores(ctx, bufferKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("(%f", since), // Exclusive lower bound
		Max: "+inf",
	}).Result()
	
	if err != nil {
		return nil, err
	}
	
	events := make([]EventData, 0, len(results))
	for _, result := range results {
		outpoint, ok := result.Member.(string)
		if !ok {
			continue
		}
		
		events = append(events, EventData{
			Outpoint: outpoint,
			Score:    result.Score,
		})
	}
	
	return events, nil
}