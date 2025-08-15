package publish

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type RedisPublish struct {
	DB *redis.Client
}

// NewRedisPublish creates a Redis publisher
func NewRedisPublish(connString string) (*RedisPublish, error) {
	log.Println("Connecting to Redis Publisher...", connString)
	opts, err := redis.ParseURL(connString)
	if err != nil {
		return nil, err
	}
	
	return &RedisPublish{
		DB: redis.NewClient(opts),
	}, nil
}

// Publish sends a message to a topic
func (r *RedisPublish) Publish(ctx context.Context, topic string, data string) error {
	_, err := r.DB.Publish(ctx, topic, data).Result()
	return err
}