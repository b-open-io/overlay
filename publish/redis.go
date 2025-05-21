package publish

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type RedisPublish struct {
	DB *redis.Client
}

func NewRedisPublish(connString string) (r *RedisPublish, err error) {
	r = &RedisPublish{}
	log.Println("Connecting to Redis Storage...", connString)
	if opts, err := redis.ParseURL(connString); err != nil {
		return nil, err
	} else {
		r.DB = redis.NewClient(opts)
		return r, nil
	}
}

func (r *RedisPublish) Publish(ctx context.Context, topic string, data string) error {
	_, err := r.DB.Publish(ctx, topic, data).Result()
	if err != nil {
		return err
	}
	return nil
}
