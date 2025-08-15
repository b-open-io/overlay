package publish

import (
	"context"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
)

type RedisPubSub struct {
	DB            *redis.Client
	subscriptions map[string]*redis.PubSub
	mu            sync.RWMutex
}

// NewRedisPublish creates a Redis publisher (legacy name for compatibility)
func NewRedisPublish(connString string) (*RedisPubSub, error) {
	return NewRedisPubSub(connString)
}

// NewRedisPubSub creates a Redis pub/sub client
func NewRedisPubSub(connString string) (*RedisPubSub, error) {
	log.Println("Connecting to Redis PubSub...", connString)
	opts, err := redis.ParseURL(connString)
	if err != nil {
		return nil, err
	}
	
	return &RedisPubSub{
		DB:            redis.NewClient(opts),
		subscriptions: make(map[string]*redis.PubSub),
	}, nil
}

// Publish sends a message to a topic
func (r *RedisPubSub) Publish(ctx context.Context, topic string, data string) error {
	_, err := r.DB.Publish(ctx, topic, data).Result()
	return err
}

// Subscribe subscribes to one or more channels
func (r *RedisPubSub) Subscribe(ctx context.Context, channels ...string) (<-chan Message, error) {
	pubsub := r.DB.Subscribe(ctx, channels...)
	
	// Store subscription for cleanup
	r.mu.Lock()
	for _, ch := range channels {
		r.subscriptions[ch] = pubsub
	}
	r.mu.Unlock()
	
	// Create message channel
	msgChan := make(chan Message, 100)
	
	// Start goroutine to forward messages
	go func() {
		defer close(msgChan)
		ch := pubsub.Channel()
		for msg := range ch {
			select {
			case msgChan <- Message{
				Channel: msg.Channel,
				Payload: msg.Payload,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	
	return msgChan, nil
}

// PSubscribe subscribes to one or more patterns
func (r *RedisPubSub) PSubscribe(ctx context.Context, patterns ...string) (<-chan Message, error) {
	pubsub := r.DB.PSubscribe(ctx, patterns...)
	
	// Store subscription for cleanup
	r.mu.Lock()
	for _, pattern := range patterns {
		r.subscriptions["pattern:"+pattern] = pubsub
	}
	r.mu.Unlock()
	
	// Create message channel
	msgChan := make(chan Message, 100)
	
	// Start goroutine to forward messages
	go func() {
		defer close(msgChan)
		ch := pubsub.Channel()
		for msg := range ch {
			select {
			case msgChan <- Message{
				Channel: msg.Channel,
				Pattern: msg.Pattern,
				Payload: msg.Payload,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	
	return msgChan, nil
}

// Unsubscribe unsubscribes from channels
func (r *RedisPubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	for _, ch := range channels {
		if pubsub, ok := r.subscriptions[ch]; ok {
			if err := pubsub.Unsubscribe(ctx, ch); err != nil {
				return err
			}
			delete(r.subscriptions, ch)
		}
	}
	return nil
}

// PUnsubscribe unsubscribes from patterns
func (r *RedisPubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	for _, pattern := range patterns {
		key := "pattern:" + pattern
		if pubsub, ok := r.subscriptions[key]; ok {
			if err := pubsub.PUnsubscribe(ctx, pattern); err != nil {
				return err
			}
			delete(r.subscriptions, key)
		}
	}
	return nil
}

// Close closes all subscriptions and the Redis connection
func (r *RedisPubSub) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Close all subscriptions
	for _, pubsub := range r.subscriptions {
		pubsub.Close()
	}
	r.subscriptions = make(map[string]*redis.PubSub)
	
	// Close Redis connection
	return r.DB.Close()
}
