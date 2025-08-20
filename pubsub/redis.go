package pubsub

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisPubSub handles both publishing and subscribing to Redis
type RedisPubSub struct {
	redisClient *redis.Client
	pubsub      *redis.PubSub
	events      chan Event
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
}

// NewRedisPubSub creates a new Redis pub/sub handler
func NewRedisPubSub(redisURL string) (*RedisPubSub, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}
	
	redisClient := redis.NewClient(opts)
	
	// Test connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	
	return &RedisPubSub{
		redisClient: redisClient,
		events:      make(chan Event, 1000),
	}, nil
}

// Publish publishes an event to Redis
func (r *RedisPubSub) Publish(ctx context.Context, topic string, data string, score ...float64) error {
	// Encode score in the message: {score}:{data}
	message := data
	if len(score) > 0 {
		message = fmt.Sprintf("%.0f:%s", score[0], data)
	}
	return r.redisClient.Publish(ctx, topic, message).Err()
}

// Subscribe subscribes to multiple topics and returns a channel of events
func (r *RedisPubSub) Subscribe(ctx context.Context, topics []string) (<-chan Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.pubsub != nil {
		return nil, fmt.Errorf("already subscribed")
	}
	
	r.pubsub = r.redisClient.Subscribe(ctx, topics...)
	
	go r.listenLoop()
	
	return r.events, nil
}

// listenLoop processes Redis pub/sub messages and converts them to Event objects
func (r *RedisPubSub) listenLoop() {
	defer close(r.events)
	
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg := <-r.pubsub.Channel():
			// Parse score from message payload: {score}:{data} or just {data}
			var member string
			var score float64
			
			if colonIndex := strings.Index(msg.Payload, ":"); colonIndex > 0 {
				// Score encoded in message
				if parsedScore, err := strconv.ParseFloat(msg.Payload[:colonIndex], 64); err == nil {
					score = parsedScore
					member = msg.Payload[colonIndex+1:]
				} else {
					// Fallback if score parsing fails
					member = msg.Payload
					score = float64(time.Now().UnixNano())
				}
			} else {
				// No score encoded, use current timestamp
				member = msg.Payload
				score = float64(time.Now().UnixNano())
			}
			
			// Create event with parsed data
			event := Event{
				Topic:  msg.Channel,
				Member: member,
				Score:  score,
				Source: "redis",
			}
			
			select {
			case r.events <- event:
			case <-r.ctx.Done():
				return
			}
		}
	}
}

// Unsubscribe unsubscribes from topics
func (r *RedisPubSub) Unsubscribe(topics []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.pubsub == nil {
		return fmt.Errorf("not subscribed")
	}
	
	return r.pubsub.Unsubscribe(r.ctx, topics...)
}

// Start starts the Redis pub/sub system
func (r *RedisPubSub) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.ctx, r.cancel = context.WithCancel(ctx)
	return nil
}

// Stop stops the Redis pub/sub system
func (r *RedisPubSub) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	if r.cancel != nil {
		r.cancel()
	}
	
	if r.pubsub != nil {
		return r.pubsub.Close()
	}
	
	return nil
}

// Close closes the Redis connection
func (r *RedisPubSub) Close() error {
	return r.redisClient.Close()
}