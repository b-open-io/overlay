package pubsub

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

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
	testCtx := context.Background()
	if err := redisClient.Ping(testCtx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	
	// Create internal context for lifecycle management
	ctx, cancel := context.WithCancel(context.Background())
	
	return &RedisPubSub{
		redisClient: redisClient,
		events:      make(chan Event, 1000),
		ctx:         ctx,
		cancel:      cancel,
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
	
	// If already subscribed, close old subscription and create new one
	if r.pubsub != nil {
		r.pubsub.Close()
	}
	
	// Create new subscription with updated topic list
	r.pubsub = r.redisClient.Subscribe(ctx, topics...)
	
	// Start listen loop if not already running
	if r.events == nil {
		r.events = make(chan Event, 1000)
		go r.listenLoop()
	}
	
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
			var score float64 = 0 // Default to 0 (no score)
			
			if colonIndex := strings.Index(msg.Payload, ":"); colonIndex > 0 {
				// Check if first part is a valid score
				if parsedScore, err := strconv.ParseFloat(msg.Payload[:colonIndex], 64); err == nil {
					score = parsedScore
					member = msg.Payload[colonIndex+1:]
				} else {
					// Not a score, treat entire payload as member
					member = msg.Payload
				}
			} else {
				// No colon, entire payload is member
				member = msg.Payload
			}
			
			// Create event with parsed data
			event := Event{
				Topic:  msg.Channel,
				Member: member,
				Score:  score, // 0 if no score provided
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