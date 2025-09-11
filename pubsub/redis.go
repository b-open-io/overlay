package pubsub

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// subscription represents a single subscriber's context and channel
type redisSubscription struct {
	ctx     context.Context
	channel chan Event
	topics  []string
}

// RedisPubSub handles both publishing and subscribing to Redis
type RedisPubSub struct {
	redisClient      *redis.Client
	pubsub           *redis.PubSub
	subscribers      sync.Map // topic -> []*redisSubscription
	subscribedTopics sync.Map // topic -> bool
	ctx              context.Context
	cancel           context.CancelFunc
	listenStarted    int64 // atomic
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
	eventChan := make(chan Event, 100) // Buffered channel to avoid blocking publishers
	
	// Create subscription
	sub := &redisSubscription{
		ctx:     ctx,
		channel: eventChan,
		topics:  topics,
	}
	
	// Add subscriber to each topic
	for _, topic := range topics {
		var subs []*redisSubscription
		if existing, ok := r.subscribers.Load(topic); ok {
			subs = existing.([]*redisSubscription)
		}
		subs = append(subs, sub)
		r.subscribers.Store(topic, subs)
		
		// Subscribe to topic in Redis if not already subscribed
		if _, subscribed := r.subscribedTopics.Load(topic); !subscribed {
			r.subscribedTopics.Store(topic, true)
			if err := r.ensureRedisSubscription(topic); err != nil {
				return nil, fmt.Errorf("failed to subscribe to Redis topic %s: %w", topic, err)
			}
		}
	}
	
	// Start listen loop if not already running
	if atomic.CompareAndSwapInt64(&r.listenStarted, 0, 1) {
		go r.listenLoop()
	}
	
	// Start cleanup goroutine for this subscription
	go func() {
		<-ctx.Done()
		r.unsubscribeSubscription(sub)
		close(eventChan)
	}()
	
	return eventChan, nil
}

// ensureRedisSubscription ensures we're subscribed to a topic in Redis
func (r *RedisPubSub) ensureRedisSubscription(topic string) error {
	if r.pubsub == nil {
		r.pubsub = r.redisClient.Subscribe(r.ctx, topic)
	} else {
		if err := r.pubsub.Subscribe(r.ctx, topic); err != nil {
			return err
		}
	}
	return nil
}

// listenLoop processes Redis pub/sub messages and converts them to Event objects
func (r *RedisPubSub) listenLoop() {
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			if r.pubsub == nil {
				continue
			}
			
			msg := <-r.pubsub.Channel()
			if msg == nil {
				continue
			}
			
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
			
			// Send to all subscribers of this topic
			if subs, ok := r.subscribers.Load(msg.Channel); ok {
				subscriptions := subs.([]*redisSubscription)
				sentCount := 0
				for _, sub := range subscriptions {
					select {
					case sub.channel <- event:
						sentCount++
					case <-r.ctx.Done():
						return
					default:
						log.Printf("RedisPubSub: Skipping full channel for topic %s", msg.Channel)
					}
				}
			}
		}
	}
}

// unsubscribeSubscription removes a specific subscription from topic subscriptions
func (r *RedisPubSub) unsubscribeSubscription(targetSub *redisSubscription) {
	for _, topic := range targetSub.topics {
		if subs, ok := r.subscribers.Load(topic); ok {
			subscriptions := subs.([]*redisSubscription)
			
			// Remove this subscription from the slice
			var newSubs []*redisSubscription
			for _, sub := range subscriptions {
				if sub != targetSub {
					newSubs = append(newSubs, sub)
				}
			}
			
			// Update or delete the topic
			if len(newSubs) == 0 {
				r.subscribers.Delete(topic)
				r.subscribedTopics.Delete(topic)
				// Unsubscribe from Redis topic
				if r.pubsub != nil {
					r.pubsub.Unsubscribe(r.ctx, topic)
				}
			} else {
				r.subscribers.Store(topic, newSubs)
			}
		}
	}
}

// Unsubscribe removes subscriptions (for compatibility)
func (r *RedisPubSub) Unsubscribe(topics []string) error {
	// This is handled automatically when the context is cancelled in Subscribe
	// For manual unsubscribe, we'd need to track subscriptions per call
	return nil
}


// Stop stops the Redis pub/sub system
func (r *RedisPubSub) Stop() error {
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
	r.cancel()
	
	// Close all subscriber channels
	r.subscribers.Range(func(key, value any) bool {
		subscriptions := value.([]*redisSubscription)
		for _, sub := range subscriptions {
			close(sub.channel)
		}
		return true
	})
	
	if r.pubsub != nil {
		r.pubsub.Close()
	}
	
	return r.redisClient.Close()
}