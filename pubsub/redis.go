package pubsub

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisPubSub handles both publishing and subscribing to Redis
type RedisPubSub struct {
	redisClient     *redis.Client
	topicClients    map[string][]*bufio.Writer // Map of topic to connected SSE clients
	topicClientsMu  *sync.Mutex
	bufferLimit     int64 // Maximum number of events to buffer per topic
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
		redisClient:    redisClient,
		topicClients:   make(map[string][]*bufio.Writer),
		topicClientsMu: &sync.Mutex{},
		bufferLimit:    1000, // Default: keep 1000 recent events per topic
	}, nil
}

// Publish publishes an event to Redis and buffers it for reconnection
func (r *RedisPubSub) Publish(ctx context.Context, topic string, data string) error {
	// Generate nanosecond timestamp score for event ordering
	score := float64(time.Now().UnixNano())
	
	// Use pipeline for atomic operations
	_, err := r.redisClient.Pipelined(ctx, func(p redis.Pipeliner) error {
		// Buffer the event in a capped sorted set
		bufferKey := fmt.Sprintf("events:%s", topic)
		
		// Add event to sorted set with nanosecond timestamp score
		if err := p.ZAdd(ctx, bufferKey, redis.Z{
			Score:  score,
			Member: data, // data should be the outpoint string
		}).Err(); err != nil {
			return err
		}
		
		// Maintain cap by removing oldest events (keep latest bufferLimit events)
		if err := p.ZRemRangeByRank(ctx, bufferKey, 0, -r.bufferLimit-1).Err(); err != nil {
			return err
		}
		
		// Publish the formatted message for SSE consumption (score:outpoint)
		message := fmt.Sprintf("%.0f:%s", score, data)
		return p.Publish(ctx, topic, message).Err()
	})
	
	return err
}

// StartBroadcasting starts broadcasting Redis messages to connected SSE clients
func (r *RedisPubSub) StartBroadcasting(ctx context.Context) error {
	// Subscribe to all channels
	pubsub := r.redisClient.PSubscribe(ctx, "*")
	defer pubsub.Close()

	msgChan := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-msgChan:
			r.broadcastToClients(msg.Channel, msg.Payload)
		}
	}
}

// AddSSEClient adds an SSE client for a specific topic
func (r *RedisPubSub) AddSSEClient(topic string, client *bufio.Writer) {
	r.topicClientsMu.Lock()
	defer r.topicClientsMu.Unlock()
	r.topicClients[topic] = append(r.topicClients[topic], client)
}

// RemoveSSEClient removes an SSE client from a topic
func (r *RedisPubSub) RemoveSSEClient(topic string, client *bufio.Writer) {
	r.topicClientsMu.Lock()
	defer r.topicClientsMu.Unlock()
	
	clients := r.topicClients[topic]
	for i, c := range clients {
		if c == client {
			r.topicClients[topic] = append(clients[:i], clients[i+1:]...)
			break
		}
	}
}

// broadcastToClients sends Redis messages to connected SSE clients
func (r *RedisPubSub) broadcastToClients(channel, payload string) {
	slog.Debug("Broadcasting Redis message", "channel", channel, "payload", payload)
	
	r.topicClientsMu.Lock()
	defer r.topicClientsMu.Unlock()
	
	clients := r.topicClients[channel]
	var activeClients []*bufio.Writer
	
	for _, client := range clients {
		// Message format is "score:outpoint"
		parts := strings.SplitN(payload, ":", 2)
		if len(parts) == 2 {
			score := parts[0]
			outpoint := parts[1]

			// Write SSE format with event field for topic filtering
			_, err1 := fmt.Fprintf(client, "event: %s\n", channel)
			_, err2 := fmt.Fprintf(client, "data: %s\n", outpoint)
			_, err3 := fmt.Fprintf(client, "id: %s\n\n", score)
			flushErr := client.Flush()

			// Keep client if all operations succeeded
			if err1 == nil && err2 == nil && err3 == nil && flushErr == nil {
				activeClients = append(activeClients, client)
			}
		}
	}
	
	// Update with only active clients
	r.topicClients[channel] = activeClients
}

// GetRecentEvents retrieves buffered events since a given score
func (r *RedisPubSub) GetRecentEvents(ctx context.Context, topic string, since float64) ([]EventData, error) {
	bufferKey := fmt.Sprintf("events:%s", topic)
	
	// Get all events with score greater than since
	results, err := r.redisClient.ZRangeByScoreWithScores(ctx, bufferKey, &redis.ZRangeBy{
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

// Close closes the Redis connection
func (r *RedisPubSub) Close() error {
	return r.redisClient.Close()
}