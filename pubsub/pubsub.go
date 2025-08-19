package pubsub

import (
	"context"
	"fmt"
	"sync"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// Event represents a unified event that can come from Redis or SSE sources
type Event struct {
	Topic  string         `json:"topic"`
	Member string         `json:"member"`  // Generic member data (usually outpoint string)
	Score  float64        `json:"score"`
	Source string         `json:"source"` // "redis", "sse:peerURL", etc.
	Data   map[string]any `json:"data,omitempty"` // Additional event data
}

// EventData represents a buffered event for reconnection (backward compatibility)
type EventData struct {
	Outpoint string  `json:"outpoint"`
	Score    float64 `json:"score"`
}

// PubSub interface for unified publishing and subscribing
type PubSub interface {
	// Publishing functionality
	Publish(ctx context.Context, topic string, data string) error

	// Subscribing functionality
	Subscribe(ctx context.Context, topics []string) (<-chan Event, error)
	Unsubscribe(topics []string) error

	// Connection management
	Start(ctx context.Context) error
	Stop() error
	Close() error
}

// SubscriberConfig configures different types of subscribers
type SubscriberConfig struct {
	// Redis configuration
	RedisURL string

	// SSE peer configuration  
	SSEPeers map[string][]string // peer URL -> topics

	// Engine for transaction processing
	Engine interface {
		Submit(ctx context.Context, taggedBEEF any, mode any, onSteakReady any) (any, error)
		GetStorage() Storage
	}
}

// Storage interface for checking transaction existence
type Storage interface {
	DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error)
}

// TransactionFetcher interface for fetching BEEF from peers
type TransactionFetcher interface {
	FetchBEEF(ctx context.Context, peerURL, topic string, outpoint transaction.Outpoint) ([]byte, error)
}

// SSEManager provides SSE client management on top of any PubSub implementation
type SSEManager struct {
	pubsub    PubSub
	clients   map[string][]interface{} // topic -> slice of SSE clients
	mu        sync.RWMutex
	events    <-chan Event
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewSSEManager creates a new SSE manager wrapping the given PubSub implementation
func NewSSEManager(pubsub PubSub) *SSEManager {
	return &SSEManager{
		pubsub:  pubsub,
		clients: make(map[string][]interface{}),
	}
}

// AddSSEClient registers an SSE client for a specific topic
func (s *SSEManager) AddSSEClient(topic string, client interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.clients[topic] = append(s.clients[topic], client)
	return nil
}

// RemoveSSEClient unregisters an SSE client from a topic
func (s *SSEManager) RemoveSSEClient(topic string, client interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	clients := s.clients[topic]
	for i, c := range clients {
		if c == client {
			s.clients[topic] = append(clients[:i], clients[i+1:]...)
			break
		}
	}
	
	return nil
}

// Start begins listening for events and broadcasting to SSE clients
func (s *SSEManager) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	
	// Start the underlying PubSub
	if err := s.pubsub.Start(ctx); err != nil {
		return err
	}
	
	// Get all topics that have clients
	s.mu.RLock()
	topics := make([]string, 0, len(s.clients))
	for topic := range s.clients {
		topics = append(topics, topic)
	}
	s.mu.RUnlock()
	
	if len(topics) > 0 {
		// Subscribe to topics
		events, err := s.pubsub.Subscribe(ctx, topics)
		if err != nil {
			return err
		}
		s.events = events
		
		// Start broadcasting goroutine
		go s.broadcastLoop()
	}
	
	return nil
}

// broadcastLoop distributes events to SSE clients
func (s *SSEManager) broadcastLoop() {
	for {
		select {
		case event := <-s.events:
			s.broadcastToClients(event)
		case <-s.ctx.Done():
			return
		}
	}
}

// broadcastToClients sends an event to all registered SSE clients for the topic
func (s *SSEManager) broadcastToClients(event Event) {
	s.mu.RLock()
	clients := s.clients[event.Topic]
	s.mu.RUnlock()
	
	// Broadcast to all clients for this topic
	for _, client := range clients {
		if writer, ok := client.(interface{ Write([]byte) (int, error); Flush() error }); ok {
			// Write SSE format: data: <member>\nid: <score>\n\n
			data := fmt.Sprintf("data: %s\nid: %.0f\n\n", event.Member, event.Score)
			if _, err := writer.Write([]byte(data)); err == nil {
				writer.Flush()
			}
		}
	}
}

// Stop stops the SSE manager and underlying PubSub
func (s *SSEManager) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return s.pubsub.Stop()
}

// Close closes the SSE manager and underlying PubSub
func (s *SSEManager) Close() error {
	s.Stop()
	return s.pubsub.Close()
}

// Expose the underlying PubSub methods
func (s *SSEManager) Publish(ctx context.Context, topic string, data string) error {
	return s.pubsub.Publish(ctx, topic, data)
}