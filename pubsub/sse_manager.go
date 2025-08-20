package pubsub

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// SSEManager manages SSE clients and their subscriptions
type SSEManager struct {
	pubsub  PubSub
	clients map[string][]interface{} // topic -> slice of SSE clients
	mu      sync.RWMutex
	events  <-chan Event
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewSSEManager creates a new SSE manager
func NewSSEManager(ctx context.Context, pubsub PubSub) *SSEManager {
	managerCtx, cancel := context.WithCancel(ctx)
	
	manager := &SSEManager{
		pubsub:  pubsub,
		clients: make(map[string][]interface{}),
		ctx:     managerCtx,
		cancel:  cancel,
	}
	
	// Start broadcast loop immediately
	go manager.broadcastLoop()
	
	return manager
}

// AddSSEClient registers an SSE client for a topic and updates subscriptions
func (s *SSEManager) AddSSEClient(topic string, client interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Add client to topic
	s.clients[topic] = append(s.clients[topic], client)
	
	// Rebuild subscription list dynamically
	s.updateSubscriptions()
	
	return nil
}

// RemoveSSEClient unregisters an SSE client and updates subscriptions
func (s *SSEManager) RemoveSSEClient(topic string, client interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Remove client from topic
	clients := s.clients[topic]
	for i, c := range clients {
		if c == client {
			s.clients[topic] = append(clients[:i], clients[i+1:]...)
			break
		}
	}
	
	// Clean up empty topics
	if len(s.clients[topic]) == 0 {
		delete(s.clients, topic)
	}
	
	// Rebuild subscription list dynamically
	s.updateSubscriptions()
	
	return nil
}

// updateSubscriptions rebuilds the subscription list based on current clients
func (s *SSEManager) updateSubscriptions() {
	if s.ctx == nil {
		return // Not started yet
	}
	
	// Build topic list from current clients
	topics := make([]string, 0, len(s.clients))
	for topic := range s.clients {
		if len(s.clients[topic]) > 0 {
			topics = append(topics, topic)
		}
	}
	
	// Update subscription
	if len(topics) > 0 {
		if events, err := s.pubsub.Subscribe(s.ctx, topics); err != nil {
			log.Printf("Failed to update SSE subscriptions: %v", err)
		} else {
			s.events = events
		}
	}
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
			var data string
			if event.Score > 0 {
				data = fmt.Sprintf("data: %s\nid: %.0f\n\n", event.Member, event.Score)
			} else {
				data = fmt.Sprintf("data: %s\n\n", event.Member)
			}
			if _, err := writer.Write([]byte(data)); err == nil {
				writer.Flush()
			}
		}
	}
}

// Stop stops the SSE manager
func (s *SSEManager) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}