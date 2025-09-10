package pubsub

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// SSEClient represents an individual SSE connection
type SSEClient struct {
	writer interface{}
	topics []string
}

// SSEManager manages SSE clients and their subscriptions
type SSEManager struct {
	pubsub             PubSub
	clients            sync.Map                // clientID -> *SSEClient
	topicClients       sync.Map                // topic -> []string (clientIDs)
	events             atomic.Value            // <-chan Event (thread-safe channel replacement)
	subscriptionCtx    context.Context         // Context for current subscription
	subscriptionCancel context.CancelFunc     // Cancel function for current subscription
	ctx                context.Context
	cancel             context.CancelFunc
}

// NewSSEManager creates a new SSE manager
func NewSSEManager(ctx context.Context, pubsub PubSub) *SSEManager {
	managerCtx, cancel := context.WithCancel(ctx)
	
	manager := &SSEManager{
		pubsub: pubsub,
		ctx:    managerCtx,
		cancel: cancel,
	}
	
	// Start broadcast loop immediately
	go manager.broadcastLoop()
	
	return manager
}

// RegisterClient registers an SSE client for multiple topics and returns a clientID
func (s *SSEManager) RegisterClient(topics []string, writer interface{}) string {
	clientID := fmt.Sprintf("sse_%d_%p", time.Now().UnixNano(), writer)
	
	client := &SSEClient{
		writer: writer,
		topics: topics,
	}
	
	// Store client
	s.clients.Store(clientID, client)
	
	// Add client to each topic's list
	for _, topic := range topics {
		s.addClientToTopic(topic, clientID)
	}
	
	log.Printf("SSEManager: Registered client %s for topics: %v", clientID, topics)
	
	// Update global subscription
	s.updateSubscriptions()
	
	return clientID
}

// DeregisterClient removes an SSE client and cleans up all subscriptions
func (s *SSEManager) DeregisterClient(clientID string) error {
	clientVal, exists := s.clients.Load(clientID)
	if !exists {
		return nil // Already removed
	}
	
	client := clientVal.(*SSEClient)
	
	// Remove client from all topic lists
	for _, topic := range client.topics {
		s.removeClientFromTopic(topic, clientID)
	}
	
	// Remove client
	s.clients.Delete(clientID)
	
	log.Printf("SSEManager: Deregistered client %s", clientID)
	
	// Update global subscription
	s.updateSubscriptions()
	
	return nil
}

// addClientToTopic adds a client ID to a topic's subscriber list (thread-safe)
func (s *SSEManager) addClientToTopic(topic, clientID string) {
	val, _ := s.topicClients.LoadOrStore(topic, []string{})
	clientIDs := val.([]string)
	clientIDs = append(clientIDs, clientID)
	s.topicClients.Store(topic, clientIDs)
}

// removeClientFromTopic removes a client ID from a topic's subscriber list (thread-safe)
func (s *SSEManager) removeClientFromTopic(topic, clientID string) {
	val, exists := s.topicClients.Load(topic)
	if !exists {
		return
	}
	
	clientIDs := val.([]string)
	for i, id := range clientIDs {
		if id == clientID {
			// Remove client from slice
			clientIDs = append(clientIDs[:i], clientIDs[i+1:]...)
			break
		}
	}
	
	if len(clientIDs) == 0 {
		s.topicClients.Delete(topic)
	} else {
		s.topicClients.Store(topic, clientIDs)
	}
}

// updateSubscriptions rebuilds the subscription list based on current clients
func (s *SSEManager) updateSubscriptions() {
	if s.ctx == nil {
		return // Not started yet
	}
	
	// Build topic list from current clients
	var topics []string
	s.topicClients.Range(func(key, value interface{}) bool {
		topic := key.(string)
		clientIDs := value.([]string)
		if len(clientIDs) > 0 {
			topics = append(topics, topic)
		}
		return true
	})
	
	log.Printf("SSEManager: updateSubscriptions called, active topics: %v", topics)
	
	// Update subscription
	if len(topics) > 0 {
		// Cancel old subscription first (triggers ChannelPubSub cleanup)
		if s.subscriptionCancel != nil {
			log.Printf("SSEManager: Cancelling old subscription")
			s.subscriptionCancel()
		}
		
		// Create new context for new subscription
		s.subscriptionCtx, s.subscriptionCancel = context.WithCancel(s.ctx)
		
		// Subscribe with new context
		if events, err := s.pubsub.Subscribe(s.subscriptionCtx, topics); err != nil {
			log.Printf("SSEManager: Failed to update subscriptions: %v", err)
		} else {
			log.Printf("SSEManager: Successfully subscribed to %d topics", len(topics))
			s.events.Store(events)
		}
	}
}


// broadcastLoop distributes events to SSE clients
func (s *SSEManager) broadcastLoop() {
	for {
		// Get current events channel safely with atomic.Value
		eventsVal := s.events.Load()
		if eventsVal == nil {
			// No subscription yet, wait for first client
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		
		events := eventsVal.(<-chan Event)
		select {
		case event, ok := <-events:
			if !ok {
				// Channel closed, wait for new subscription
				continue
			}
			s.broadcastToClients(event)
		case <-s.ctx.Done():
			return
		}
	}
}

// broadcastToClients sends an event to all registered SSE clients for the topic
func (s *SSEManager) broadcastToClients(event Event) {
	val, exists := s.topicClients.Load(event.Topic)
	if !exists {
		return // No clients for this topic
	}
	
	clientIDs := val.([]string)
	log.Printf("SSEManager: Broadcasting event %s to %d clients for topic %s", event.Member, len(clientIDs), event.Topic)
	
	// Broadcast to all clients for this topic
	sentCount := 0
	for _, clientID := range clientIDs {
		if clientVal, exists := s.clients.Load(clientID); exists {
			client := clientVal.(*SSEClient)
			if writer, ok := client.writer.(interface{ Write([]byte) (int, error); Flush() error }); ok {
				var data string
				if event.Score > 0 {
					data = fmt.Sprintf("event: %s\ndata: %s\nid: %.0f\n\n", event.Topic, event.Member, event.Score)
				} else {
					data = fmt.Sprintf("event: %s\ndata: %s\n\n", event.Topic, event.Member)
				}
				if _, err := writer.Write([]byte(data)); err == nil {
					writer.Flush()
					sentCount++
				} else {
					log.Printf("SSEManager: Failed to write to client %s: %v", clientID, err)
				}
			}
		}
	}
	
	log.Printf("SSEManager: Successfully sent event to %d/%d clients", sentCount, len(clientIDs))
}

// Stop stops the SSE manager
func (s *SSEManager) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}