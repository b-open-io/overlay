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
	topicClients       map[string][]string     // topic -> []string (clientIDs)
	topicMutex         sync.RWMutex            // Protects topicClients map
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
		pubsub:       pubsub,
		topicClients: make(map[string][]string),
		ctx:          managerCtx,
		cancel:       cancel,
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
	s.topicMutex.Lock()
	defer s.topicMutex.Unlock()

	if s.topicClients[topic] == nil {
		s.topicClients[topic] = []string{}
	}
	s.topicClients[topic] = append(s.topicClients[topic], clientID)
}

// removeClientFromTopic removes a client ID from a topic's subscriber list (thread-safe)
func (s *SSEManager) removeClientFromTopic(topic, clientID string) {
	s.topicMutex.Lock()
	defer s.topicMutex.Unlock()

	clients := s.topicClients[topic]
	for i, id := range clients {
		if id == clientID {
			s.topicClients[topic] = append(clients[:i], clients[i+1:]...)
			break
		}
	}

	if len(s.topicClients[topic]) == 0 {
		delete(s.topicClients, topic)
	}
}

// updateSubscriptions rebuilds the subscription list based on current clients
func (s *SSEManager) updateSubscriptions() {
	if s.ctx == nil {
		return // Not started yet
	}
	
	// Build topic list from current clients
	s.topicMutex.RLock()
	var topics []string
	for topic, clientIDs := range s.topicClients {
		if len(clientIDs) > 0 {
			topics = append(topics, topic)
		}
	}
	s.topicMutex.RUnlock()
	
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
	s.topicMutex.RLock()
	clientIDs, exists := s.topicClients[event.Topic]
	if !exists {
		s.topicMutex.RUnlock()
		return // No clients for this topic
	}

	// Make a copy to avoid holding the lock during broadcast
	clientIDsCopy := make([]string, len(clientIDs))
	copy(clientIDsCopy, clientIDs)
	s.topicMutex.RUnlock()
	
	// Broadcast to all clients for this topic
	sentCount := 0
	var disconnectedClients []string
	
	for _, clientID := range clientIDsCopy {
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
					if flushErr := writer.Flush(); flushErr == nil {
						sentCount++
					} else {
						log.Printf("SSEManager: Failed to flush to client %s: %v", clientID, flushErr)
						disconnectedClients = append(disconnectedClients, clientID)
					}
				} else {
					log.Printf("SSEManager: Failed to write to client %s: %v", clientID, err)
					disconnectedClients = append(disconnectedClients, clientID)
				}
			}
		}
	}
	
	// Clean up disconnected clients
	for _, clientID := range disconnectedClients {
		s.DeregisterClient(clientID)
	}
	
}

// Stop stops the SSE manager
func (s *SSEManager) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}