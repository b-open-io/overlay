package pubsub

import (
	"context"
	"sync"
)

// ChannelPubSub implements the PubSub interface using Go channels
// This provides a no-dependency pub/sub solution for SQLite-based deployments
// Recent events are handled by the storage layer via LookupOutpoints
type ChannelPubSub struct {
	subscribers map[string][]chan Event // topic -> list of subscriber channels
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewChannelPubSub creates a new channel-based pub/sub implementation
func NewChannelPubSub() *ChannelPubSub {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &ChannelPubSub{
		subscribers: make(map[string][]chan Event),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Publish sends data to all subscribers of a topic
func (cp *ChannelPubSub) Publish(ctx context.Context, topic string, data string, score ...float64) error {
	cp.mu.RLock()
	subscribers := cp.subscribers[topic]
	cp.mu.RUnlock()
	
	// Use provided score or 0 if none provided
	var eventScore float64 = 0
	if len(score) > 0 {
		eventScore = score[0]
	}
	
	// Create event for real-time notification
	event := Event{
		Topic:  topic,
		Member: data, // Store data as member (usually txid string)
		Score:  eventScore,
		Source: "channels",
	}
	
	// Send to all current subscribers
	for _, ch := range subscribers {
		select {
		case ch <- event:
			// Successfully sent
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Channel is full, skip this subscriber to avoid blocking
		}
	}
	
	return nil
}

// Subscribe creates a subscription to the given topics
func (cp *ChannelPubSub) Subscribe(ctx context.Context, topics []string) (<-chan Event, error) {
	eventChan := make(chan Event, 100) // Buffered channel to avoid blocking publishers
	
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	// Add subscriber to each topic
	for _, topic := range topics {
		cp.subscribers[topic] = append(cp.subscribers[topic], eventChan)
	}
	
	// Start cleanup goroutine for this subscription
	go func() {
		<-ctx.Done()
		cp.unsubscribeChannel(eventChan, topics)
		close(eventChan)
	}()
	
	return eventChan, nil
}

// Unsubscribe removes subscriptions (for compatibility)
func (cp *ChannelPubSub) Unsubscribe(topics []string) error {
	// This is handled automatically when the context is cancelled in Subscribe
	// For manual unsubscribe, we'd need to track channels per subscription
	return nil
}

// unsubscribeChannel removes a specific channel from topic subscriptions
func (cp *ChannelPubSub) unsubscribeChannel(eventChan chan Event, topics []string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	for _, topic := range topics {
		subscribers := cp.subscribers[topic]
		for i, ch := range subscribers {
			if ch == eventChan {
				// Remove this channel from the slice
				cp.subscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
				break
			}
		}
		
		// Clean up empty topic subscriptions
		if len(cp.subscribers[topic]) == 0 {
			delete(cp.subscribers, topic)
		}
	}
}

// Start initializes the pub/sub system
func (cp *ChannelPubSub) Start(ctx context.Context) error {
	// Channel-based pub/sub doesn't need explicit startup
	return nil
}

// Stop stops the pub/sub system
func (cp *ChannelPubSub) Stop() error {
	cp.cancel()
	return nil
}

// Close closes the pub/sub system
func (cp *ChannelPubSub) Close() error {
	cp.cancel()
	
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	// Close all subscriber channels
	for _, subscribers := range cp.subscribers {
		for _, ch := range subscribers {
			close(ch)
		}
	}
	
	// Clear all data
	cp.subscribers = make(map[string][]chan Event)
	
	return nil
}