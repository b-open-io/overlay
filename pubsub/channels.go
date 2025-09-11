package pubsub

import (
	"context"
	"log"
	"sync"
)

// channelSubscription represents a single subscriber's context and channel
type channelSubscription struct {
	ctx     context.Context
	channel chan Event
	topics  []string
}

// ChannelPubSub implements the PubSub interface using Go channels
// This provides a no-dependency pub/sub solution for SQLite-based deployments
// Recent events are handled by the storage layer via LookupOutpoints
type ChannelPubSub struct {
	subscribers sync.Map // topic -> []*channelSubscription
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewChannelPubSub creates a new channel-based pub/sub implementation
func NewChannelPubSub() *ChannelPubSub {
	ctx, cancel := context.WithCancel(context.Background())

	return &ChannelPubSub{
		ctx:    ctx,
		cancel: cancel,
	}
}

// Publish sends data to all subscribers of a topic
func (cp *ChannelPubSub) Publish(ctx context.Context, topic string, data string, score ...float64) error {
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
	sentCount := 0
	if subs, ok := cp.subscribers.Load(topic); ok {
		subscriptions := subs.([]*channelSubscription)
		for _, sub := range subscriptions {
			select {
			case sub.channel <- event:
				sentCount++
			case <-ctx.Done():
				return ctx.Err()
			default:
				log.Printf("ChannelPubSub: Skipping full channel for topic %s", topic)
			}
		}
	}

	return nil
}

// Subscribe creates a subscription to the given topics
func (cp *ChannelPubSub) Subscribe(ctx context.Context, topics []string) (<-chan Event, error) {
	eventChan := make(chan Event, 100) // Buffered channel to avoid blocking publishers

	// Create subscription
	sub := &channelSubscription{
		ctx:     ctx,
		channel: eventChan,
		topics:  topics,
	}

	// Add subscriber to each topic
	for _, topic := range topics {
		var subs []*channelSubscription
		if existing, ok := cp.subscribers.Load(topic); ok {
			subs = existing.([]*channelSubscription)
		}
		subs = append(subs, sub)
		cp.subscribers.Store(topic, subs)
	}

	// Start cleanup goroutine for this subscription
	go func() {
		<-ctx.Done()
		cp.unsubscribeSubscription(sub)
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

// unsubscribeSubscription removes a specific subscription from topic subscriptions
func (cp *ChannelPubSub) unsubscribeSubscription(targetSub *channelSubscription) {
	for _, topic := range targetSub.topics {
		if subs, ok := cp.subscribers.Load(topic); ok {
			subscriptions := subs.([]*channelSubscription)
			
			// Remove this subscription from the slice
			var newSubs []*channelSubscription
			for _, sub := range subscriptions {
				if sub != targetSub {
					newSubs = append(newSubs, sub)
				}
			}
			
			// Update or delete the topic
			if len(newSubs) == 0 {
				cp.subscribers.Delete(topic)
			} else {
				cp.subscribers.Store(topic, newSubs)
			}
		}
	}
}

// Stop stops the pub/sub system
func (cp *ChannelPubSub) Stop() error {
	cp.cancel()
	return nil
}

// Close closes the pub/sub system
func (cp *ChannelPubSub) Close() error {
	cp.cancel()

	// Close all subscriber channels
	cp.subscribers.Range(func(key, value any) bool {
		subscriptions := value.([]*channelSubscription)
		for _, sub := range subscriptions {
			close(sub.channel)
		}
		return true
	})

	return nil
}
