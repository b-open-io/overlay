package publish

import "context"

// Message represents a pub/sub message
type Message struct {
	Channel string
	Pattern string  // For pattern subscriptions
	Payload string
}

// PubSub interface combines publishing and subscription capabilities
type PubSub interface {
	// Publishing
	Publish(ctx context.Context, topic string, data string) error
	
	// Subscribing
	Subscribe(ctx context.Context, channels ...string) (<-chan Message, error)
	PSubscribe(ctx context.Context, patterns ...string) (<-chan Message, error)
	Unsubscribe(ctx context.Context, channels ...string) error
	PUnsubscribe(ctx context.Context, patterns ...string) error
	Close() error
}

// Publisher is a legacy interface for backward compatibility
// New code should use PubSub
type Publisher interface {
	Publish(ctx context.Context, topic string, data string) error
}
