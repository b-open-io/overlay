package publish

import "context"

// Publisher interface for publishing events
type Publisher interface {
	Publish(ctx context.Context, topic string, data string) error
}