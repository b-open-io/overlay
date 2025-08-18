package publish

import "context"

// EventData represents a buffered event for reconnection
type EventData struct {
	Outpoint string  `json:"outpoint"`
	Score    float64 `json:"score"`
}

// Publisher interface for publishing events
type Publisher interface {
	Publish(ctx context.Context, topic string, data string) error
	GetRecentEvents(ctx context.Context, topic string, since float64) ([]EventData, error)
}