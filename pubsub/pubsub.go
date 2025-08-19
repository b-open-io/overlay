package pubsub

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// Event represents a unified event that can come from Redis or SSE sources
type Event struct {
	Topic    string             `json:"topic"`
	Outpoint transaction.Outpoint `json:"outpoint"`
	Score    float64            `json:"score"`
	Source   string             `json:"source"` // "redis", "sse:peerURL", etc.
	Data     map[string]any     `json:"data,omitempty"` // Additional event data
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
	GetRecentEvents(ctx context.Context, topic string, since float64) ([]EventData, error)

	// Subscribing functionality
	Subscribe(ctx context.Context, topics []string) (<-chan Event, error)
	Unsubscribe(topics []string) error

	// Connection management
	Start(ctx context.Context) error
	Stop() error
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
	DoesAppliedTransactionExist(ctx context.Context, txid *chainhash.Hash) (bool, error)
}

// TransactionFetcher interface for fetching BEEF from peers
type TransactionFetcher interface {
	FetchBEEF(ctx context.Context, peerURL, topic string, outpoint transaction.Outpoint) ([]byte, error)
}