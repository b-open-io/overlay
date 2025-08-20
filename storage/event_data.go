package storage

import (
	"context"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// ScoredMember represents a member with its score in sorted set operations
type ScoredMember struct {
	Member string
	Score  float64
}

// OutputData represents an input or output with its data
type OutputData struct {
	TxID     *chainhash.Hash `json:"txid,omitempty"` // Transaction ID (for inputs: source txid, for outputs: current txid)
	Vout     uint32          `json:"vout"`
	Data     interface{}     `json:"data,omitempty"`
	Script   []byte          `json:"script"`
	Satoshis uint64          `json:"satoshis"`
	Spend    *chainhash.Hash `json:"spend,omitempty"` // Spending transaction ID (only populated if spent)
	Score    float64         `json:"score"`           // Sort score for ordering/pagination
}

// TransactionData represents a transaction with its inputs and outputs
type TransactionData struct {
	TxID    chainhash.Hash `json:"txid"`
	Inputs  []*OutputData  `json:"inputs"`
	Outputs []*OutputData  `json:"outputs"`
}

// EventDataStorage extends the base Storage interface with event data and lookup capabilities
// This consolidates all database operations into a single storage interface
type EventDataStorage interface {
	engine.Storage

	// GetBeefStorage returns the underlying BEEF storage implementation
	GetBeefStorage() beef.BeefStorage
	
	// GetPubSub returns the PubSub interface for event publishing and buffering
	// Returns nil if no pubsub is configured
	GetPubSub() pubsub.PubSub

	// Block Data Methods
	// GetTransactionsByTopicAndHeight returns all transactions for a topic at a specific block height
	// Returns transaction structure with inputs/outputs but no protocol-specific data fields
	GetTransactionsByTopicAndHeight(ctx context.Context, topic string, height uint32) ([]*TransactionData, error)

	// Event Management Methods
	// SaveEvents associates multiple events with a single output, storing arbitrary data
	SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, topic string, height uint32, idx uint64, data interface{}) error

	// FindEvents returns all events associated with a given outpoint
	FindEvents(ctx context.Context, outpoint *transaction.Outpoint) ([]string, error)

	// Event Query Methods
	// LookupOutpoints returns outpoints matching the given query criteria
	LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error)

	// LookupEventScores returns lightweight event scores for simple queries (no parsing/data loading)
	LookupEventScores(ctx context.Context, topic string, event string, fromScore float64) ([]ScoredMember, error)

	// GetOutputData retrieves the data associated with a specific output
	GetOutputData(ctx context.Context, outpoint *transaction.Outpoint) (interface{}, error)

	// FindOutputData returns outputs matching the given query criteria as OutputData objects
	// Supports paging with score-based 'from' parameter and can include spent outputs for history
	FindOutputData(ctx context.Context, question *EventQuestion) ([]*OutputData, error)

	// Set Operations - for whitelists, topic management, etc.
	SAdd(ctx context.Context, key string, members ...string) error
	SMembers(ctx context.Context, key string) ([]string, error) 
	SRem(ctx context.Context, key string, members ...string) error
	SIsMember(ctx context.Context, key, member string) (bool, error)

	// Hash Operations - for peer configurations, output data, etc.
	HSet(ctx context.Context, key, field, value string) error
	HGet(ctx context.Context, key, field string) (string, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HDel(ctx context.Context, key string, fields ...string) error

	// Sorted Set Operations - for queues, progress tracking, event scoring, etc.
	ZAdd(ctx context.Context, key string, members ...ScoredMember) error
	ZRem(ctx context.Context, key string, members ...string) error
	ZRangeByScore(ctx context.Context, key string, min, max float64, offset, count int64) ([]ScoredMember, error)
	ZScore(ctx context.Context, key, member string) (float64, error)
	ZCard(ctx context.Context, key string) (int64, error)
}

// EventQuestion defines query parameters for event-based lookups
type EventQuestion struct {
	Event       string    `json:"event"`
	Events      []string  `json:"events"`
	Topic       string    `json:"topic"`           // Required topic scoping
	JoinType    *JoinType `json:"join"`
	From        float64   `json:"from"`
	Until       float64   `json:"until"`
	Limit       int       `json:"limit"`
	UnspentOnly bool      `json:"unspentOnly"`
	Reverse     bool      `json:"rev"`
}

// JoinType defines how multiple events are combined in queries
type JoinType int

const (
	// JoinTypeIntersect returns outputs that have ALL specified events
	JoinTypeIntersect JoinType = iota
	// JoinTypeUnion returns outputs that have ANY of the specified events
	JoinTypeUnion
	// JoinTypeDifference returns outputs from first event minus those in subsequent events
	JoinTypeDifference
)

// OutpointResult contains the result of an outpoint lookup
type OutpointResult struct {
	Outpoint *transaction.Outpoint `json:"outpoint"`
	Score    float64               `json:"score"`
	Data     interface{}           `json:"data,omitempty"`
}
