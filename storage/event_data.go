package storage

import (
	"context"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// OutputData represents an input or output with its data
type OutputData struct {
	TxID     *chainhash.Hash `json:"txid,omitempty"`     // Source transaction ID (only for inputs)
	Vout     uint32          `json:"vout"`
	Data     interface{}     `json:"data,omitempty"`
	Script   []byte          `json:"script"`
	Satoshis uint64          `json:"satoshis"`
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
	
	// GetPublisher returns the underlying publisher implementation
	GetPublisher() publish.Publisher
	
	// Block Data Methods
	// GetTransactionsByTopicAndHeight returns all transactions for a topic at a specific block height
	// Returns transaction structure with inputs/outputs but no protocol-specific data fields
	GetTransactionsByTopicAndHeight(ctx context.Context, topic string, height uint32) ([]*TransactionData, error)
	
	// Event Management Methods
	// SaveEvents associates multiple events with a single output, storing arbitrary data
	SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, height uint32, idx uint64, data interface{}) error
	
	// FindEvents returns all events associated with a given outpoint
	FindEvents(ctx context.Context, outpoint *transaction.Outpoint) ([]string, error)
	
	// Event Query Methods
	// LookupOutpoints returns outpoints matching the given query criteria
	LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error)
	
	// GetOutputData retrieves the data associated with a specific output
	GetOutputData(ctx context.Context, outpoint *transaction.Outpoint) (interface{}, error)
}

// EventQuestion defines query parameters for event-based lookups
type EventQuestion struct {
	Event       string     `json:"event"`
	Events      []string   `json:"events"`
	JoinType    *JoinType  `json:"join"`
	From        float64    `json:"from"`
	Until       float64    `json:"until"`
	Limit       int        `json:"limit"`
	UnspentOnly bool       `json:"unspentOnly"`
	Reverse     bool       `json:"rev"`
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
	Outpoint *transaction.Outpoint
	Score    float64
	Data     interface{}
}