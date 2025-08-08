// Package events provides event-based lookup services for BSV overlay networks.
// It supports storing and querying transaction outputs by event names with
// multiple backend implementations (Redis, MongoDB, SQLite).
//
// Scoring System:
// Events are sorted by a score that combines block height and transaction index.
// - Mined transactions: score = height + idx/1e9 (e.g., 850000.000000123)
//   - Block height forms the integer part
//   - Block index forms the decimal part (9 digits, zero-padded)
//   - Supports block heights up to 9,007,199 (7+ digits)
//   - Supports block indices up to 999,999,999 (9 digits)
// - Unmined transactions: score = current unix timestamp (seconds)
//   - Ensures unmined transactions sort after all mined transactions
//   - Unix timestamps are currently ~1.7 billion, well above any block height
package events

import (
	"context"

	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// JoinType defines how multiple events are combined in queries
type JoinType int

var (
	// JoinTypeIntersect returns outputs that have ALL specified events
	JoinTypeIntersect  JoinType = 0
	// JoinTypeUnion returns outputs that have ANY of the specified events
	JoinTypeUnion      JoinType = 1
	// JoinTypeDifference returns outputs from first event minus those in subsequent events
	JoinTypeDifference JoinType = 2
)

// SpentStatus defines filtering options for spent/unspent outputs
type SpentStatus bool

const (
	// SpentStatusUnspent includes only unspent outputs
	SpentStatusUnspent SpentStatus = false
	// SpentStatusSpent includes only spent outputs
	SpentStatusSpent SpentStatus = true
)

type Question struct {
	Event       string    `json:"event"`
	Events      []string  `json:"events"`
	JoinType    *JoinType `json:"join"`
	From        float64   `json:"from"`
	Until       float64   `json:"until"`
	Limit       int       `json:"limit"`
	UnspentOnly bool      `json:"unspentOnly"`
	Reverse     bool      `json:"rev"`
}

// OutpointResult contains the result of an outpoint lookup
type OutpointResult struct {
	Outpoint *transaction.Outpoint
	Score    float64
	Data     interface{}
}

// EventLookup defines the interface for event-based lookup services.
// It extends engine.LookupService with event-specific functionality.
type EventLookup interface {
	engine.LookupService
	
	// SaveEvents associates multiple events with a single output in one operation.
	// The data parameter can be used to store arbitrary data (as JSON) associated with the outpoint.
	SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, height uint32, idx uint64, data interface{}) error
	
	// LookupOutpoints returns outpoints matching the given query criteria.
	// The includeData parameter controls whether to fetch associated data (default: false).
	LookupOutpoints(ctx context.Context, question *Question, includeData ...bool) ([]*OutpointResult, error)
	
	// GetBlockData returns all transactions for a specific event at a given block height.
	// This method returns the data exactly as stored without any protocol-specific mapping.
	GetBlockData(ctx context.Context, event string, height uint32) ([]map[string]interface{}, error)
}
