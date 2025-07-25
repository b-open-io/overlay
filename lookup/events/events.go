package events

import (
	"context"

	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

type JoinType int

var (
	JoinTypeIntersect  JoinType = 0
	JoinTypeUnion      JoinType = 1
	JoinTypeDifference JoinType = 2
)

type BlockPos struct {
	Height uint32 `json:"height"`
	Idx    uint64 `json:"idx"`
}
type Question struct {
	Event    string    `json:"event"`
	Events   []string  `json:"events"`
	JoinType *JoinType `json:"join"`
	From     BlockPos  `json:"from"`
	Limit    int       `json:"limit"`
	Spent    *bool     `json:"spent"`
	Reverse  bool      `json:"rev"`
}

type EventLookup interface {
	engine.LookupService
	SaveEvent(ctx context.Context, outpoint *transaction.Outpoint, event string, height uint32, idx uint64) error
	SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, height uint32, idx uint64) error
	// Lookup(ctx context.Context, q *lookup.LookupQuestion) (*lookup.LookupAnswer, error)
	// OutputSpent(ctx context.Context, outpoint *transaction.Outpoint, topic string) error
	// OutputsSpent(ctx context.Context, outpoints []*transaction.Outpoint, topic string) error
	// OutputDeleted(ctx context.Context, outpoint *transaction.Outpoint, topic string) error
	// OutputBlockHeightUpdated(ctx context.Context, outpoint *transaction.Outpoint, blockHeight uint32, blockIdx uint64) error
	// GetDocumentation() string
	// GetMetaData() *overlay.MetaData
}
