package queue

import "context"

// ScoredMember represents a member with its score in sorted set operations
type ScoredMember struct {
	Member string
	Score  float64
}

// ScoreRange defines range parameters for sorted set queries
type ScoreRange struct {
	Min    *float64 // nil = -inf
	Max    *float64 // nil = +inf 
	Offset int64    // 0 = start from beginning
	Count  int64    // 0 = all (default), positive = limit
}

// QueueStorage provides Redis-like operations for queues, caches, and configuration
type QueueStorage interface {
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

	// Sorted Set Operations - for queues, progress tracking, fee balances, etc.
	ZAdd(ctx context.Context, key string, members ...ScoredMember) error
	ZRem(ctx context.Context, key string, members ...string) error
	ZRange(ctx context.Context, key string, scoreRange ScoreRange) ([]ScoredMember, error)
	ZScore(ctx context.Context, key, member string) (float64, error)
	ZCard(ctx context.Context, key string) (int64, error)
	ZIncrBy(ctx context.Context, key, member string, increment float64) (float64, error)
	ZSum(ctx context.Context, key string) (float64, error)

	// Resource management
	Close() error
}
