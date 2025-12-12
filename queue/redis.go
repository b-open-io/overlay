package queue

import (
	"context"
	"log"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type RedisQueueStorage struct {
	client *redis.Client
}

// Close closes the Redis client connection
func (r *RedisQueueStorage) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

func NewRedisQueueStorage(connString string) (*RedisQueueStorage, error) {
	log.Println("Connecting to Redis Queue Storage...", connString)
	opts, err := redis.ParseURL(connString)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)
	return &RedisQueueStorage{client: client}, nil
}

// Set Operations
func (s *RedisQueueStorage) SAdd(ctx context.Context, key string, members ...string) error {
	return s.client.SAdd(ctx, key, members).Err()
}

func (s *RedisQueueStorage) SMembers(ctx context.Context, key string) ([]string, error) {
	return s.client.SMembers(ctx, key).Result()
}

func (s *RedisQueueStorage) SRem(ctx context.Context, key string, members ...string) error {
	return s.client.SRem(ctx, key, members).Err()
}

func (s *RedisQueueStorage) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return s.client.SIsMember(ctx, key, member).Result()
}

// Hash Operations
func (s *RedisQueueStorage) HSet(ctx context.Context, key, field, value string) error {
	return s.client.HSet(ctx, key, field, value).Err()
}

func (s *RedisQueueStorage) HGet(ctx context.Context, key, field string) (string, error) {
	return s.client.HGet(ctx, key, field).Result()
}

func (s *RedisQueueStorage) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return s.client.HGetAll(ctx, key).Result()
}

func (s *RedisQueueStorage) HDel(ctx context.Context, key string, fields ...string) error {
	return s.client.HDel(ctx, key, fields...).Err()
}

func (s *RedisQueueStorage) HMSet(ctx context.Context, key string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}
	return s.client.HMSet(ctx, key, fields).Err()
}

func (s *RedisQueueStorage) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	if len(fields) == 0 {
		return []string{}, nil
	}
	results, err := s.client.HMGet(ctx, key, fields...).Result()
	if err != nil {
		return nil, err
	}
	values := make([]string, len(results))
	for i, result := range results {
		if result != nil {
			values[i] = result.(string)
		}
	}
	return values, nil
}

// Sorted Set Operations
func (s *RedisQueueStorage) ZAdd(ctx context.Context, key string, members ...ScoredMember) error {
	var redisMembers []redis.Z
	for _, member := range members {
		redisMembers = append(redisMembers, redis.Z{
			Score:  member.Score,
			Member: member.Member,
		})
	}
	return s.client.ZAdd(ctx, key, redisMembers...).Err()
}

func (s *RedisQueueStorage) ZRem(ctx context.Context, key string, members ...string) error {
	return s.client.ZRem(ctx, key, members).Err()
}


func (s *RedisQueueStorage) ZRange(ctx context.Context, key string, scoreRange ScoreRange) ([]ScoredMember, error) {
	min := "-inf"
	max := "+inf"
	if scoreRange.Min != nil {
		min = strconv.FormatFloat(*scoreRange.Min, 'f', -1, 64)
	}
	if scoreRange.Max != nil {
		max = strconv.FormatFloat(*scoreRange.Max, 'f', -1, 64)
	}

	count := int64(-1) // Redis -1 = all
	if scoreRange.Count > 0 {
		count = scoreRange.Count
	}

	results, err := s.client.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: scoreRange.Offset,
		Count:  count,
	}).Result()
	if err != nil {
		return nil, err
	}

	members := make([]ScoredMember, len(results))
	for i, result := range results {
		members[i] = ScoredMember{
			Member: result.Member.(string),
			Score:  result.Score,
		}
	}
	return members, nil
}

func (s *RedisQueueStorage) ZRevRange(ctx context.Context, key string, scoreRange ScoreRange) ([]ScoredMember, error) {
	min := "-inf"
	max := "+inf"
	if scoreRange.Min != nil {
		min = strconv.FormatFloat(*scoreRange.Min, 'f', -1, 64)
	}
	if scoreRange.Max != nil {
		max = strconv.FormatFloat(*scoreRange.Max, 'f', -1, 64)
	}

	count := int64(-1) // Redis -1 = all
	if scoreRange.Count > 0 {
		count = scoreRange.Count
	}

	results, err := s.client.ZRevRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: scoreRange.Offset,
		Count:  count,
	}).Result()
	if err != nil {
		return nil, err
	}

	members := make([]ScoredMember, len(results))
	for i, result := range results {
		members[i] = ScoredMember{
			Member: result.Member.(string),
			Score:  result.Score,
		}
	}
	return members, nil
}

func (s *RedisQueueStorage) ZScore(ctx context.Context, key, member string) (float64, error) {
	return s.client.ZScore(ctx, key, member).Result()
}

func (s *RedisQueueStorage) ZCard(ctx context.Context, key string) (int64, error) {
	return s.client.ZCard(ctx, key).Result()
}

// Fee balance management
func (s *RedisQueueStorage) ZIncrBy(ctx context.Context, key, member string, increment float64) (float64, error) {
	return s.client.ZIncrBy(ctx, key, increment, member).Result()
}

func (s *RedisQueueStorage) ZSum(ctx context.Context, key string) (float64, error) {
	// Get all members with scores
	results, err := s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return 0, err
	}
	
	var sum float64
	for _, result := range results {
		sum += result.Score
	}
	return sum, nil
}

