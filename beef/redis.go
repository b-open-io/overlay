package beef

import (
	"context"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/b-open-io/overlay/internal/utils"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/redis/go-redis/v9"
)

type RedisBeefStorage struct {
	db               *redis.Client
	ttl              time.Duration
	hexpireSupported bool
}

func NewRedisBeefStorage(connString string) (*RedisBeefStorage, error) {
	r := &RedisBeefStorage{}

	// Parse TTL from query parameters if present
	// Example: redis://localhost:6379?ttl=24h
	var cacheTTL time.Duration
	connStringForRedis := connString

	if idx := strings.Index(connString, "?"); idx != -1 {
		// Extract TTL from query string before parsing URL
		queryStr := connString[idx+1:]
		params := parseRedisQueryParams(queryStr)
		if ttlStr, ok := params["ttl"]; ok {
			if ttl, err := time.ParseDuration(ttlStr); err == nil {
				cacheTTL = ttl
			}
		}
		// Redis ParseURL doesn't understand ttl parameter, so remove it
		connStringForRedis = connString[:idx]
	}

	r.ttl = cacheTTL

	slog.Debug("Connecting to Redis BeefStorage", "url", utils.SanitizeConnectionString(connStringForRedis))
	if opts, err := redis.ParseURL(connStringForRedis); err != nil {
		return nil, err
	} else {
		r.db = redis.NewClient(opts)

		// Test if HEXPIRE is supported by trying it on a test key
		if cacheTTL > 0 {
			r.hexpireSupported = r.testHExpireSupport()
			if !r.hexpireSupported {
				log.Println("Warning: HEXPIRE not supported by this Redis server. TTL will not be set on individual entries.")
			}
		}

		return r, nil
	}
}

func (t *RedisBeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	txidStr := txid.String()

	beefBytes, err := t.db.HGet(ctx, BeefKey, txidStr).Bytes()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return beefBytes, nil
}

func (t *RedisBeefStorage) testHExpireSupport() bool {
	ctx := context.Background()
	testKey := "beef:test:hexpire"
	testField := "test"

	// Clean up test key when done
	defer t.db.Del(ctx, testKey)

	// Try to set a field and expire it
	t.db.HSet(ctx, testKey, testField, "test")

	// Try HEXPIRE command - if it fails, the command is not supported
	err := t.db.HExpire(ctx, testKey, time.Second, testField).Err()

	return err == nil
}

func (t *RedisBeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	txidStr := txid.String()
	_, err := t.db.Pipelined(ctx, func(p redis.Pipeliner) error {
		if err := p.HSet(ctx, BeefKey, txidStr, beefBytes).Err(); err != nil {
			return err
		}
		// Set TTL on the individual hash field if configured and supported
		if t.ttl > 0 && t.hexpireSupported {
			// HExpire sets expiration on specific hash fields
			p.HExpire(ctx, BeefKey, t.ttl, txidStr)
		}
		return nil
	})
	return err
}

// parseRedisQueryParams extracts key-value pairs from a query string
func parseRedisQueryParams(query string) map[string]string {
	params := make(map[string]string)
	for _, param := range strings.Split(query, "&") {
		if kv := strings.SplitN(param, "=", 2); len(kv) == 2 {
			params[kv[0]] = kv[1]
		}
	}
	return params
}

// UpdateMerklePath is not supported by Redis storage
func (r *RedisBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return nil, nil
}

// Close closes the Redis client connection
func (r *RedisBeefStorage) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}
