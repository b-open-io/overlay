package beef

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
	"github.com/redis/go-redis/v9"
)

type RedisBeefStorage struct {
	db               *redis.Client
	ttl              time.Duration
	fallback         BeefStorage
	hexpireSupported bool
}

func NewRedisBeefStorage(connString string, fallback BeefStorage) (*RedisBeefStorage, error) {
	r := &RedisBeefStorage{
		fallback: fallback,
	}

	// Parse TTL from query parameters if present
	// Example: redis://localhost:6379?ttl=24h
	var cacheTTL time.Duration
	cleanConnString := connString

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
		cleanConnString = connString[:idx]
	}

	r.ttl = cacheTTL

	log.Println("Connecting to Redis BeefStorage...", cleanConnString)
	if opts, err := redis.ParseURL(cleanConnString); err != nil {
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

func (t *RedisBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	txidStr := txid.String()

	// Try to load from Redis first
	if beefBytes, err := t.db.HGet(ctx, BeefKey, txidStr).Bytes(); err != nil && err != redis.Nil {
		return nil, err
	} else if err == nil && beefBytes != nil {
		return beefBytes, nil
	}

	// Not found in Redis, try fallback
	if t.fallback != nil {
		beefBytes, err := t.fallback.LoadBeef(ctx, txid)
		if err == nil {
			// Cache the result from fallback
			t.SaveBeef(ctx, txid, beefBytes)
		}
		return beefBytes, err
	}

	return nil, ErrNotFound
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

func (t *RedisBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
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

// Close closes the Redis client connection and fallback storage
func (r *RedisBeefStorage) Close() error {
	var errs []error
	
	if r.db != nil {
		if err := r.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Redis client: %w", err))
		}
	}
	
	if r.fallback != nil {
		if err := r.fallback.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close fallback storage: %w", err))
		}
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors closing Redis beef storage: %v", errs)
	}
	return nil
}

// UpdateMerklePath updates the merkle path for a transaction by delegating to the fallback
func (r *RedisBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash, ct chaintracker.ChainTracker) ([]byte, error) {
	if r.fallback != nil {
		beefBytes, err := r.fallback.UpdateMerklePath(ctx, txid, ct)
		if err == nil && len(beefBytes) > 0 {
			// Update our own storage with the new beef
			r.SaveBeef(ctx, txid, beefBytes)
		}
		return beefBytes, err
	}
	return nil, ErrNotFound
}
