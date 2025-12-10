package beef

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/redis/go-redis/v9"
)

const (
	txField    = "tx"
	proofField = "prf"
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
	cleanConnString := connString

	if idx := strings.Index(connString, "?"); idx != -1 {
		queryStr := connString[idx+1:]
		params := parseRedisQueryParams(queryStr)
		if ttlStr, ok := params["ttl"]; ok {
			if ttl, err := time.ParseDuration(ttlStr); err == nil {
				cacheTTL = ttl
			}
		}
		cleanConnString = connString[:idx]
	}

	r.ttl = cacheTTL

	log.Println("Connecting to Redis BeefStorage...", cleanConnString)
	if opts, err := redis.ParseURL(cleanConnString); err != nil {
		return nil, err
	} else {
		r.db = redis.NewClient(opts)

		// Test if HEXPIRE is supported
		if cacheTTL > 0 {
			r.hexpireSupported = r.testHExpireSupport()
			if !r.hexpireSupported {
				log.Println("Warning: HEXPIRE not supported. Proof TTL will use key-level expiry.")
			}
		}

		return r, nil
	}
}

func (r *RedisBeefStorage) beefKey(txid *chainhash.Hash) string {
	return "beef:" + txid.String()
}

func (t *RedisBeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// Get both tx and proof in single HMGET call
	key := t.beefKey(txid)
	results, err := t.db.HMGet(ctx, key, txField, proofField).Result()
	if err != nil {
		return nil, err
	}

	// Check if tx exists
	if results[0] == nil {
		return nil, ErrNotFound
	}

	rawTx, ok := results[0].(string)
	if !ok {
		return nil, ErrNotFound
	}

	var proof []byte
	if results[1] != nil {
		if proofStr, ok := results[1].(string); ok {
			proof = []byte(proofStr)
		}
	}

	return assembleBEEF(txid, []byte(rawTx), proof)
}

func (t *RedisBeefStorage) testHExpireSupport() bool {
	ctx := context.Background()
	testKey := "beef:test:hexpire"
	testField := "test"

	defer t.db.Del(ctx, testKey)
	t.db.HSet(ctx, testKey, testField, "test")
	err := t.db.HExpire(ctx, testKey, time.Second, testField).Err()

	return err == nil
}

func (t *RedisBeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	rawTx, proof, err := splitBEEF(beefBytes)
	if err != nil {
		return err
	}

	key := t.beefKey(txid)

	// Store tx and proof together in hash
	fields := map[string]interface{}{
		txField: rawTx,
	}
	if len(proof) > 0 {
		fields[proofField] = proof
	}

	if err := t.db.HSet(ctx, key, fields).Err(); err != nil {
		return err
	}

	// Apply TTL
	if t.ttl > 0 && len(proof) > 0 {
		if t.hexpireSupported {
			t.db.HExpire(ctx, key, t.ttl, proofField)
		} else {
			// Fallback: expire entire key (tx + proof together)
			t.db.Expire(ctx, key, t.ttl)
		}
	}

	return nil
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

// GetRawTx loads just the raw transaction bytes
func (r *RedisBeefStorage) GetRawTx(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	rawTx, err := r.db.HGet(ctx, r.beefKey(txid), txField).Bytes()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	return rawTx, err
}

// GetProof loads just the merkle proof bytes
func (r *RedisBeefStorage) GetProof(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	proof, err := r.db.HGet(ctx, r.beefKey(txid), proofField).Bytes()
	if err == redis.Nil {
		return nil, ErrNotFound
	}
	return proof, err
}

// Close closes the Redis client connection
func (r *RedisBeefStorage) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}
