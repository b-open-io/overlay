package beef

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker/headers_client"
	"github.com/redis/go-redis/v9"
)

type RedisBeefStorage struct {
	BaseBeefStorage
	db  *redis.Client
	ttl time.Duration
}

func NewRedisBeefStorage(connString string, cacheTTL time.Duration) (*RedisBeefStorage, error) {
	r := &RedisBeefStorage{
		ttl: cacheTTL,
	}
	log.Println("Connecting to Redis BeefStorage...", connString)
	if opts, err := redis.ParseURL(connString); err != nil {
		return nil, err
	} else {
		r.db = redis.NewClient(opts)
		return r, nil
	}
}

func (t *RedisBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	txidStr := txid.String()
	if beefBytes, err := t.db.HGet(ctx, BeefKey, txidStr).Bytes(); err != nil && err != redis.Nil {
		return nil, err
	} else if err == nil && beefBytes != nil {
		return beefBytes, nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	beefBytes, err := t.BaseBeefStorage.LoadBeef(ctx, txid)
	if err == nil {
		t.SaveBeef(ctx, txid, beefBytes)
	}
	return beefBytes, err
}

func (t *RedisBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	_, err := t.db.Pipelined(ctx, func(p redis.Pipeliner) error {
		if err := p.HSet(ctx, BeefKey, txid.String(), beefBytes).Err(); err != nil {
			return err
		} else if t.ttl > 0 {
			if err := p.Expire(ctx, BeefKey, t.ttl).Err(); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// LoadTx loads a transaction from BEEF storage with optional merkle path validation
// This overrides BaseBeefStorage.LoadTx to ensure RedisBeefStorage.LoadBeef is called
func (t *RedisBeefStorage) LoadTx(ctx context.Context, txid *chainhash.Hash, chaintracker *headers_client.Client) (*transaction.Transaction, error) {
	// Load BEEF from storage - this will use RedisBeefStorage.LoadBeef
	beefBytes, err := t.LoadBeef(ctx, txid)
	if err != nil {
		return nil, err
	}

	// Parse BEEF to get the transaction
	_, tx, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, err
	}

	// Validate merkle path if present and chaintracker is provided
	if tx.MerklePath != nil && chaintracker != nil {
		if err := t.BaseBeefStorage.validateMerklePath(ctx, tx, txid, chaintracker); err != nil {
			return nil, err
		}
	}

	return tx, nil
}
