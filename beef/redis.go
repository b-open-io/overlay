package beef

import (
	"context"
	"log"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/redis/go-redis/v9"
)

type RedisBeefStorage struct {
	BaseBeefStorage
	db *redis.Client
}

func NewRedisBeefStorage(connString string) (*RedisBeefStorage, error) {
	r := &RedisBeefStorage{}
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
	return t.db.HSet(ctx, BeefKey, txid.String(), beefBytes).Err()
}
