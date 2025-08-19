package storage

import (
	"fmt"
	
	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/redis/go-redis/v9"
)

// BaseEventDataStorage provides common fields and methods for all EventDataStorage implementations
// This uses Go's struct embedding to achieve code reuse across different storage backends
type BaseEventDataStorage struct {
	beefStore beef.BeefStorage
	pubRedis  *redis.Client     // Separate Redis client for direct access
	pubsub *pubsub.RedisPubSub // RedisPubSub for event publishing and buffering
}

// NewBaseEventDataStorage creates a new BaseEventDataStorage with the given dependencies
func NewBaseEventDataStorage(beefStore beef.BeefStorage, pubRedis *redis.Client) BaseEventDataStorage {
	var redisPubSub *pubsub.RedisPubSub
	if pubRedis != nil {
		// Create RedisPubSub from the same connection details
		// Extract connection string from client options
		opts := pubRedis.Options()
		connString := fmt.Sprintf("redis://%s", opts.Addr)
		if opts.Password != "" {
			connString = fmt.Sprintf("redis://:%s@%s", opts.Password, opts.Addr)
		}
		if opts.DB != 0 {
			connString = fmt.Sprintf("%s/%d", connString, opts.DB)
		}
		
		if pubsubClient, err := pubsub.NewRedisPubSub(connString); err == nil {
			redisPubSub = pubsubClient
		}
	}
	
	return BaseEventDataStorage{
		beefStore: beefStore,
		pubRedis:  pubRedis,
		pubsub:    redisPubSub,
	}
}

// GetBeefStorage returns the underlying BEEF storage implementation
func (b *BaseEventDataStorage) GetBeefStorage() beef.BeefStorage {
	return b.beefStore
}

// GetRedisClient returns the publishing Redis client
// This may be nil if no publishing is configured
func (b *BaseEventDataStorage) GetRedisClient() *redis.Client {
	return b.pubRedis
}

// GetPubSub returns the underlying RedisPubSub for event publishing and buffering
// Returns nil if no pubsub is configured
func (b *BaseEventDataStorage) GetPubSub() *pubsub.RedisPubSub {
	return b.pubsub
}
