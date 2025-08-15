package storage

import (
	"github.com/b-open-io/overlay/beef"
	"github.com/redis/go-redis/v9"
)

// BaseEventDataStorage provides common fields and methods for all EventDataStorage implementations
// This uses Go's struct embedding to achieve code reuse across different storage backends
type BaseEventDataStorage struct {
	beefStore beef.BeefStorage
	pubRedis  *redis.Client  // Separate Redis client for publishing
}

// NewBaseEventDataStorage creates a new BaseEventDataStorage with the given dependencies
func NewBaseEventDataStorage(beefStore beef.BeefStorage, pubRedis *redis.Client) BaseEventDataStorage {
	return BaseEventDataStorage{
		beefStore: beefStore,
		pubRedis:  pubRedis,
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
