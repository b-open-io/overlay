package storage

import (
	"fmt"
	
	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
	"github.com/redis/go-redis/v9"
)

// BaseEventDataStorage provides common fields and methods for all EventDataStorage implementations
// This uses Go's struct embedding to achieve code reuse across different storage backends
type BaseEventDataStorage struct {
	beefStore beef.BeefStorage
	pubRedis  *redis.Client     // Separate Redis client for direct access
	publisher publish.Publisher // Publisher interface for event publishing with buffering
}

// NewBaseEventDataStorage creates a new BaseEventDataStorage with the given dependencies
func NewBaseEventDataStorage(beefStore beef.BeefStorage, pubRedis *redis.Client) BaseEventDataStorage {
	var publisher publish.Publisher
	if pubRedis != nil {
		// Create Redis publisher from the same connection details
		// Extract connection string from client options
		opts := pubRedis.Options()
		connString := fmt.Sprintf("redis://%s", opts.Addr)
		if opts.Password != "" {
			connString = fmt.Sprintf("redis://:%s@%s", opts.Password, opts.Addr)
		}
		if opts.DB != 0 {
			connString = fmt.Sprintf("%s/%d", connString, opts.DB)
		}
		
		if redisPublisher, err := publish.NewRedisPublish(connString); err == nil {
			publisher = redisPublisher
		}
	}
	
	return BaseEventDataStorage{
		beefStore: beefStore,
		pubRedis:  pubRedis,
		publisher: publisher,
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

// GetPublisher returns the underlying Publisher for event publishing and buffering
// Returns nil if no publisher is configured
func (b *BaseEventDataStorage) GetPublisher() publish.Publisher {
	return b.publisher
}
