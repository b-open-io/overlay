package storage

import (
	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
)

// BaseEventDataStorage provides common fields and methods for all EventDataStorage implementations
// This uses Go's struct embedding to achieve code reuse across different storage backends
type BaseEventDataStorage struct {
	beefStore    beef.BeefStorage
	pubsub       pubsub.PubSub       // Generic PubSub interface for event publishing and buffering
	queueStorage queue.QueueStorage  // QueueStorage interface for Redis-like operations
}

// NewBaseEventDataStorage creates a new BaseEventDataStorage with the given dependencies
func NewBaseEventDataStorage(beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubsub pubsub.PubSub) BaseEventDataStorage {
	return BaseEventDataStorage{
		beefStore:    beefStore,
		queueStorage: queueStorage,
		pubsub:       pubsub,
	}
}

// GetBeefStorage returns the underlying BEEF storage implementation
func (b *BaseEventDataStorage) GetBeefStorage() beef.BeefStorage {
	return b.beefStore
}

// GetPubSub returns the PubSub interface for event publishing and buffering
// Returns nil if no pubsub is configured
func (b *BaseEventDataStorage) GetPubSub() pubsub.PubSub {
	return b.pubsub
}

// GetQueueStorage returns the QueueStorage interface for Redis-like operations
func (b *BaseEventDataStorage) GetQueueStorage() queue.QueueStorage {
	return b.queueStorage
}
