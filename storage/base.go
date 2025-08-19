package storage

import (
	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
)

// BaseEventDataStorage provides common fields and methods for all EventDataStorage implementations
// This uses Go's struct embedding to achieve code reuse across different storage backends
type BaseEventDataStorage struct {
	beefStore beef.BeefStorage
	pubsub    pubsub.PubSub // Generic PubSub interface for event publishing and buffering
}

// NewBaseEventDataStorage creates a new BaseEventDataStorage with the given dependencies
func NewBaseEventDataStorage(beefStore beef.BeefStorage, pubsub pubsub.PubSub) BaseEventDataStorage {
	return BaseEventDataStorage{
		beefStore: beefStore,
		pubsub:    pubsub,
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
