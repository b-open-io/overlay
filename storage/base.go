package storage

import (
	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
)

// BaseEventDataStorage provides common fields and methods for all EventDataStorage implementations
// This uses Go's struct embedding to achieve code reuse across different storage backends
type BaseEventDataStorage struct {
	beefStore beef.BeefStorage
	pub       publish.Publisher
}

// NewBaseEventDataStorage creates a new BaseEventDataStorage with the given dependencies
func NewBaseEventDataStorage(beefStore beef.BeefStorage, pub publish.Publisher) BaseEventDataStorage {
	return BaseEventDataStorage{
		beefStore: beefStore,
		pub:       pub,
	}
}

// GetBeefStorage returns the underlying BEEF storage implementation
func (b *BaseEventDataStorage) GetBeefStorage() beef.BeefStorage {
	return b.beefStore
}

// GetPublisher returns the underlying publisher implementation
func (b *BaseEventDataStorage) GetPublisher() publish.Publisher {
	return b.pub
}
