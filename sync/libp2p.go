package sync

import (
	"context"
	"fmt"
	"log"

	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/storage"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
)

// LibP2PSyncConfig holds the configuration for LibP2P synchronization
type LibP2PSyncConfig struct {
	Engine  *engine.Engine
	Storage *storage.EventDataStorage
	Topics  []string
	Context context.Context
}

// LibP2PSyncManager manages the lifecycle of LibP2P sync
type LibP2PSyncManager struct {
	libp2pSync *pubsub.LibP2PSync
}

// RegisterLibP2PSync sets up and starts LibP2P synchronization with the given topics
func RegisterLibP2PSync(config *LibP2PSyncConfig) (*LibP2PSyncManager, error) {
	if config == nil || config.Engine == nil || config.Storage == nil || config.Context == nil {
		log.Fatal("RegisterLibP2PSync: config, engine, storage, and context are required")
	}

	if len(config.Topics) == 0 {
		log.Println("No topics configured for LibP2P sync")
		return &LibP2PSyncManager{}, nil
	}

	// Get beef storage from the event storage
	beefStorage := config.Storage.GetBeefStorage()

	// Create LibP2P sync instance
	libp2pSync, err := pubsub.NewLibP2PSync(config.Engine, config.Storage, beefStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to create LibP2P sync: %w", err)
	}

	// Start LibP2P sync
	if err := libp2pSync.Start(config.Context, config.Topics); err != nil {
		return nil, fmt.Errorf("failed to start LibP2P sync: %w", err)
	}

	log.Printf("Started LibP2P sync with topics: %v", config.Topics)
	
	return &LibP2PSyncManager{libp2pSync: libp2pSync}, nil
}

// Stop stops the LibP2P sync
func (m *LibP2PSyncManager) Stop() {
	if m.libp2pSync != nil {
		m.libp2pSync.Stop()
		log.Println("Stopped LibP2P sync")
	}
}

// GetLibP2PSync returns the underlying LibP2PSync instance for use in other components
func (m *LibP2PSyncManager) GetLibP2PSync() *pubsub.LibP2PSync {
	return m.libp2pSync
}