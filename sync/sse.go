package sync

import (
	"context"
	"log"

	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/storage"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
)

// SSESyncConfig holds the configuration for SSE synchronization
type SSESyncConfig struct {
	Engine      *engine.Engine
	Storage     storage.EventDataStorage
	PeerTopics  map[string][]string // peer URL -> topics
	Context     context.Context
}

// SSESyncManager manages the lifecycle of SSE sync
type SSESyncManager struct {
	sseSync *pubsub.SSESync
}

// RegisterSSESync sets up and starts SSE synchronization with the given peer configuration
func RegisterSSESync(config *SSESyncConfig) (*SSESyncManager, error) {
	if config == nil || config.Engine == nil || config.Storage == nil || config.Context == nil {
		log.Fatal("RegisterSSESync: config, engine, storage, and context are required")
	}

	if len(config.PeerTopics) == 0 {
		log.Println("No peers configured for SSE sync")
		return &SSESyncManager{}, nil
	}

	// Initialize SSE sync with engine and storage
	sseSync := pubsub.NewSSESync(config.Engine, config.Storage)

	// Start SSE sync
	if err := sseSync.Start(config.Context, config.PeerTopics); err != nil {
		log.Printf("Failed to start SSE sync: %v", err)
		return nil, err
	}

	log.Printf("Started SSE sync with peers: %v", config.PeerTopics)
	
	return &SSESyncManager{sseSync: sseSync}, nil
}

// Stop stops the SSE sync
func (m *SSESyncManager) Stop() {
	if m.sseSync != nil {
		m.sseSync.Stop()
		log.Println("Stopped SSE sync")
	}
}