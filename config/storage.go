package config

import (
	"fmt"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
	"github.com/b-open-io/overlay/storage"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// CreateEventStorage creates a fully configured event storage with BEEF storage, Queue storage, and PubSub.
//
// Parameters:
//   - eventURL: Event storage connection string (e.g., "redis://localhost:6379", "./overlay.db")
//   - beefStorage: Pre-configured BEEF storage (create with beef.NewStorage)
//   - queueURL: Queue storage connection string (e.g., "redis://localhost:6379", "./queue.db")
//   - pubsubURL: PubSub connection string (e.g., "redis://localhost:6379", "channels://")
//   - headersClient: Headers client for merkle proof validation and reconciliation
//
// Example configurations:
//
//  1. PostgreSQL for events with SPV validation:
//     beefStorage, _ := beef.NewStorage("redis://localhost:6379", headersClient)
//     CreateEventStorage("postgresql://localhost:5432/overlay", beefStorage, "redis://localhost:6379", "redis://localhost:6379", headersClient)
//
//  2. MongoDB for events with hierarchical BEEF storage and validation:
//     beefStorage, _ := beef.NewStorage(`["lru://1gb", "redis://localhost:6379", "junglebus://"]`, headersClient)
//     CreateEventStorage("mongodb://localhost:27017/bsv21", beefStorage, "redis://localhost:6379", "redis://localhost:6379", headersClient)
//
//  3. SQLite for events without SPV validation:
//     beefStorage, _ := beef.NewStorage("./beef_storage/", nil)
//     CreateEventStorage("./overlay.db", beefStorage, "./queue.db", "channels://", headersClient)
//
//  4. Default no-dependency setup:
//     beefStorage, _ := beef.NewStorage("", nil)
//     CreateEventStorage("", beefStorage, "", "", chainTracker)
func CreateEventStorage(eventURL string, beefStorage *beef.Storage, queueURL, pubsubURL string, chainTracker chaintracker.ChainTracker) (*storage.EventDataStorage, error) {

	// Create Queue storage
	queueStorage, err := queue.CreateQueueStorage(queueURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue storage: %w", err)
	}

	// Create PubSub - let it handle defaults
	pubSubImpl, err := pubsub.CreatePubSub(pubsubURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create pub/sub: %w", err)
	}

	// Create event storage from connection string
	eventStorage, err := storage.CreateEventDataStorage(eventURL, beefStorage, queueStorage, pubSubImpl, chainTracker)
	if err != nil {
		return nil, fmt.Errorf("failed to create event storage: %w", err)
	}

	return eventStorage, nil
}
