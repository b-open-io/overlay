package config

import (
	"fmt"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/headers"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
	"github.com/b-open-io/overlay/storage"
)

// CreateEventStorage creates a fully configured event storage with BEEF storage, Queue storage, and PubSub.
//
// Parameters:
//   - eventURL: Event storage connection string (e.g., "redis://localhost:6379", "./overlay.db")
//   - beefURL: BEEF storage connection string(s) (can be hierarchical)
//   - queueURL: Queue storage connection string (e.g., "redis://localhost:6379", "./queue.db")
//   - pubsubURL: PubSub connection string (e.g., "redis://localhost:6379", "channels://")
//   - headersClient: Headers client for merkle proof validation and reconciliation
//
// The beefURL parameter can be:
//   - A single connection string: "redis://localhost:6379"
//   - A JSON array of connection strings: `["lru://100mb", "redis://localhost:6379", "junglebus://"]`
//   - A comma-separated list: "lru://100mb,redis://localhost:6379,junglebus://"
//
// Note: If your connection strings contain commas, use the JSON array format.
//
// When a headers client is provided, the BEEF storage will validate merkle proofs on load
// and the storage will perform merkle root reconciliation.
//
// Example configurations:
//
//  1. PostgreSQL for events:
//     CreateEventStorage("postgresql://localhost:5432/overlay", "redis://localhost:6379", "redis://localhost:6379", "redis://localhost:6379", headersClient)
//
//  2. MongoDB for events with hierarchical BEEF storage:
//     CreateEventStorage("mongodb://localhost:27017/bsv21", `["lru://1gb", "redis://localhost:6379", "junglebus://"]`, "redis://localhost:6379", "redis://localhost:6379", headersClient)
//
//  3. SQLite for events, filesystem for BEEF, SQLite queue, channel pubsub:
//     CreateEventStorage("./overlay.db", "./beef_storage/", "./queue.db", "channels://", headersClient)
//
//  4. Default no-dependency setup:
//     CreateEventStorage("", "", "", "", headersClient)  // Uses ~/.1sat/overlay.db, ~/.1sat/beef/, ~/.1sat/queue.db, channels://
func CreateEventStorage(eventURL, beefURL, queueURL, pubsubURL string, headersClient *headers.Client) (*storage.EventDataStorage, error) {
	// Create BEEF storage with optional validation
	var beefStorage beef.BeefStorage
	var err error

	if headersClient != nil {
		// Create validating BEEF storage when headers client is provided
		beefStorage, err = beef.CreateValidatingBeefStorage(beefURL, headersClient)
	} else {
		// Create regular BEEF storage without validation
		beefStorage, err = beef.CreateBeefStorage(beefURL)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create BEEF storage: %w", err)
	}

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
	eventStorage, err := storage.CreateEventDataStorage(eventURL, beefStorage, queueStorage, pubSubImpl, headersClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create event storage: %w", err)
	}

	return eventStorage, nil
}
