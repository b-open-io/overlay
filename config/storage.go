package config

import (
	"fmt"

	"github.com/b-open-io/overlay/beef"
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
//
// The beefURL parameter can be:
//   - A single connection string: "redis://localhost:6379"
//   - A JSON array of connection strings: `["lru://100mb", "redis://localhost:6379", "junglebus://"]`
//   - A comma-separated list: "lru://100mb,redis://localhost:6379,junglebus://"
//
// Note: If your connection strings contain commas, use the JSON array format.
//
// Example configurations:
//
//  1. All Redis:
//     CreateEventStorage("redis://localhost:6379", "redis://localhost:6379", "redis://localhost:6379", "redis://localhost:6379")
//
//  2. MongoDB for events, hierarchical BEEF storage, Redis queue and pubsub:
//     CreateEventStorage("mongodb://localhost:27017/bsv21", `["lru://1gb", "redis://localhost:6379", "junglebus://"]`, "redis://localhost:6379", "redis://localhost:6379")
//
//  3. SQLite for events, filesystem for BEEF, SQLite queue, channel pubsub:
//     CreateEventStorage("./overlay.db", "./beef_storage/", "./queue.db", "channels://")
//
//  4. Default no-dependency setup:
//     CreateEventStorage("", "", "", "")  // Uses ~/.1sat/overlay.db, ~/.1sat/beef/, ~/.1sat/queue.db, channels://
func CreateEventStorage(eventURL, beefURL, queueURL, pubsubURL string) (*storage.EventDataStorage, error) {
	// Create BEEF storage - let it handle parsing and defaults
	beefStorage, err := beef.CreateBeefStorage(beefURL)
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
	eventStorage, err := storage.CreateEventDataStorage(eventURL, beefStorage, queueStorage, pubSubImpl)
	if err != nil {
		return nil, fmt.Errorf("failed to create event storage: %w", err)
	}

	return eventStorage, nil
}
