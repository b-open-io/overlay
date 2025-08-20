package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
func CreateEventStorage(eventURL, beefURL, queueURL, pubsubURL string) (storage.EventDataStorage, error) {
	// Parse beefURL to determine if it's a single string or array
	var beefURLStrings []string

	if beefURL != "" {
		// First try to parse as JSON array
		if strings.HasPrefix(strings.TrimSpace(beefURL), "[") {
			if err := json.Unmarshal([]byte(beefURL), &beefURLStrings); err != nil {
				return nil, fmt.Errorf("invalid JSON array for BEEF storage: %w", err)
			}
		} else if strings.Contains(beefURL, ",") {
			// If it contains commas, split it
			beefURLStrings = strings.Split(beefURL, ",")
			// Trim whitespace from each element
			for i, s := range beefURLStrings {
				beefURLStrings[i] = strings.TrimSpace(s)
			}
		} else {
			// Single connection string
			beefURLStrings = []string{beefURL}
		}
	}

	// Create BEEF storage from connection strings (defaults to ~/.1sat/beef/ if not set)
	if len(beefURLStrings) == 0 {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			beefURLStrings = []string{"./beef_storage/"} // Fallback
		} else {
			dotOneSatDir := filepath.Join(homeDir, ".1sat")
			if err := os.MkdirAll(dotOneSatDir, 0755); err != nil {
				beefURLStrings = []string{"./beef_storage/"} // Fallback if can't create dir
			} else {
				beefURLStrings = []string{filepath.Join(dotOneSatDir, "beef")}
			}
		}
	}
	beefStorage, err := beef.CreateBeefStorage(beefURLStrings)
	if err != nil {
		return nil, fmt.Errorf("failed to create BEEF storage: %w", err)
	}

	// Create Queue storage
	queueStorage, err := queue.CreateQueueStorage(queueURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue storage: %w", err)
	}

	// Create PubSub implementation
	var pubSubImpl pubsub.PubSub
	if pubsubURL == "" {
		pubsubURL = "channels://" // Default to channel-based pub/sub
	}

	switch {
	case strings.HasPrefix(pubsubURL, "redis://"):
		// Create Redis-based pub/sub
		if redisPubSub, err := pubsub.NewRedisPubSub(pubsubURL); err != nil {
			return nil, fmt.Errorf("failed to create Redis pub/sub: %w", err)
		} else {
			pubSubImpl = redisPubSub
		}

	case strings.HasPrefix(pubsubURL, "channels://"), pubsubURL == "":
		// Create channel-based pub/sub (no dependencies)
		pubSubImpl = pubsub.NewChannelPubSub()

	default:
		return nil, fmt.Errorf("unsupported pub/sub URL scheme: %s", pubsubURL)
	}

	// Create event storage from connection string  
	eventStorage, err := storage.CreateEventDataStorage(eventURL, beefStorage, queueStorage, pubSubImpl)
	if err != nil {
		return nil, fmt.Errorf("failed to create event storage: %w", err)
	}

	return eventStorage, nil
}
