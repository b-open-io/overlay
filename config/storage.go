package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
	"github.com/b-open-io/overlay/storage"
)

// CreateEventStorage creates a fully configured event storage with BEEF storage and optional publisher.
//
// If empty strings are provided, falls back to environment variables:
//   - eventURL: EVENTS_URL env var (defaults to ./overlay.db)
//   - beefURL: BEEF_URL env var (defaults to ./beef_storage/)
//   - pubsubURL: PUBSUB_URL env var (optional - if not provided, no events will be published)
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
//     CreateEventStorage("redis://localhost:6379", "redis://localhost:6379", "redis://localhost:6379")
//
//  2. MongoDB for events, hierarchical BEEF storage:
//     CreateEventStorage("mongodb://localhost:27017/bsv21", `["lru://1gb", "redis://localhost:6379", "junglebus://"]`, "redis://localhost:6379")
//
//  3. SQLite for events, filesystem for BEEF (good for development):
//     CreateEventStorage("./overlay.db", "./beef_storage/", "redis://localhost:6379")
//
//  4. Use environment variables:
//     CreateEventStorage("", "", "")
func CreateEventStorage(eventURL, beefURL, pubsubURL string) (storage.EventDataStorage, error) {
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

	// Create BEEF storage from connection strings (defaults to ./beef_storage/ if not set)
	beefStorage, err := beef.CreateBeefStorage(beefURLStrings)
	if err != nil {
		return nil, fmt.Errorf("failed to create BEEF storage: %w", err)
	}

	// Create publisher if URL is provided (optional)
	var publisher publish.Publisher
	if pubsubURL == "" {
		pubsubURL = os.Getenv("PUBSUB_URL")
	}
	
	if pubsubURL != "" {
		// Create publisher (actually PubSub but storage only needs Publisher interface)
		var err error
		publisher, err = publish.NewRedisPublish(pubsubURL)
		if err != nil {
			return nil, fmt.Errorf("failed to create publisher: %w", err)
		}
	}
	// If pubsubURL is empty, publisher remains nil and no events will be published

	// Create event storage from connection string (defaults to ./overlay.db if not set)
	eventStorage, err := storage.CreateEventDataStorage(eventURL, beefStorage, publisher)
	if err != nil {
		return nil, fmt.Errorf("failed to create event storage: %w", err)
	}

	return eventStorage, nil
}
