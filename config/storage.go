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

// CreateEventStorage creates a fully configured event storage with BEEF storage and publisher.
//
// If empty strings are provided, falls back to environment variables:
//   - eventConnStr: EVENT_STORAGE env var (defaults to ./overlay.db)
//   - beefConnStr: BEEF_STORAGE env var (defaults to ./beef_storage/)
//   - redisURL: REDIS_URL env var (required for publisher)
//
// The beefConnStr parameter can be:
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
func CreateEventStorage(eventConnStr, beefConnStr, redisURL string) (storage.EventDataStorage, error) {
	// Parse beefConnStr to determine if it's a single string or array
	var beefConnStrings []string

	if beefConnStr != "" {
		// First try to parse as JSON array
		if strings.HasPrefix(strings.TrimSpace(beefConnStr), "[") {
			if err := json.Unmarshal([]byte(beefConnStr), &beefConnStrings); err != nil {
				return nil, fmt.Errorf("invalid JSON array for BEEF storage: %w", err)
			}
		} else if strings.Contains(beefConnStr, ",") {
			// If it contains commas, split it
			beefConnStrings = strings.Split(beefConnStr, ",")
			// Trim whitespace from each element
			for i, s := range beefConnStrings {
				beefConnStrings[i] = strings.TrimSpace(s)
			}
		} else {
			// Single connection string
			beefConnStrings = []string{beefConnStr}
		}
	}

	// Create BEEF storage from connection strings (defaults to ./beef_storage/ if not set)
	beefStorage, err := beef.CreateBeefStorage(beefConnStrings)
	if err != nil {
		return nil, fmt.Errorf("failed to create BEEF storage: %w", err)
	}

	// Get Redis URL for publisher
	if redisURL == "" {
		redisURL = os.Getenv("REDIS_URL")
		if redisURL == "" {
			return nil, fmt.Errorf("REDIS_URL not provided for publisher")
		}
	}

	// Create publisher (always Redis for now)
	publisher, err := publish.NewRedisPublish(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	// Create event storage from connection string (defaults to ./overlay.db if not set)
	eventStorage, err := storage.CreateEventDataStorage(eventConnStr, beefStorage, publisher)
	if err != nil {
		return nil, fmt.Errorf("failed to create event storage: %w", err)
	}

	return eventStorage, nil
}
