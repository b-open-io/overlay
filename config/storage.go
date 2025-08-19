package config

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/storage"
	"github.com/redis/go-redis/v9"
)

// CreateEventStorage creates a fully configured event storage with BEEF storage and optional PubSub.
//
// Parameters:
//   - eventURL: Event storage connection string (e.g., "redis://localhost:6379", "./overlay.db")
//   - beefURL: BEEF storage connection string(s) (can be hierarchical)
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
//     CreateEventStorage("redis://localhost:6379", "redis://localhost:6379", "redis://localhost:6379")
//
//  2. MongoDB for events, hierarchical BEEF storage, Redis pubsub:
//     CreateEventStorage("mongodb://localhost:27017/bsv21", `["lru://1gb", "redis://localhost:6379", "junglebus://"]`, "redis://localhost:6379")
//
//  3. SQLite for events, filesystem for BEEF, channel pubsub:
//     CreateEventStorage("./overlay.db", "./beef_storage/", "channels://")
//
//  4. Default no-dependency setup:
//     CreateEventStorage("", "", "")  // Uses ./overlay.db, ./beef_storage/, channels://
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

	// Create Redis client if URL is provided (optional)
	var redisClient *redis.Client
	if pubsubURL != "" {
		opts, err := redis.ParseURL(pubsubURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse PubSub URL: %w", err)
		}
		redisClient = redis.NewClient(opts)
		
		// Test connection
		ctx := context.Background()
		if err := redisClient.Ping(ctx).Err(); err != nil {
			return nil, fmt.Errorf("failed to connect to PubSub Redis: %w", err)
		}
	}
	// If pubsubURL is empty, redisClient remains nil and no Redis operations will be available

	// Create event storage from connection string  
	eventStorage, err := storage.CreateEventDataStorage(eventURL, beefStorage, pubsubURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create event storage: %w", err)
	}

	return eventStorage, nil
}
