package config

import (
	"fmt"
	"os"
	
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
// Example configurations:
//
// 1. All Redis:
//    CreateEventStorage("redis://localhost:6379", "redis://localhost:6379", "redis://localhost:6379")
//
// 2. MongoDB for events, Redis for BEEF (recommended for production):
//    CreateEventStorage("mongodb://localhost:27017/bsv21", "redis://localhost:6379", "redis://localhost:6379")
//
// 3. SQLite for events, filesystem for BEEF (good for development):
//    CreateEventStorage("./overlay.db", "./beef_storage/", "redis://localhost:6379")
//
// 4. Use environment variables:
//    CreateEventStorage("", "", "")
func CreateEventStorage(eventConnStr, beefConnStr, redisURL string) (storage.EventDataStorage, error) {
	// Create BEEF storage from connection string (defaults to ./beef_storage/ if not set)
	beefStorage, err := beef.CreateBeefStorage(beefConnStr)
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