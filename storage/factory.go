package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
)

// CreateEventDataStorage creates the appropriate EventDataStorage implementation
// from a connection string. Auto-detects the storage type from the URL scheme.
//
// Supported formats:
//   - redis://localhost:6379
//   - mongodb://localhost:27017/dbname
//   - sqlite:///path/to/overlay.db or sqlite://overlay.db
//   - ./overlay.db (inferred as SQLite)
//
// If no connection string is provided, defaults to ./overlay.db
// The pubSubURL parameter is optional and specifies the pub/sub backend:
//   - redis://localhost:6379 (for Redis pub/sub)
//   - channels:// (for in-memory channel-based pub/sub)
//   - "" (defaults to channels://)
func CreateEventDataStorage(connectionString string, beefStore beef.BeefStorage, pubSubURL string) (EventDataStorage, error) {
	// If no connection string provided, try EVENT_STORAGE environment variable
	if connectionString == "" {
		connectionString = os.Getenv("EVENT_STORAGE")
		if connectionString == "" {
			// Default to ~/.1sat directory
			homeDir, err := os.UserHomeDir()
			if err != nil {
				connectionString = "./overlay.db" // Fallback
			} else {
				dotOneSatDir := filepath.Join(homeDir, ".1sat")
				if err := os.MkdirAll(dotOneSatDir, 0755); err != nil {
					connectionString = "./overlay.db" // Fallback if can't create dir
				} else {
					connectionString = filepath.Join(dotOneSatDir, "overlay.db")
				}
			}
		}
	}

	// Create PubSub implementation
	var pubSubImpl pubsub.PubSub
	if pubSubURL == "" {
		pubSubURL = os.Getenv("PUBSUB_URL")
		if pubSubURL == "" {
			pubSubURL = "channels://" // Default to channel-based pub/sub
		}
	}

	switch {
	case strings.HasPrefix(pubSubURL, "redis://"):
		// Create Redis-based pub/sub
		if redisPubSub, err := pubsub.NewRedisPubSub(pubSubURL); err != nil {
			return nil, fmt.Errorf("failed to create Redis pub/sub: %w", err)
		} else {
			pubSubImpl = redisPubSub
		}

	case strings.HasPrefix(pubSubURL, "channels://"), pubSubURL == "":
		// Create channel-based pub/sub (no dependencies)
		pubSubImpl = pubsub.NewChannelPubSub()

	default:
		return nil, fmt.Errorf("unsupported pub/sub URL scheme: %s", pubSubURL)
	}

	// Detect storage type from connection string and create storage with pub/sub
	switch {
	case strings.HasPrefix(connectionString, "redis://"):
		return NewRedisEventDataStorage(connectionString, beefStore, pubSubImpl)

	case strings.HasPrefix(connectionString, "mongodb://"), strings.HasPrefix(connectionString, "mongo://"):
		// MongoDB driver will extract database name from the connection string
		return NewMongoEventDataStorage(connectionString, beefStore, pubSubImpl)

	case strings.HasPrefix(connectionString, "sqlite://"):
		// Remove sqlite:// prefix (can be sqlite:// or sqlite:///path)
		path := strings.TrimPrefix(connectionString, "sqlite://")
		path = strings.TrimPrefix(path, "/") // Handle sqlite:///path format
		if path == "" {
			path = "./overlay.db"
		}
		return NewSQLiteEventDataStorage(path, beefStore, pubSubImpl)

	case strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"):
		// Looks like a SQLite database file
		return NewSQLiteEventDataStorage(connectionString, beefStore, pubSubImpl)

	case filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../"):
		// Looks like a filesystem path - assume SQLite
		if !strings.HasSuffix(connectionString, ".db") {
			connectionString = connectionString + "/overlay.db"
		}
		return NewSQLiteEventDataStorage(connectionString, beefStore, pubSubImpl)

	default:
		return nil, fmt.Errorf("unable to determine storage type from connection string: %s", connectionString)
	}
}
