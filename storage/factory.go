package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
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
// The beefStore, queueStorage, and pubSub parameters are required dependencies.
func CreateEventDataStorage(connectionString string, beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubSub pubsub.PubSub) (EventDataStorage, error) {
	// If no connection string provided, default to ~/.1sat directory
	if connectionString == "" {
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

	// Detect storage type from connection string and create storage
	switch {
	case strings.HasPrefix(connectionString, "redis://"):
		return NewRedisEventDataStorage(connectionString, beefStore, queueStorage, pubSub)

	case strings.HasPrefix(connectionString, "mongodb://"), strings.HasPrefix(connectionString, "mongo://"):
		// MongoDB driver will extract database name from the connection string
		return NewMongoEventDataStorage(connectionString, beefStore, queueStorage, pubSub)

	case strings.HasPrefix(connectionString, "sqlite://"):
		// Remove sqlite:// prefix (can be sqlite:// or sqlite:///path)
		path := strings.TrimPrefix(connectionString, "sqlite://")
		path = strings.TrimPrefix(path, "/") // Handle sqlite:///path format
		if path == "" {
			path = "./overlay.db"
		}
		return NewSQLiteEventDataStorage(path, beefStore, queueStorage, pubSub)

	case strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"):
		// Looks like a SQLite database file
		return NewSQLiteEventDataStorage(connectionString, beefStore, queueStorage, pubSub)

	case filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../"):
		// Looks like a filesystem path - assume SQLite
		if !strings.HasSuffix(connectionString, ".db") {
			connectionString = connectionString + "/overlay.db"
		}
		return NewSQLiteEventDataStorage(connectionString, beefStore, queueStorage, pubSub)

	default:
		return nil, fmt.Errorf("unable to determine storage type from connection string: %s", connectionString)
	}
}
