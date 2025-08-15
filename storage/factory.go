package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/b-open-io/overlay/beef"
	"github.com/redis/go-redis/v9"
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
func CreateEventDataStorage(connectionString string, beefStore beef.BeefStorage, pubRedis *redis.Client) (EventDataStorage, error) {
	// If no connection string provided, try EVENT_STORAGE environment variable
	if connectionString == "" {
		connectionString = os.Getenv("EVENT_STORAGE")
		if connectionString == "" {
			// Default to local SQLite database
			connectionString = "./overlay.db"
		}
	}

	// Detect storage type from connection string
	switch {
	case strings.HasPrefix(connectionString, "redis://"):
		return NewRedisEventDataStorage(connectionString, beefStore, pubRedis)

	case strings.HasPrefix(connectionString, "mongodb://"), strings.HasPrefix(connectionString, "mongo://"):
		// MongoDB driver will extract database name from the connection string
		return NewMongoEventDataStorage(connectionString, beefStore, pubRedis)

	case strings.HasPrefix(connectionString, "sqlite://"):
		// Remove sqlite:// prefix (can be sqlite:// or sqlite:///path)
		path := strings.TrimPrefix(connectionString, "sqlite://")
		path = strings.TrimPrefix(path, "/") // Handle sqlite:///path format
		if path == "" {
			path = "./overlay.db"
		}
		return NewSQLiteEventDataStorage(path, beefStore, pubRedis)

	case strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"):
		// Looks like a SQLite database file
		return NewSQLiteEventDataStorage(connectionString, beefStore, pubRedis)

	case filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../"):
		// Looks like a filesystem path - assume SQLite
		if !strings.HasSuffix(connectionString, ".db") {
			connectionString = connectionString + "/overlay.db"
		}
		return NewSQLiteEventDataStorage(connectionString, beefStore, pubRedis)

	default:
		return nil, fmt.Errorf("unable to determine storage type from connection string: %s", connectionString)
	}
}
