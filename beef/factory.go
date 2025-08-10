package beef

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CreateBeefStorage creates the appropriate BeefStorage implementation
// from a connection string. Auto-detects the storage type from the URL scheme.
//
// Supported formats:
//   - redis://localhost:6379
//   - mongodb://localhost:27017/beef
//   - sqlite:///path/to/beef.db or sqlite://beef.db
//   - file:///path/to/storage/dir
//   - ./beef.db (inferred as SQLite)
//   - ./beef_storage/ (inferred as filesystem)
//
// If no connection string is provided, defaults to ./beef_storage/ directory
func CreateBeefStorage(connectionString string) (BeefStorage, error) {
	// If no connection string provided, try BEEF_STORAGE environment variable
	if connectionString == "" {
		connectionString = os.Getenv("BEEF_STORAGE")
		if connectionString == "" {
			// Default to local filesystem storage
			connectionString = "./beef_storage"
		}
	}
	
	// Parse cache TTL from environment if using Redis
	var cacheTTL time.Duration
	if ttlStr := os.Getenv("BEEF_CACHE_TTL"); ttlStr != "" {
		if ttl, err := time.ParseDuration(ttlStr); err == nil {
			cacheTTL = ttl
		}
	}
	
	// Detect storage type from connection string
	switch {
	case strings.HasPrefix(connectionString, "redis://"):
		return NewRedisBeefStorage(connectionString, cacheTTL)
		
	case strings.HasPrefix(connectionString, "mongodb://"), strings.HasPrefix(connectionString, "mongo://"):
		// MongoDB driver will extract database name from the connection string
		return NewMongoBeefStorage(connectionString)
		
	case strings.HasPrefix(connectionString, "sqlite://"):
		// Remove sqlite:// prefix (can be sqlite:// or sqlite:///path)
		path := strings.TrimPrefix(connectionString, "sqlite://")
		path = strings.TrimPrefix(path, "/") // Handle sqlite:///path format
		if path == "" {
			path = "./beef.db"
		}
		return NewSQLiteBeefStorage(path)
		
	case strings.HasPrefix(connectionString, "file://"):
		// Remove file:// prefix
		path := strings.TrimPrefix(connectionString, "file://")
		if path == "" {
			path = "./beef_storage"
		}
		return NewFilesystemBeefStorage(path)
		
	case strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"):
		// Looks like a SQLite database file
		return NewSQLiteBeefStorage(connectionString)
		
	case filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../"):
		// Looks like a filesystem path
		// If it ends with a known DB extension, treat as SQLite
		if strings.HasSuffix(connectionString, ".db") || strings.HasSuffix(connectionString, ".sqlite") {
			return NewSQLiteBeefStorage(connectionString)
		}
		// Otherwise treat as filesystem storage directory
		return NewFilesystemBeefStorage(connectionString)
		
	default:
		return nil, fmt.Errorf("unable to determine storage type from connection string: %s", connectionString)
	}
}