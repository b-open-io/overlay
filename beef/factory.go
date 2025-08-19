package beef

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

// CreateBeefStorage creates a hierarchical stack of BeefStorage implementations
// from a slice of connection strings. Each storage layer uses the next as its fallback.
//
// The first connection string creates the top-level storage (checked first),
// and the last creates the bottom-level fallback (checked last).
//
// Supported formats:
//   - lru://?size=100mb or lru://?size=1gb (in-memory LRU cache with size limit)
//   - redis://localhost:6379?ttl=24h (Redis with optional TTL parameter)
//   - sqlite:///path/to/beef.db or sqlite://beef.db
//   - file:///path/to/storage/dir
//   - junglebus:// (fetches from JungleBus API)
//   - ./beef.db (inferred as SQLite)
//   - ./beef_storage/ (inferred as filesystem)
//
// Example:
//
//	CreateBeefStorage([]string{"lru://?size=100mb", "redis://localhost:6379", "sqlite://beef.db", "junglebus://"})
//	Creates: LRU -> Redis -> SQLite -> JungleBus
func CreateBeefStorage(connectionStrings []string) (BeefStorage, error) {
	// Require at least one connection string
	if len(connectionStrings) == 0 {
		return nil, fmt.Errorf("no storage configurations provided")
	}

	// Build the storage stack from bottom to top
	// Start with nil fallback for the bottom layer
	var storage BeefStorage

	// Process connection strings in reverse order (bottom to top)
	for i := len(connectionStrings) - 1; i >= 0; i-- {
		connectionString := strings.TrimSpace(connectionStrings[i])

		// Create the appropriate storage with current storage as fallback
		switch {
		case strings.HasPrefix(connectionString, "lru://"):
			// Parse size from query parameter: lru://?size=100mb
			u, err := url.Parse(connectionString)
			if err != nil {
				return nil, fmt.Errorf("invalid LRU URL format: %w", err)
			}

			sizeStr := u.Query().Get("size")
			if sizeStr == "" {
				return nil, fmt.Errorf("LRU size not specified, use format: lru://?size=100mb")
			}

			size, err := ParseSize(sizeStr)
			if err != nil {
				return nil, fmt.Errorf("invalid LRU size format %s: %w", sizeStr, err)
			}
			storage = NewLRUBeefStorage(size, storage)

		case strings.HasPrefix(connectionString, "redis://"):
			// TTL is now parsed inside NewRedisBeefStorage
			var err error
			storage, err = NewRedisBeefStorage(connectionString, storage)
			if err != nil {
				return nil, err
			}

		case strings.HasPrefix(connectionString, "junglebus://"):
			// Convert junglebus://host to https://host
			// If no host specified (just "junglebus://"), use default
			host := strings.TrimPrefix(connectionString, "junglebus://")
			if host == "" {
				host = "junglebus.gorillapool.io"
			}
			junglebusURL := "https://" + host
			storage = NewJunglebusBeefStorage(junglebusURL, storage)

		case strings.HasPrefix(connectionString, "sqlite://"):
			// Remove sqlite:// prefix (can be sqlite:// or sqlite:///path)
			path := strings.TrimPrefix(connectionString, "sqlite://")
			path = strings.TrimPrefix(path, "/") // Handle sqlite:///path format
			if path == "" {
				path = "./beef.db"
			}
			var err error
			storage, err = NewSQLiteBeefStorage(path, storage)
			if err != nil {
				return nil, err
			}

		case strings.HasPrefix(connectionString, "file://"):
			// Remove file:// prefix
			path := strings.TrimPrefix(connectionString, "file://")
			if path == "" {
				path = "./beef_storage"
			}
			var err error
			storage, err = NewFilesystemBeefStorage(path, storage)
			if err != nil {
				return nil, err
			}

		case strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"):
			// Looks like a SQLite database file
			var err error
			storage, err = NewSQLiteBeefStorage(connectionString, storage)
			if err != nil {
				return nil, err
			}

		case filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../"):
			// Looks like a filesystem path
			// If it ends with a known DB extension, treat as SQLite
			if strings.HasSuffix(connectionString, ".db") || strings.HasSuffix(connectionString, ".sqlite") {
				var err error
				storage, err = NewSQLiteBeefStorage(connectionString, storage)
				if err != nil {
					return nil, err
				}
			} else {
				// Otherwise treat as filesystem storage directory
				var err error
				storage, err = NewFilesystemBeefStorage(connectionString, storage)
				if err != nil {
					return nil, err
				}
			}

		default:
			return nil, fmt.Errorf("unable to determine storage type from connection string: %s", connectionString)
		}
	}

	if storage == nil {
		return nil, fmt.Errorf("no valid storage configurations provided")
	}

	return storage, nil
}
