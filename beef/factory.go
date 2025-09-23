package beef

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// expandHomePath expands ~ to home directory if the path starts with ~/
func expandHomePath(path string) (string, error) {
	if strings.HasPrefix(path, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get home directory: %w", err)
		}
		return filepath.Join(homeDir, path[2:]), nil
	}
	return path, nil
}

// CreateBeefStorage creates a hierarchical stack of BeefStorage implementations
// from a connection string. The connection string can be:
//   - A single connection string: "redis://localhost:6379"
//   - A JSON array of connection strings: `["lru://100mb", "redis://localhost:6379", "junglebus://"]`
//   - A comma-separated list: "lru://100mb,redis://localhost:6379,junglebus://"
//   - Empty string: defaults to ~/.1sat/beef/ (falls back to ./beef/)
//
// Note: If your connection strings contain commas, use the JSON array format.
//
// Supported storage formats:
//   - lru://?size=100mb or lru://?size=1gb (in-memory LRU cache with size limit)
//   - redis://localhost:6379?ttl=24h (Redis with optional TTL parameter)
//   - sqlite:///path/to/beef.db or sqlite://beef.db
//   - file:///path/to/storage/dir
//   - s3://bucket-name/prefix/?region=us-west-2&endpoint=https://s3.amazonaws.com
//   - junglebus:// (fetches from JungleBus API)
//   - ./beef.db (inferred as SQLite)
//   - ./beef/ (inferred as filesystem)
//
// Example:
//
//	CreateBeefStorage(`["lru://?size=100mb", "redis://localhost:6379", "sqlite://beef.db", "junglebus://"]`)
//	Creates: LRU -> Redis -> SQLite -> JungleBus
func CreateBeefStorage(connectionString string) (BeefStorage, error) {
	// Parse connection string to determine if it's a single string or array
	var connectionStrings []string

	if connectionString != "" {
		// First try to parse as JSON array
		if strings.HasPrefix(strings.TrimSpace(connectionString), "[") {
			if err := json.Unmarshal([]byte(connectionString), &connectionStrings); err != nil {
				return nil, fmt.Errorf("invalid JSON array for BEEF storage: %w", err)
			}
		} else if strings.Contains(connectionString, ",") {
			// If it contains commas, split it
			connectionStrings = strings.Split(connectionString, ",")
			// Trim whitespace from each element
			for i, s := range connectionStrings {
				connectionStrings[i] = strings.TrimSpace(s)
			}
		} else {
			// Single connection string
			connectionStrings = []string{connectionString}
		}
	}

	// Create BEEF storage from connection strings (defaults to ~/.1sat/beef/ if not set)
	if len(connectionStrings) == 0 {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			connectionStrings = []string{"./beef/"} // Fallback
		} else {
			dotOneSatDir := filepath.Join(homeDir, ".1sat")
			if err := os.MkdirAll(dotOneSatDir, 0755); err != nil {
				connectionStrings = []string{"./beef/"} // Fallback if can't create dir
			} else {
				connectionStrings = []string{filepath.Join(dotOneSatDir, "beef")}
			}
		}
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
			var err error
			storage, err = NewRedisBeefStorage(connectionString, storage)
			if err != nil {
				return nil, err
			}

		case strings.HasPrefix(connectionString, "s3://"):
			// Parse S3 URL: s3://bucket-name/prefix/?region=us-west-2&endpoint=https://s3.amazonaws.com
			u, err := url.Parse(connectionString)
			if err != nil {
				return nil, fmt.Errorf("invalid S3 URL format: %w", err)
			}

			// Extract bucket and prefix from host and path
			bucket := u.Host
			prefix := strings.TrimPrefix(u.Path, "/")

			// Check for custom endpoint (for MinIO/S3-compatible)
			endpoint := u.Query().Get("endpoint")
			region := u.Query().Get("region")

			// Create S3 client configuration
			var s3Storage *S3BeefStorage
			if endpoint != "" || region != "" {
				// Custom configuration needed
				cfg, err := CreateS3Config(endpoint, region)
				if err != nil {
					return nil, fmt.Errorf("failed to create S3 config: %w", err)
				}
				client := NewS3ClientFromConfig(cfg)
				s3Storage = NewS3BeefStorageWithClient(client, bucket, prefix, storage)
			} else {
				// Use default AWS configuration
				s3Storage, err = NewS3BeefStorage(bucket, prefix, storage)
				if err != nil {
					return nil, err
				}
			}
			storage = s3Storage

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
			// Expand ~ to home directory
			expandedPath, err := expandHomePath(path)
			if err != nil {
				return nil, err
			}
			storage, err = NewSQLiteBeefStorage(expandedPath, storage)
			if err != nil {
				return nil, err
			}

		case strings.HasPrefix(connectionString, "file://"):
			// Remove file:// prefix
			path := strings.TrimPrefix(connectionString, "file://")
			if path == "" {
				path = "./beef"
			}
			// Expand ~ to home directory
			expandedPath, err := expandHomePath(path)
			if err != nil {
				return nil, err
			}
			storage, err = NewFilesystemBeefStorage(expandedPath, storage)
			if err != nil {
				return nil, err
			}

		case strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"):
			// Looks like a SQLite database file
			// Expand ~ to home directory
			expandedPath, err := expandHomePath(connectionString)
			if err != nil {
				return nil, err
			}
			storage, err = NewSQLiteBeefStorage(expandedPath, storage)
			if err != nil {
				return nil, err
			}

		case filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../") || strings.HasPrefix(connectionString, "~/"):
			// Looks like a filesystem path
			// Expand ~ to home directory
			expandedPath, err := expandHomePath(connectionString)
			if err != nil {
				return nil, err
			}
			
			// If it ends with a known DB extension, treat as SQLite
			if strings.HasSuffix(expandedPath, ".db") || strings.HasSuffix(expandedPath, ".sqlite") {
				storage, err = NewSQLiteBeefStorage(expandedPath, storage)
				if err != nil {
					return nil, err
				}
			} else {
				// Otherwise treat as filesystem storage directory
				storage, err = NewFilesystemBeefStorage(expandedPath, storage)
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

	// Wrap the entire chain with deduplication
	return NewDedupBeefStorage(storage), nil
}

// CreateValidatingBeefStorage creates a BeefStorage with merkle proof validation.
// It wraps the storage created by CreateBeefStorage with a ValidatingBeefStorage layer
// that validates merkle proofs on load and updates invalid ones.
func CreateValidatingBeefStorage(connectionString string, chainTracker chaintracker.ChainTracker) (BeefStorage, error) {
	// Create the base storage chain
	storage, err := CreateBeefStorage(connectionString)
	if err != nil {
		return nil, err
	}

	// Wrap with validation if chainTracker is provided
	if chainTracker != nil {
		storage = NewValidatingBeefStorage(storage, chainTracker)
	}

	return storage, nil
}
