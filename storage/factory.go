package storage

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/headers"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
)

// CreateEventDataStorage creates the appropriate EventDataStorage implementation
// from a connection string. Auto-detects the storage type from the URL scheme.
//
// Supported formats:
//   - redis://localhost:6379 (deprecated)
//   - postgresql://user:pass@localhost:5432/dbname or postgres://...
//   - user:pass@tcp(host:port)/dbname or mysql://... (MySQL)
//   - mongodb://localhost:27017/dbname
//   - sqlite:///path/to/overlay.db or sqlite://overlay.db
//   - ./overlay.db (inferred as SQLite)
//
// If no connection string is provided, defaults to ./overlay.db
// The beefStore, queueStorage, pubSub, and headersClient parameters are required dependencies.
func CreateEventDataStorage(connectionString string, beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubSub pubsub.PubSub, headersClient *headers.Client) (*EventDataStorage, error) {
	// Create factory function based on storage type
	var factory TopicDataStorageFactory

	switch {
	case strings.HasPrefix(connectionString, "redis://"):
		// Redis is deprecated - fallback to single-topic behavior for now
		return nil, fmt.Errorf("redis storage is deprecated, please use postgresql, mongodb or sqlite")

	case strings.HasPrefix(connectionString, "postgresql://"), strings.HasPrefix(connectionString, "postgres://"):
		// Create PostgreSQL factory function
		factory = func(topic string) (TopicDataStorage, error) {
			return NewPostgresTopicDataStorage(topic, connectionString, beefStore, queueStorage, pubSub, headersClient)
		}

	case strings.Contains(connectionString, "@tcp("), strings.Contains(connectionString, "mysql://"):
		// Create MySQL factory function
		// Supports both DSN format (user:pass@tcp(host:port)/dbname) and URL format (mysql://...)
		factory = func(topic string) (TopicDataStorage, error) {
			return NewMySQLTopicDataStorage(topic, connectionString, beefStore, queueStorage, pubSub, headersClient)
		}

	case strings.HasPrefix(connectionString, "mongodb://"), strings.HasPrefix(connectionString, "mongo://"):
		// Create MongoDB factory function
		factory = func(topic string) (TopicDataStorage, error) {
			return NewMongoTopicDataStorage(topic, connectionString, beefStore, queueStorage, pubSub, headersClient)
		}

	case connectionString == "", strings.HasPrefix(connectionString, "sqlite://"), strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"), filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../"):
		// SQLite (default or explicit path)
		factory = func(topic string) (TopicDataStorage, error) {
			return NewSQLiteTopicDataStorage(topic, connectionString, beefStore, queueStorage, pubSub, headersClient)
		}

	default:
		return nil, fmt.Errorf("unrecognized connection string format: %s", connectionString)
	}

	return NewEventDataStorage(factory, beefStore, queueStorage, pubSub), nil
}
