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
//   - redis://localhost:6379 (deprecated)
//   - mongodb://localhost:27017/dbname
//   - sqlite:///path/to/overlay.db or sqlite://overlay.db
//   - ./overlay.db (inferred as SQLite)
//
// If no connection string is provided, defaults to ./overlay.db
// The beefStore, queueStorage, and pubSub parameters are required dependencies.
func CreateEventDataStorage(connectionString string, beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubSub pubsub.PubSub) (*EventDataStorage, error) {
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

	// Create factory function based on storage type
	var factory TopicDataStorageFactory

	switch {
	case strings.HasPrefix(connectionString, "redis://"):
		// Redis is deprecated - fallback to single-topic behavior for now
		return nil, fmt.Errorf("redis storage is deprecated, please use mongodb or sqlite")

	case strings.HasPrefix(connectionString, "mongodb://"), strings.HasPrefix(connectionString, "mongo://"):
		// Create MongoDB factory function
		factory = func(topic string) (TopicDataStorage, error) {
			return NewMongoTopicDataStorage(topic, connectionString, beefStore, queueStorage, pubSub)
		}

	case strings.HasPrefix(connectionString, "sqlite://"):
		// Remove sqlite:// prefix (can be sqlite:// or sqlite:///path)
		basePath := strings.TrimPrefix(connectionString, "sqlite://")
		basePath = strings.TrimPrefix(basePath, "/") // Handle sqlite:///path format
		if basePath == "" {
			basePath = "./overlay.db"
		}
		// Remove .db extension to create base path for topic databases
		if strings.HasSuffix(basePath, ".db") {
			basePath = strings.TrimSuffix(basePath, ".db")
		}
		
		// Create SQLite factory function
		factory = func(topic string) (TopicDataStorage, error) {
			topicPath := fmt.Sprintf("%s_%s.db", basePath, topic)
			return NewSQLiteTopicDataStorage(topic, topicPath, beefStore, queueStorage, pubSub)
		}

	case strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"):
		// Remove extension to create base path for topic databases
		basePath := strings.TrimSuffix(connectionString, filepath.Ext(connectionString))
		
		// Create SQLite factory function
		factory = func(topic string) (TopicDataStorage, error) {
			topicPath := fmt.Sprintf("%s_%s.db", basePath, topic)
			return NewSQLiteTopicDataStorage(topic, topicPath, beefStore, queueStorage, pubSub)
		}

	case filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../"):
		// Looks like a filesystem path - assume SQLite
		basePath := connectionString
		if strings.HasSuffix(connectionString, ".db") {
			basePath = strings.TrimSuffix(connectionString, ".db")
		} else if !strings.HasSuffix(connectionString, "/") {
			basePath = connectionString + "/overlay"
		} else {
			basePath = connectionString + "overlay"
		}
		
		// Create SQLite factory function
		factory = func(topic string) (TopicDataStorage, error) {
			topicPath := fmt.Sprintf("%s_%s.db", basePath, topic)
			return NewSQLiteTopicDataStorage(topic, topicPath, beefStore, queueStorage, pubSub)
		}

	default:
		return nil, fmt.Errorf("unable to determine storage type from connection string: %s", connectionString)
	}

	return NewEventDataStorage(factory, beefStore, queueStorage, pubSub), nil
}
