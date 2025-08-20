package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CreateQueueStorage creates a QueueStorage implementation from a connection string
func CreateQueueStorage(connString string) (QueueStorage, error) {
	if connString == "" {
		// Default to SQLite in ~/.1sat/queue.db
		return NewSQLiteQueueStorage(getDefaultQueuePath())
	}

	switch {
	case strings.HasPrefix(connString, "redis://"):
		return NewRedisQueueStorage(connString)
	case strings.HasPrefix(connString, "mongodb://"):
		return NewMongoQueueStorage(connString)
	case strings.HasSuffix(connString, ".db") || !strings.Contains(connString, "://"):
		return NewSQLiteQueueStorage(connString)
	default:
		return nil, fmt.Errorf("unsupported queue storage URL: %s", connString)
	}
}

// getDefaultQueuePath returns the default SQLite queue storage path
func getDefaultQueuePath() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "./queue.db" // Fallback to current directory
	}

	dotOneSatDir := filepath.Join(homeDir, ".1sat")
	if err := os.MkdirAll(dotOneSatDir, 0755); err != nil {
		return "./queue.db" // Fallback if can't create directory
	}

	return filepath.Join(dotOneSatDir, "queue.db")
}