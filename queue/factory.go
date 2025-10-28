package queue

import (
	"fmt"
	"path/filepath"
	"strings"
)

// CreateQueueStorage creates a QueueStorage implementation from a connection string
func CreateQueueStorage(connString string) (QueueStorage, error) {
	switch {
	case strings.HasPrefix(connString, "redis://"):
		return NewRedisQueueStorage(connString)
	case strings.HasPrefix(connString, "postgresql://"), strings.HasPrefix(connString, "postgres://"):
		return NewPostgresQueueStorage(connString)
	case strings.HasPrefix(connString, "mysql://"):
		return NewMySQLQueueStorage(connString)
	case strings.HasPrefix(connString, "mongodb://"):
		return NewMongoQueueStorage(connString)
	case connString == "", strings.HasPrefix(connString, "sqlite://"), strings.HasSuffix(connString, ".db"), filepath.IsAbs(connString) || strings.HasPrefix(connString, "./") || strings.HasPrefix(connString, "../") || !strings.Contains(connString, "://"):
		// SQLite (default or explicit path)
		return NewSQLiteQueueStorage(connString)
	default:
		return nil, fmt.Errorf("unrecognized queue storage format: %s", connString)
	}
}

