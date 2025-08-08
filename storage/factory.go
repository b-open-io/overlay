package storage

import (
	"fmt"
	"os"
	"strings"
	
	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
)

// CreateEventDataStorage creates the appropriate EventDataStorage implementation
// based on the EVENT_STORAGE environment variable
func CreateEventDataStorage(dbName string, beefStore beef.BeefStorage, publisher publish.Publisher) (EventDataStorage, error) {
	storageType := strings.ToLower(os.Getenv("EVENT_STORAGE"))
	
	// Default to SQLite if not specified - no external dependencies needed
	if storageType == "" {
		storageType = "sqlite"
	}
	
	switch storageType {
	case "mongo", "mongodb":
		mongoURL := os.Getenv("MONGO_URL")
		if mongoURL == "" {
			mongoURL = "mongodb://localhost:27017"
		}
		return NewMongoEventDataStorage(mongoURL, dbName, beefStore, publisher)
		
	case "redis":
		redisURL := os.Getenv("REDIS_URL")
		if redisURL == "" {
			redisURL = "redis://localhost:6379"
		}
		return NewRedisEventDataStorage(redisURL, beefStore, publisher)
		
	case "sqlite":
		// SQLite path can be specified via SQLITE_PATH env var
		// or defaults to a local file based on dbName
		sqlitePath := os.Getenv("SQLITE_PATH")
		if sqlitePath == "" {
			sqlitePath = fmt.Sprintf("./%s.db", dbName)
		}
		return NewSQLiteEventDataStorage(sqlitePath, beefStore, publisher)
		
	default:
		return nil, fmt.Errorf("unsupported storage type: %s (supported: mongo, redis, sqlite)", storageType)
	}
}