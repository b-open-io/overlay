// Package storage provides abstractions and implementations for storing and retrieving
// transaction outputs in overlay services.
//
// The package includes implementations for:
//   - Redis: High-performance in-memory storage with persistence
//   - MongoDB: Document-based storage with flexible querying
//   - SQLite: Embedded database with full SQL support and WAL mode
//
// Key Features:
//   - Transaction output storage and retrieval
//   - Topic-based output organization
//   - Interaction tracking between hosts and topics
//   - Integration with BEEF storage and event publishing
//   - Database-agnostic queue management operations
//
// EventDataStorage Interface:
//
// All storage backends implement the EventDataStorage interface which extends
// engine.Storage with Redis-like operations for queue management:
//   - Sorted sets (ZAdd, ZRange, ZScore, ZRem) for priority queues
//   - Sets (SAdd, SMembers, SRem) for whitelists and blacklists
//   - Hashes (HSet, HGet, HMSet, HGetAll) for configuration and state
//
// Storage implementations handle:
//   - Output insertion and updates
//   - Spent/unspent status tracking
//   - Block height updates
//   - Topic membership management
//   - Applied transaction tracking
//   - Queue management operations
//
// Example Usage:
//
//	// Create Redis storage with EventDataStorage interface
//	storage, err := storage.NewRedisEventDataStorage(
//	    "redis://localhost:6379",
//	    beefStore,
//	    publisher
//	)
//
//	// Insert an output
//	err = storage.InsertOutput(ctx, output)
//
//	// Find outputs for a topic
//	outputs, err := storage.FindUTXOsForTopic(ctx, "my-topic", 0, 100)
//
//	// Use queue management operations
//	err = storage.ZAdd(ctx, "processing-queue", storage.ZMember{
//	    Score: 850000.000000123,
//	    Member: "tx1",
//	})
//
//	// Create SQLite storage with EventDataStorage interface
//	sqliteStorage, err := storage.NewSQLiteEventDataStorage(
//	    "/path/to/database.db",
//	    beefStore,
//	    publisher
//	)
package storage
