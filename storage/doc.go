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
//
// Storage implementations handle:
//   - Output insertion and updates
//   - Spent/unspent status tracking
//   - Block height updates
//   - Topic membership management
//   - Applied transaction tracking
//
// Example Usage:
//
//	// Create Redis storage
//	storage, err := storage.NewRedisStorage(
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
//	// Create SQLite storage
//	sqliteStorage, err := storage.NewSQLiteStorage(
//	    "/path/to/database.db",
//	    beefStore,
//	    publisher
//	)
package storage
