// Package storage provides abstractions and implementations for storing and retrieving
// transaction outputs in overlay services.
//
// The package includes implementations for:
//   - PostgreSQL: Scalable relational database with connection pooling
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
// engine.Storage with event-based querying and data management.
//
// Storage implementations handle:
//   - Output insertion and updates
//   - Spent/unspent status tracking
//   - Block height updates
//   - Topic membership management
//   - Applied transaction tracking
//   - Event management and querying
//
// Example Usage:
//
//	// Create SQLite storage with EventDataStorage interface
//	storage, err := storage.CreateEventDataStorage(
//	    "/path/to/database.db",
//	    beefStore,
//	    queueStorage,
//	    pubSub,
//	    headersClient
//	)
//
//	// Insert an output
//	err = storage.InsertOutput(ctx, output, "my-topic")
//
//	// Find outputs for a topic
//	outputs, err := storage.FindUTXOsForTopic(ctx, "my-topic", 0, 100, true)
//
//	// Save events for an output
//	err = storage.SaveEvents(ctx, outpoint, []string{"event1", "event2"}, "topic", 850000.000000123, eventData)
package storage
