// Package beef provides storage and retrieval functionality for BEEF
// (Background Evaluation Extended Format) data in overlay services.
//
// BEEF is a compact format for representing Bitcoin transactions along with
// their merkle proofs, enabling SPV (Simplified Payment Verification) without
// needing the full blockchain.
//
// The package includes implementations for:
//   - Redis: Fast in-memory storage with optional persistence
//   - MongoDB: Document storage with replication support
//
// Storage Interface:
//
// The BeefStorage interface defines two core operations:
//   - SaveBeef: Store BEEF data associated with a transaction ID
//   - LoadBeef: Retrieve BEEF data by transaction ID
//
// Example Usage:
//
//	// Create Redis BEEF storage
//	beefStore, err := beef.NewRedisBeefStorage("redis://localhost:6379")
//
//	// Save BEEF data
//	err = beefStore.SaveBeef(ctx, &txid, beefBytes)
//
//	// Load BEEF data
//	beefData, err := beefStore.LoadBeef(ctx, &txid)
//
// Implementation Notes:
//   - BEEF data is stored as binary data (base64 encoded in Redis)
//   - Transaction IDs are used as keys for efficient lookup
//   - Storage implementations should handle concurrent access safely
package beef
