// Package beef provides storage and retrieval functionality for BEEF
// (Background Evaluation Extended Format) data in overlay services.
//
// BEEF is a compact format for representing Bitcoin transactions along with
// their merkle proofs, enabling SPV (Simplified Payment Verification) without
// needing the full blockchain.
//
// The package includes implementations for:
//   - Redis: Fast in-memory storage with optional persistence and TTL
//   - MongoDB: Document storage with replication support
//   - SQLite: Embedded database for local deployments
//   - Filesystem: Directory-based storage for simple file persistence
//   - LRU: In-memory cache with size limits and fallback support
//   - JungleBus: Network fetching from JungleBus service
//
// Storage Interface:
//
// The BeefStorage interface defines two core operations:
//   - SaveBeef: Store BEEF data associated with a transaction ID
//   - LoadBeef: Retrieve BEEF data by transaction ID
//
// Example Usage:
//
//	// Create Redis BEEF storage with TTL and fallback
//	beefStore, err := beef.NewRedisBeefStorage("redis://localhost:6379?ttl=24h", fallbackStorage)
//
//	// Save BEEF data
//	err = beefStore.SaveBeef(ctx, &txid, beefBytes)
//
//	// Load BEEF data
//	beefData, err := beefStore.LoadBeef(ctx, &txid)
//
// Storage Chaining:
//
// Multiple storage backends can be chained for caching and fallback:
//
//	diskStorage := beef.NewFilesystemBeefStorage("/data/beef")
//	redisCache := beef.NewRedisBeefStorage("redis://localhost:6379?ttl=1h", diskStorage)
//	lruCache := beef.NewLRUBeefStorage(50*1024*1024, redisCache)  // 50MB LRU -> Redis -> Disk
//
// Implementation Notes:
//   - BEEF data is stored as binary data (base64 encoded in Redis)
//   - Transaction IDs are used as keys for efficient lookup
//   - Storage implementations should handle concurrent access safely
//   - LoadTx is now a standalone function, not a method on BeefStorage
package beef
