# Overlay

A Go library providing infrastructure components for building BSV overlay services. This library includes event-based lookup services, storage abstractions, BEEF handling, and transaction processing infrastructure.

## Overview

The overlay library provides infrastructure components for building BSV overlay services that implement the core interfaces defined in [go-overlay-services](https://github.com/bsv-blockchain/go-overlay-services):

- **`lookup/events`** - Event-based lookup services implementing `engine.LookupService`
- **`storage`** - Storage backends implementing `engine.Storage` (Redis, MongoDB, SQLite)
- **`queue`** - Database-agnostic cache and queue operations (sets, hashes, sorted sets)
- **`beef`** - BEEF storage implementing `beef.BeefStorage`
- **`pubsub`** - Pub/sub system with Redis publishing and SSE broadcasting
- **`subscriber`** - JungleBus subscription management for simple transaction streaming use cases

When building overlay services, you implement `engine.TopicManager` for your protocol-specific admission logic, while this library provides the storage and indexing infrastructure through `engine.LookupService` implementations.

## Installation

```bash
go get github.com/b-open-io/overlay
```

## BEEF Storage

The beef package provides implementations of the `beef.BeefStorage` interface with support for caching and fallback chains.

### Available BEEF Storage Backends

```go
// Filesystem storage (default, no dependencies)
beefStore, err := beef.NewFilesystemBeefStorage("/path/to/beef/directory")

// SQLite BEEF storage
beefStore, err := beef.NewSQLiteBeefStorage("/path/to/beef.db")

// Redis BEEF storage with optional TTL
beefStore, err := beef.NewRedisBeefStorage(
    "redis://localhost:6379?ttl=24h",  // TTL for cache expiration
    fallbackStorage,                    // Optional fallback storage
)

// LRU in-memory cache with size limit
beefStore, err := beef.NewLRUBeefStorage(
    100 * 1024 * 1024,  // 100MB cache size
    fallbackStorage,     // Falls back to disk/database
)

// JungleBus integration for fetching from network
beefStore, err := beef.NewJungleBusBeefStorage(
    "https://junglebus.gorillapool.io",
    fallbackStorage,  // Optional local cache
)
```

### BEEF Storage Operations

```go
// Save BEEF data
err = beefStore.SaveBeef(ctx, txid, beefBytes)

// Load BEEF data
beefBytes, err := beefStore.LoadBeef(ctx, txid)

// Load a transaction with dependencies (standalone function)
tx, err := beef.LoadTx(ctx, beefStore, txid, chaintracker)
```

### Chaining BEEF Storage

Chain storage backends for caching and fallback:

```go
// Create a multi-tier storage system
diskStorage := beef.NewFilesystemBeefStorage("/data/beef")
redisCache := beef.NewRedisBeefStorage("redis://localhost:6379?ttl=1h", diskStorage)
lruCache := beef.NewLRUBeefStorage(50*1024*1024, redisCache)  // 50MB LRU -> Redis -> Disk

// Reads check LRU first, then Redis, then disk
// Writes go to all layers
beefBytes, err := lruCache.LoadBeef(ctx, txid)
```

## Storage

The storage package provides implementations of the `engine.Storage` interface for storing and retrieving transaction outputs. The `EventDataStorage` interface extends `engine.Storage` with event-specific operations and integrates with the separate `queue` package for cache and queue management.

### Storage Factory Configuration

The easiest way to create storage is using the factory with connection strings:

```go
import "github.com/b-open-io/overlay/config"

// Create storage from environment or flags
storage, err := config.CreateEventStorage(
    eventStorageURL,  // Event storage: "mongodb://localhost:27017/mydb", "./overlay.db" 
    beefStorageURL,   // BEEF storage: "lru://1gb,redis://localhost:6379,junglebus://"
    queueStorageURL,  // Queue storage: "redis://localhost:6379", "./queue.db"
    pubsubURL,        // PubSub: "redis://localhost:6379", "channels://" (default)
)
```

**Default Configuration** (no dependencies):
- Event Storage: `~/.1sat/overlay.db` (SQLite)
- BEEF Storage: `~/.1sat/beef/` (filesystem)
- Queue Storage: `~/.1sat/queue.db` (SQLite)
- PubSub: `channels://` (in-memory Go channels)

**Connection String Formats**:
- **Redis**: `redis://[user:password@]host:port`
- **MongoDB**: `mongodb://[user:password@]host:port/database`
- **SQLite**: `./database.db` or `sqlite://path/to/db`
- **Channels**: `channels://` (no external dependencies)

### Storage Backend Examples

```go
// SQLite Storage (default)
storage, err := storage.NewSQLiteEventDataStorage(
    "/path/to/database.db",
    beefStore,
    publisher
)

// Redis Storage
storage, err := storage.NewRedisEventDataStorage(
    "redis://localhost:6379",
    beefStore,
    publisher
)

// MongoDB Storage
storage, err := storage.NewMongoEventDataStorage(
    "mongodb://localhost:27017/mydb",
    beefStore,
    publisher
)
```

### Basic Storage Operations

```go
// Insert an output
err = storage.InsertOutput(ctx, output)

// Find outputs by topic
outputs, err := storage.FindUTXOsForTopic(ctx, "my-topic", 0, 100)

// Load topic-contextualized BEEF with merged AncillaryBeef
beef, err := storage.LoadBeefByTxidAndTopic(ctx, txid, "my-topic")

// Mark outputs as spent
err = storage.MarkOutputsSpent(ctx, outpoints)
```

## Basic Querying

Once your protocol is indexing events, you can query them using simple event-based lookups:

### Single Event Queries

```go
// Query outputs by a single event type
FALSE := false
answer, err := tokenLookup.Lookup(ctx, &lookup.LookupQuestion{
    Query: json.Marshal(&events.Question{
        Event:    "type:mint",    // Single event query
        From:     850000,         // Starting from block 850000
        Limit:    100,
        Spent:    &FALSE,         // Only unspent outputs
        Reverse:  false,          // Forward chronological order
    }),
})

// Find all tokens owned by a specific address
answer, err := tokenLookup.Lookup(ctx, &lookup.LookupQuestion{
    Query: json.Marshal(&events.Question{
        Event:    "owner:1ABC...xyz",  // All tokens for this owner
        From:     0,                   // From genesis
        Limit:    50,
    }),
})
```

### Value Aggregation

```go
// Simple value aggregation for a single event
totalSupply, count, err := tokenLookup.ValueSumUint64(ctx, "sym:GOLD", events.SpentStatusUnspent)

// Find all events for a specific output
events, err := tokenLookup.FindEvents(ctx, outpoint)
// Returns: ["token:abc123", "type:transfer", "owner:1ABC...xyz:abc123"]
```

### Event Naming Patterns

Design event names to support the queries your protocol needs. Events can be hierarchical (using colons as separators) to enable prefix-based searching:

```go
// BSV21 token protocol patterns
"id:abc123"                    // Token by ID
"sym:GOLD"                     // Token by symbol  
"p2pkh:1ABC...xyz:abc123"      // Address-specific token events
"list:1SELLER...xyz:abc123"    // Marketplace listings by seller
"type:mint"                    // Operation type events

// Example patterns for other protocols
"nft:collection:abc"           // NFT by collection
"vote:proposal:123"            // Governance votes
"auction:item:456"             // Auction events
"social:user:alice"            // Social protocol events
```

## Advanced Querying

For complex queries, search for multiple events using join operations:

```go
// Find outputs with ANY of these token operations (Union)
FALSE := false
question := &events.Question{
    Events:   []string{"type:mint", "type:transfer", "type:burn"},
    JoinType: &events.JoinTypeUnion,
    From:     850000,  // Starting from block 850000
    Limit:    50,
    Spent:    &FALSE, // Only unspent outputs
}

// Find tokens that have ALL of these events (Intersection)
// Useful for finding tokens that meet multiple criteria
question = &events.Question{
    Events:   []string{"list:abc123", "verified:abc123"},
    JoinType: &events.JoinTypeIntersect,
    From:     850000.000000123, // Can specify exact position with block index
    Limit:    20,
}

// Find tokens with first event but NOT the others (Difference)
// Useful for exclusion queries
question = &events.Question{
    Events:   []string{"tradeable:abc123", "locked:abc123", "frozen:abc123"},
    JoinType: &events.JoinTypeDifference, // Has "tradeable" but not "locked" or "frozen"
    From:     0,
    Limit:    100,
}
```

## Queue Management

The `queue` package provides database-agnostic cache and queue operations through the `QueueStorage` interface. Access queue operations via `storage.GetQueueStorage()`:

### Sorted Sets (Priority Queues)

```go
// Add items to a priority queue
queueStore := storage.GetQueueStorage()
err = queueStore.ZAdd(ctx, "processing-queue", 
    queue.ScoredMember{Score: 850000.000000123, Member: "tx1"},
    queue.ScoredMember{Score: 850000.000000124, Member: "tx2"},
)

// Get items by score range
members, err := queueStore.ZRangeByScore(ctx, "processing-queue", 
    850000, 851000,  // min, max scores
    0, 100)         // offset, limit (0 = no limit)

// Get score for a specific member
score, err := queueStore.ZScore(ctx, "processing-queue", "tx1")

// Increment score atomically
newScore, err := queueStore.ZIncrBy(ctx, "processing-queue", "tx1", 0.001)

// Remove processed items
err = queueStore.ZRem(ctx, "processing-queue", "tx1", "tx2")
```

### Sets (Whitelists, Blacklists)

```go
// Add to whitelist
err = queueStore.SAdd(ctx, "token-whitelist", "token1", "token2", "token3")

// Check membership
members, err := queueStore.SMembers(ctx, "token-whitelist")

// Remove from whitelist
err = queueStore.SRem(ctx, "token-whitelist", "token2")
```

### Hashes (Configuration, State)

```go
// Set individual field
err = queueStore.HSet(ctx, "config", "max-size", "1000")

// Get single field
value, err := queueStore.HGet(ctx, "config", "max-size")

// Get all fields
allConfig, err := queueStore.HGetAll(ctx, "config")

// Delete fields
err = queueStore.HDel(ctx, "config", "old-field")
```

## PubSub

The pubsub package provides a unified publish/subscribe system for real-time events and SSE streaming:

### RedisPubSub - Publishing and SSE Broadcasting

```go
// Create Redis pub/sub handler
pubsub, err := pubsub.NewRedisPubSub("redis://localhost:6379")

// Publish an event with optional score for consistency
err = pubsub.Publish(ctx, "topic", data, score)

// Get recent events for SSE reconnection
events, err := pubsub.GetRecentEvents(ctx, "topic", sinceScore)

// Start broadcasting Redis events to SSE clients
go pubsub.StartBroadcasting(ctx)
```

### SSESync - Peer Synchronization

```go
// Create SSE sync manager
sseSync := pubsub.NewSSESync(engine, storage)

// Start SSE sync with peer-to-topics mapping
peerTopics := map[string][]string{
    "https://peer1.com": {"topic1", "topic2"},
    "https://peer2.com": {"topic3"},
}
err = sseSync.Start(ctx, peerTopics)
```

### PeerBroadcaster - Transaction Broadcasting

```go
// Create peer broadcaster with topic mapping
broadcaster := pubsub.NewPeerBroadcaster(peerTopics)

// Broadcast successful transactions to peers
err = broadcaster.BroadcastTransaction(ctx, taggedBEEF)
```

### LibP2PSync - Decentralized Peer Synchronization

LibP2P provides decentralized transaction synchronization with automatic peer discovery:

```go
// Create LibP2P sync manager with Bitcoin identity
libp2pSync, err := pubsub.NewLibP2PSync(engine, storage, beefStorage)

// Start sync for specific topics
topics := []string{"tm_tokenId1", "tm_tokenId2"}
err = libp2pSync.Start(ctx, topics)

// Publish transaction to LibP2P mesh
err = libp2pSync.PublishTxid(ctx, "tm_tokenId", txid)
```

**Key Features:**
- **Bitcoin Identity**: Uses secp256k1 keys stored in `~/.1sat/libp2p.key`
- **Automatic Peer Discovery**: mDNS for local networks, DHT for global discovery
- **Custom BEEF Protocol**: `/bsv-overlay/beef/1.0.0` for direct peer-to-peer BEEF requests
- **Topic-Contextualized BEEF**: Serves merged BEEF with AncillaryBeef per topic
- **Transaction-Level Sync**: Publishes txid (not outpoints) eliminating deduplication complexity

**Bootstrap Node:**
```bash
# Run standalone bootstrap node for network connectivity
./bootstrap -p 4001

# Other overlay services can connect to bootstrap addresses for peer discovery
```

**Integration with Overlay Services:**
```go
// Enable LibP2P sync alongside existing SSESync
if LIBP2P_SYNC {
    libp2pSync, err := pubsub.NewLibP2PSync(engine, storage, beefStorage)
    if err := libp2pSync.Start(ctx, topics); err != nil {
        log.Printf("Failed to start LibP2P sync: %v", err)
    }
}
```

## Routes Package

The routes package provides reusable HTTP route handlers for common overlay service functionality.

### Common Routes

Register standard event, block, and BEEF serving endpoints:

```go
import "github.com/b-open-io/overlay/routes"

// Create API group
onesat := app.Group("/api/1sat")

// Register common routes
routes.RegisterCommonRoutes(onesat, &routes.CommonRoutesConfig{
    Storage:      store,
    ChainTracker: chaintracker,
})
```

**Endpoints:**
- `GET/POST /events/:topic/:event/(history|unspent)` - Event-based queries
- `GET /block/(tip|:height)` - Block information
- `GET /beef/:topic/:txid` - BEEF data serving

**Utility:**
- `routes.ParseEventQuery(c) *storage.EventQuestion` - Parse query parameters (`from`, `limit`)

### SSE Streaming Routes

Add Server-Sent Events streaming:

```go
routes.RegisterSSERoutes(onesat, &routes.SSERoutesConfig{
    Storage: store,
    Context: ctx,
})
```

**Endpoints:**
- `GET /subscribe/:events` - SSE stream with resumption support

### Enhanced Submit Routes

Register transaction submission with peer broadcasting:

```go
routes.RegisterSubmitRoutes(app, &routes.SubmitRoutesConfig{
    Engine:          engine,
    LibP2PSync:      libp2pSync,     // Optional
    PeerBroadcaster: broadcaster,    // Optional
})
```

**Endpoints:**
- `POST /api/v1/submit` - Enhanced transaction submission

## Sync Package

The sync package provides peer synchronization management.

### SSE Sync

```go
import "github.com/b-open-io/overlay/sync"

// Configure peer-topic mapping
peerTopics := map[string][]string{
    "https://peer1.com": {"topic1", "topic2"},
    "https://peer2.com": {"topic3"},
}

// Register SSE sync
sseSyncManager, err := sync.RegisterSSESync(&sync.SSESyncConfig{
    Engine:     engine,
    Storage:    store,
    PeerTopics: peerTopics,
    Context:    ctx,
})

defer sseSyncManager.Stop()
```

### LibP2P Sync

```go
// Configure topics
topics := []string{"tm_token1", "tm_token2"}

// Register LibP2P sync
libp2pSyncManager, err := sync.RegisterLibP2PSync(&sync.LibP2PSyncConfig{
    Engine:  engine,
    Storage: store,
    Topics:  topics,
    Context: ctx,
})

// Access LibP2P instance for other components
libp2pSync := libp2pSyncManager.GetLibP2PSync()

defer libp2pSyncManager.Stop()
```

## Building Overlay Services

### Topic Manager Implementation

Topic managers implement the `engine.TopicManager` interface from go-overlay-services. They examine incoming transactions and decide which outputs are relevant to your protocol:

```go
type MyTokenTopicManager struct {
    // Your protocol-specific fields
}

// IdentifyAdmissibleOutputs examines a transaction and returns admission instructions
func (tm *MyTokenTopicManager) IdentifyAdmissibleOutputs(ctx context.Context, 
    beef []byte, previousCoins map[uint32]*transaction.TransactionOutput) (overlay.AdmittanceInstructions, error) {
    
    // Parse the transaction from BEEF
    _, tx, _, err := transaction.ParseBeef(beef)
    if err != nil {
        return overlay.AdmittanceInstructions{}, err
    }
    
    var outputsToAdmit []*overlay.Output
    
    // Check each output
    for vout, output := range tx.Outputs {
        // Your protocol logic - does this output belong in your topic?
        if isMyProtocol(output.LockingScript) {
            outputsToAdmit = append(outputsToAdmit, &overlay.Output{
                Vout: uint32(vout),
            })
        }
    }
    
    return overlay.AdmittanceInstructions{
        Outputs: outputsToAdmit,
    }, nil
}

// IdentifyNeededInputs returns any inputs needed for validation (optional)
func (tm *MyTokenTopicManager) IdentifyNeededInputs(ctx context.Context, beef []byte) ([]*transaction.Outpoint, error) {
    // Return any inputs you need to validate the transaction
    // Often returns nil for simple protocols
    return nil, nil
}

func (tm *MyTokenTopicManager) GetDocumentation() string {
    return "My token protocol topic manager"
}

func (tm *MyTokenTopicManager) GetMetaData() *overlay.MetaData {
    return &overlay.MetaData{
        Name: "MyToken",
        // Other metadata
    }
}
```

### Lookup Service Implementation

After your topic manager admits outputs, the lookup service processes them to create searchable events. Implement the `engine.LookupService` interface by embedding one of the existing event lookup implementations:

```go
// Define your custom lookup service by embedding an existing implementation
type TokenLookup struct {
    events.MongoEventLookup  // Choose MongoDB, Redis, or SQLite
}

// Create a constructor for your lookup service
func NewTokenLookup(connString, dbName string, storage engine.Storage) (*TokenLookup, error) {
    mongo, err := events.NewMongoEventLookup(connString, dbName, storage)
    if err != nil {
        return nil, err
    }
    return &TokenLookup{MongoEventLookup: *mongo}, nil
}

// Implement OutputAdmittedByTopic - this is where your protocol logic lives
func (l *TokenLookup) OutputAdmittedByTopic(ctx context.Context, payload *engine.OutputAdmittedByTopic) error {
    // Parse the locking script to identify token operations
    if tokenData := parseTokenScript(payload.LockingScript); tokenData != nil {
        // Create compound event names for efficient searching
        events := []string{
            fmt.Sprintf("token:%s", tokenData.ID),              // token:abc123
            fmt.Sprintf("type:%s", tokenData.Type),             // type:mint or type:transfer
            fmt.Sprintf("owner:%s:%s", tokenData.Owner, tokenData.ID), // owner:1ABC...xyz:abc123
        }
        
        // Add symbol-based events for tokens with symbols
        if tokenData.Symbol != "" {
            events = append(events, fmt.Sprintf("sym:%s", tokenData.Symbol)) // sym:GOLD
        }
        
        // Save all events with the token amount as value
        err := l.SaveEvents(ctx, payload.Outpoint, events, 
            blockHeight, blockIndex, tokenData.Amount)
        if err != nil {
            return err
        }
    }
    return nil
}
```

## Technical Details

### Scoring System

Events are sorted using a decimal notation score that combines block height and transaction index:
- **Mined transactions**: `score = height + index/1e9` (e.g., 850000.000000123)
  - Block height forms the integer part
  - Block index forms the decimal part (9 digits, zero-padded)
  - Supports up to 999,999,999 transactions per block with full precision
- **Unmined transactions**: `score = unix timestamp` (seconds)
  - Ensures unmined transactions sort after all mined transactions

### Backend Options

All backends provide the same interface:

- **Redis** - `redis://localhost:6379`
- **MongoDB** - `mongodb://localhost:27017/dbname`
- **SQLite** - `./overlay.db` (default)