# Overlay

A Go library providing infrastructure components for building BSV overlay services. This library includes event-based lookup services, storage abstractions, BEEF (Background Evaluation Extended Format) handling, publishing capabilities, and transaction processing infrastructure.

## Overview

The overlay library provides a complete set of infrastructure components for building BSV overlay services. These components implement the core interfaces defined in [go-overlay-services](https://github.com/bsv-blockchain/go-overlay-services):

### Core Components

- **`lookup/events`** - Event-based lookup services implementing `engine.LookupService`
- **`storage`** - Storage backends implementing `engine.Storage` (Redis, MongoDB, SQLite)
- **`beef`** - BEEF storage implementing `beef.BeefStorage`
- **`publish`** - Event publishing implementing `publish.Publisher`
- **`subscriber`** - JungleBus subscription management for transaction streaming
- **`processor`** - Transaction processing utilities with Redis queue integration
- **`purse`** - Payment purse for funding transactions with UTXO management

### Interface Compatibility

When building overlay services with go-overlay-services, you'll implement:
- **`engine.TopicManager`** - Your custom logic for admitting outputs (not provided by this library)
- **`engine.LookupService`** - Provided by embedding this library's event lookup implementations

This library focuses on the storage and indexing infrastructure, while you define the protocol-specific admission and indexing logic.

## Installation

```bash
go get github.com/b-open-io/overlay
```

## How Overlay Services Work

Overlay services process Bitcoin transactions according to protocol-specific rules. The processing flow has two main stages:

1. **Topic Management** - Determines which outputs should be admitted to your topic
2. **Lookup Services** - Indexes admitted outputs and makes them searchable

### Building Your Topic Manager

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

### Building Your Lookup Service

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

### Event Naming Patterns

The power of the event system comes from using compound event names that enable efficient prefix searching and hierarchical organization. Here are examples from real protocols:

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

The key is to design your event names to support the queries your protocol needs. Events can be hierarchical (using colons as separators) to enable prefix-based searching.

### Querying Your Events

Once your protocol is indexing events, you can query them in several ways:

```go
// Simple value aggregation for a single event
totalSupply, count, err := tokenLookup.ValueSumUint64(ctx, "sym:GOLD", events.SpentStatusUnspent)

// Find all events for a specific output
events, err := tokenLookup.FindEvents(ctx, outpoint)
// Returns: ["token:abc123", "type:transfer", "owner:1ABC...xyz:abc123"]

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

Choose the backend that best fits your deployment needs:

- **Redis** - Best for high-performance production deployments with clustering support
- **MongoDB** - Ideal for complex queries and large datasets with built-in replication
- **SQLite** - Perfect for development, testing, and smaller deployments

All backends provide the same interface, so you can switch between them without changing your protocol logic.

### Advanced Multi-Event Queries

For more complex queries, you can search for multiple events using join operations:

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

## Storage Package

The storage package provides implementations of the `engine.Storage` interface from go-overlay-services for storing and retrieving transaction outputs.

### Redis Storage

```go
storage, err := storage.NewRedisStorage(
    "redis://localhost:6379",
    beefStore,
    publisher
)

// Insert an output
err = storage.InsertOutput(ctx, output)

// Find outputs by topic
outputs, err := storage.FindUTXOsForTopic(ctx, "my-topic", 0, 100)
```

### MongoDB Storage

```go
storage, err := storage.NewMongoStorage(
    "mongodb://localhost:27017",
    "mydb",
    beefStore,
    publisher
)
```

### SQLite Storage

```go
storage, err := storage.NewSQLiteStorage(
    "/path/to/database.db",
    beefStore,
    publisher
)
```

SQLite storage features:
- WAL mode for concurrent reads/writes
- Full ACID compliance
- Embedded database (no server required)
- Optimized for local development and smaller deployments

## BEEF Storage

The beef package provides implementations of the `beef.BeefStorage` interface for storing and retrieving BEEF (Background Evaluation Extended Format) data.

```go
// Redis BEEF storage
beefStore, err := beef.NewRedisBeefStorage("redis://localhost:6379")

// MongoDB BEEF storage
beefStore, err := beef.NewMongoBeefStorage("mongodb://localhost:27017", "mydb")

// Save BEEF data
err = beefStore.SaveBeef(ctx, txid, beefBytes)

// Load BEEF data
beefBytes, err := beefStore.LoadBeef(ctx, txid)
```

## Publishing

The publish package provides implementations of the `publish.Publisher` interface for real-time event notifications:

```go
// Redis publisher
publisher, err := publish.NewRedisPublisher("redis://localhost:6379")

// Publish an event
err = publisher.Publish(ctx, "topic", data)
```