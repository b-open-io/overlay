package storage

import (
	"context"
	"fmt"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// IMMUTABILITY_DEPTH defines how many blocks deep an output must be to be considered immutable
const IMMUTABILITY_DEPTH = 100

// BaseEventDataStorage provides common fields and methods for all EventDataStorage implementations
// This uses Go's struct embedding to achieve code reuse across different storage backends
type BaseEventDataStorage struct {
	beefStore    *beef.Storage
	pubsub       pubsub.PubSub                // Generic PubSub interface for event publishing and buffering
	queueStorage queue.QueueStorage           // QueueStorage interface for Redis-like operations
	chainTracker chaintracker.ChainTracker    // ChainTracker for merkle validation and chain state
}

// NewBaseEventDataStorage creates a new BaseEventDataStorage with the given dependencies
func NewBaseEventDataStorage(beefStore *beef.Storage, queueStorage queue.QueueStorage, pubsub pubsub.PubSub, chainTracker chaintracker.ChainTracker) BaseEventDataStorage {
	return BaseEventDataStorage{
		beefStore:    beefStore,
		queueStorage: queueStorage,
		pubsub:       pubsub,
		chainTracker: chainTracker,
	}
}

// GetBeefStorage returns the underlying BEEF storage implementation
func (b *BaseEventDataStorage) GetBeefStorage() *beef.Storage {
	return b.beefStore
}

// GetPubSub returns the PubSub interface for event publishing and buffering
// Returns nil if no pubsub is configured
func (b *BaseEventDataStorage) GetPubSub() pubsub.PubSub {
	return b.pubsub
}

// GetQueueStorage returns the QueueStorage interface for Redis-like operations
func (b *BaseEventDataStorage) GetQueueStorage() queue.QueueStorage {
	return b.queueStorage
}

// GetChainTracker returns the ChainTracker for merkle validation and chain state
func (b *BaseEventDataStorage) GetChainTracker() chaintracker.ChainTracker {
	return b.chainTracker
}

// ExtractMerkleInfoFromBEEF parses BEEF and extracts merkle path information
// Returns blockHeight, merkleRoot, and validationState. merkleRoot will be nil if no valid merkle path exists
func (b *BaseEventDataStorage) ExtractMerkleInfoFromBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) (blockHeight uint32, blockIndex uint64, merkleRoot *chainhash.Hash, validationState engine.MerkleState, err error) {
	// Default to unmined state
	validationState = engine.MerkleStateUnmined

	// Parse BEEF to extract merkle path
	_, tx, _, err := transaction.ParseBeef(beef)
	if err != nil {
		// Return error from parsing
		return 0, 0, nil, validationState, fmt.Errorf("failed to parse BEEF: %w", err)
	}

	// If there's no merkle path, return defaults (not an error)
	if tx.MerklePath == nil {
		return 0, 0, nil, validationState, nil
	}

	blockHeight = tx.MerklePath.BlockHeight

	// Extract block index from the merkle path
	for _, leaf := range tx.MerklePath.Path[0] {
		if leaf.Hash != nil && leaf.Hash.Equal(*txid) {
			blockIndex = leaf.Offset
			break
		}
	}

	// Calculate merkle root from the path
	root, err := tx.MerklePath.ComputeRoot(txid)
	if err != nil {
		// Return error from computing root
		return blockHeight, blockIndex, nil, validationState, fmt.Errorf("failed to compute merkle root: %w", err)
	}
	merkleRoot = root

	// Determine validation state
	validationState = engine.MerkleStateValidated

	// Check if it should be immutable
	if b.chainTracker != nil {
		currentHeight, err := b.chainTracker.CurrentHeight(ctx)
		if err == nil {
			depth := currentHeight - blockHeight
			if depth >= IMMUTABILITY_DEPTH {
				validationState = engine.MerkleStateImmutable
			}
		}
	}

	return blockHeight, blockIndex, merkleRoot, validationState, nil
}
