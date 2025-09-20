package headers

import (
	"context"
	"fmt"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker/headers_client"
)

type ClientParams struct {
	Url    string
	ApiKey string
}

type Client struct {
	// Embed the go-sdk headers client to inherit all its methods
	*headers_client.Client

	// Cache of valid merkle roots by height
	// This acts as our source of truth for the main chain
	merkleCache sync.Map // map[uint32]chainhash.Hash

	// Track last evaluated key for merkle root pagination
	lastEvaluatedKey *chainhash.Hash
}

// NewClient creates a new headers client with ClientParams
func NewClient(params ClientParams) *Client {
	// Create the underlying go-sdk client
	sdkClient := &headers_client.Client{
		Url:    params.Url,
		ApiKey: params.ApiKey,
	}

	return &Client{
		Client: sdkClient,
	}
}

func (c *Client) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	// Get the merkle root for this height (from cache or remote)
	validRoot, err := c.GetMerkleRootForHeight(ctx, height)
	if err != nil {
		return false, err
	}

	return validRoot.IsEqual(root), nil
}

// GetMerkleRootForHeight returns the merkle root for a specific height
// It checks the cache first, and if not present, fetches from remote and caches it
func (c *Client) GetMerkleRootForHeight(ctx context.Context, height uint32) (*chainhash.Hash, error) {
	// Check cache
	if cachedRoot, ok := c.merkleCache.Load(height); ok {
		root := cachedRoot.(chainhash.Hash)
		return &root, nil
	}

	// Cache miss - fetch from remote
	header, err := c.BlockByHeight(ctx, height)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch header for height %d: %w", height, err)
	}

	// Cache the result (MerkleRoot is already a chainhash.Hash)
	c.merkleCache.Store(height, header.MerkleRoot)

	return &header.MerkleRoot, nil
}

// These methods are now inherited from the embedded headers_client.Client
// Only override them if we need custom behavior

// MerkleRootChange represents a detected change in merkle root at a specific height
type MerkleRootChange struct {
	Height      uint32
	OldRoot     *chainhash.Hash // nil if this height wasn't previously cached
	NewRoot     *chainhash.Hash
	IsReorg     bool // true if we had a different root cached
}

// SyncMerkleRoots fetches and caches merkle roots for recent blocks
// Returns a slice of heights where merkle roots have changed (new or different)
// Uses the bulk /chain/merkleroot API to efficiently sync merkle roots
func (c *Client) SyncMerkleRoots(ctx context.Context) ([]MerkleRootChange, error) {
	var changes []MerkleRootChange
	// If we don't have a starting point, calculate one
	if c.lastEvaluatedKey == nil {
		// Get the current chain tip
		chaintip, err := c.GetChaintip(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get chaintip: %w", err)
		}

		// Calculate starting height (100 blocks back or genesis)
		startHeight := uint32(1)
		if chaintip.Height > 100 {
			startHeight = chaintip.Height - 100
		}

		// Fetch the starting block to get its merkle root for pagination
		startHeader, err := c.BlockByHeight(ctx, startHeight)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch starting block at height %d: %w", startHeight, err)
		}

		// Use the merkle root as the starting key
		c.lastEvaluatedKey = &startHeader.MerkleRoot
	}

	batchSize := 2000 // Default batch size from block-headers-service

	// Fetch merkle roots in batches
	for {
		// Call the merkle roots API
		merkleRoots, err := c.Client.GetMerkleRoots(ctx, batchSize, c.lastEvaluatedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch merkle roots: %w", err)
		}

		// No more data
		if len(merkleRoots) == 0 {
			break
		}

		// Cache all the merkle roots and detect changes
		for _, mr := range merkleRoots {
			height := uint32(mr.BlockHeight)
			newRoot := mr.MerkleRoot

			// Use Swap to atomically get the old value and set the new one
			oldValue, loaded := c.merkleCache.Swap(height, newRoot)

			if loaded {
				// We had a value before
				oldRoot := oldValue.(chainhash.Hash)
				if !oldRoot.IsEqual(&newRoot) {
					// This is a reorg - the merkle root changed
					changes = append(changes, MerkleRootChange{
						Height:  height,
						OldRoot: &oldRoot,
						NewRoot: &newRoot,
						IsReorg: true,
					})
				}
			} else {
				// This is a new height we haven't seen before
				changes = append(changes, MerkleRootChange{
					Height:  height,
					OldRoot: nil,
					NewRoot: &newRoot,
					IsReorg: false,
				})
			}

		}

		// Update lastEvaluatedKey to the last merkle root we received
		if len(merkleRoots) > 0 {
			lastRoot := merkleRoots[len(merkleRoots)-1].MerkleRoot
			c.lastEvaluatedKey = &lastRoot
		}

		// If we got less than batchSize, we've reached the end
		if len(merkleRoots) < batchSize {
			break
		}
	}

	return changes, nil
}