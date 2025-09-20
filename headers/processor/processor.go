package processor

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/b-open-io/overlay/headers"
	"github.com/b-open-io/overlay/storage"
)

var (
	// Global state for deduplication
	processInFlight atomic.Bool
	lastProcessedHeight atomic.Uint32
)

// Start begins monitoring for new blocks with periodic polling
func Start(ctx context.Context, headersClient *headers.Client, eventStorage *storage.EventDataStorage, pollInterval time.Duration) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// Process immediately on start
	if err := ProcessChaintip(ctx, headersClient, eventStorage); err != nil {
		fmt.Printf("Error processing chaintip: %v\n", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ProcessChaintip(ctx, headersClient, eventStorage); err != nil {
				fmt.Printf("Error processing chaintip: %v\n", err)
			}
		}
	}
}

// ProcessChaintip handles blockchain updates by syncing merkle roots and reconciling outputs
func ProcessChaintip(ctx context.Context, headersClient *headers.Client, eventStorage *storage.EventDataStorage) error {
	// Try to claim the processing slot
	if !processInFlight.CompareAndSwap(false, true) {
		// Already processing, skip this call
		return nil
	}

	// We got the lock, ensure we clear it when done
	defer processInFlight.Store(false)

	// First, sync merkle roots and detect any changes
	changes, err := headersClient.SyncMerkleRoots(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync merkle roots: %w", err)
	}

	// Get current chaintip for immutability calculations
	chaintip, err := headersClient.GetChaintip(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chaintip: %w", err)
	}

	// If there are no changes and we've already processed this height, nothing to do
	lastHeight := lastProcessedHeight.Load()
	if len(changes) == 0 && chaintip.Height <= lastHeight {
		return nil // No new blocks or changes
	}

	// Log any detected changes
	for _, change := range changes {
		if change.IsReorg {
			fmt.Printf("Reorg detected at height %d (old: %s, new: %s)\n",
				change.Height, change.OldRoot, change.NewRoot)
		} else {
			fmt.Printf("New block at height %d (merkle: %s)\n",
				change.Height, change.NewRoot)
		}
	}

	// Reconcile validated outputs whenever there are changes or a new chaintip
	// This will handle outputs at changed heights AND any outputs validated before we had merkle info
	if len(changes) > 0 || chaintip.Height > lastHeight {
		if err := eventStorage.ReconcileValidatedMerkleRoots(ctx); err != nil {
			return fmt.Errorf("failed to reconcile validated outputs: %w", err)
		}
	}

	// Update last processed height
	lastProcessedHeight.Store(chaintip.Height)

	return nil
}

