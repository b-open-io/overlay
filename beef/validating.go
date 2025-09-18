package beef

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

// ValidatingBeefStorage wraps a BeefStorage chain to validate merkle proofs on load
// and automatically update invalid proofs when possible
type ValidatingBeefStorage struct {
	storage      BeefStorage
	chainTracker chaintracker.ChainTracker
}

// NewValidatingBeefStorage creates a new validating wrapper around a BeefStorage chain
func NewValidatingBeefStorage(storage BeefStorage, chainTracker chaintracker.ChainTracker) *ValidatingBeefStorage {
	return &ValidatingBeefStorage{
		storage:      storage,
		chainTracker: chainTracker,
	}
}

// LoadBeef loads BEEF data and validates its merkle proof, updating if invalid
func (v *ValidatingBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// Load the BEEF from underlying storage
	beefBytes, err := v.storage.LoadBeef(ctx, txid)
	if err != nil {
		return nil, err
	}

	// Parse the BEEF to get the transaction with merkle path
	_, tx, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		// If we can't parse it, return it as-is (let caller deal with parse errors)
		return beefBytes, nil
	}

	// Check if transaction has a merkle path
	if tx.MerklePath == nil {
		// No merkle path, attempt to update
		updatedBeef, err := v.storage.UpdateMerklePath(ctx, txid, v.chainTracker)
		if err == nil && len(updatedBeef) > 0 {
			return updatedBeef, nil
		}
		// If update failed, return original BEEF
		return beefBytes, nil
	}

	// Validate the merkle proof
	valid, err := tx.MerklePath.Verify(ctx, txid, v.chainTracker)
	if err != nil {
		// If we can't verify (e.g., chainTracker issues), return original BEEF
		return beefBytes, nil
	}

	if !valid {
		// Merkle proof is invalid, attempt to update
		// UpdateMerklePath will validate the new proof internally
		updatedBeef, err := v.storage.UpdateMerklePath(ctx, txid, v.chainTracker)
		if err != nil {
			// Update failed or new proof is invalid
			return nil, err
		}
		return updatedBeef, nil
	}

	// Merkle proof is valid, return the BEEF as-is
	return beefBytes, nil
}

// SaveBeef saves BEEF data by delegating to the underlying storage
func (v *ValidatingBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	return v.storage.SaveBeef(ctx, txid, beefBytes)
}

// UpdateMerklePath updates the merkle path by delegating to the underlying storage
func (v *ValidatingBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash, ct chaintracker.ChainTracker) ([]byte, error) {
	return v.storage.UpdateMerklePath(ctx, txid, ct)
}

// Close closes the underlying storage
func (v *ValidatingBeefStorage) Close() error {
	return v.storage.Close()
}