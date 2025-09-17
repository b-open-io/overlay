package beef

import (
	"context"

	"github.com/b-open-io/overlay/dedup"
	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// DedupBeefStorage wraps a BeefStorage chain with deduplication
type DedupBeefStorage struct {
	chain  BeefStorage
	loader *dedup.Loader[chainhash.Hash, []byte]
	saver  *dedup.Saver[chainhash.Hash, []byte]
}

// NewDedupBeefStorage creates a deduplicated wrapper around a BeefStorage chain
func NewDedupBeefStorage(chain BeefStorage) *DedupBeefStorage {
	dedupStorage := &DedupBeefStorage{
		chain: chain,
	}
	
	// Create loader that executes the entire chain
	dedupStorage.loader = dedup.NewLoader(func(txid chainhash.Hash) ([]byte, error) {
		return chain.LoadBeef(context.Background(), &txid)
	})
	
	// Create saver that executes the entire chain
	dedupStorage.saver = dedup.NewSaver(func(txid chainhash.Hash, beefBytes []byte) error {
		return chain.SaveBeef(context.Background(), &txid, beefBytes)
	})
	
	return dedupStorage
}

// LoadBeef loads BEEF data with deduplication across the entire fallback chain
func (d *DedupBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return d.loader.Load(*txid)
}

// SaveBeef saves BEEF data with deduplication to prevent concurrent saves of the same txid
func (d *DedupBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	return d.saver.Save(*txid, beefBytes)
}

// UpdateMerklePath updates the merkle path for a transaction by delegating to the underlying chain
func (d *DedupBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return d.chain.UpdateMerklePath(ctx, txid)
}

// Close closes the underlying chain and cleans up deduplication
func (d *DedupBeefStorage) Close() error {
	d.loader.Clear()
	d.saver.Clear()
	return d.chain.Close()
}