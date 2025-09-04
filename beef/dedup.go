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
	
	return dedupStorage
}

// LoadBeef loads BEEF data with deduplication across the entire fallback chain
func (d *DedupBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return d.loader.Load(*txid)
}

// SaveBeef delegates to the underlying chain
func (d *DedupBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	return d.chain.SaveBeef(ctx, txid, beefBytes)
}

// Close closes the underlying chain and cleans up deduplication
func (d *DedupBeefStorage) Close() error {
	d.loader.Clear()
	return d.chain.Close()
}