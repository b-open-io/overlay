package beef

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/joho/godotenv"
)

// MaxConcurrentRequests limits the number of concurrent BEEF fetch operations
const MaxConcurrentRequests = 16

type JunglebusBeefStorage struct {
	junglebusURL string
	limiter      chan struct{}
}

func NewJunglebusBeefStorage(junglebusURL string) *JunglebusBeefStorage {
	// If no URL provided, try to get from environment
	if junglebusURL == "" {
		godotenv.Load(".env")
		junglebusURL = os.Getenv("JUNGLEBUS")
	}

	// If still empty, use default
	if junglebusURL == "" {
		junglebusURL = "https://junglebus.gorillapool.io"
	}

	return &JunglebusBeefStorage{
		junglebusURL: junglebusURL,
		limiter:      make(chan struct{}, MaxConcurrentRequests),
	}
}

func (t *JunglebusBeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// Acquire limiter before making HTTP request
	select {
	case t.limiter <- struct{}{}:
		defer func() { <-t.limiter }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return t.fetchBeef(txid)
}

func (t *JunglebusBeefStorage) fetchBeef(txid *chainhash.Hash) ([]byte, error) {
	if t.junglebusURL == "" {
		return nil, ErrNotFound
	}

	txidStr := txid.String()
	url := fmt.Sprintf("%s/v1/transaction/beef/%s", t.junglebusURL, txidStr)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	} else if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http-err-%d-%s", resp.StatusCode, txidStr)
	}

	beefBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	_, _, _, err = transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, err
	}

	return beefBytes, nil
}

// Put is a no-op for JungleBus (read-only)
func (t *JunglebusBeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	// JungleBus is read-only, cannot write
	return nil
}

// UpdateMerklePath fetches a fresh BEEF with merkle proof from JungleBus
func (t *JunglebusBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// JungleBus always returns fresh BEEFs with merkle proofs, so just fetch again
	return t.Get(ctx, txid)
}

// GetRawTx fetches just the raw transaction from JungleBus
func (t *JunglebusBeefStorage) GetRawTx(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	select {
	case t.limiter <- struct{}{}:
		defer func() { <-t.limiter }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if t.junglebusURL == "" {
		return nil, ErrNotFound
	}

	url := fmt.Sprintf("%s/v1/transaction/get/%s/bin", t.junglebusURL, txid.String())
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	} else if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http-err-%d-%s", resp.StatusCode, txid.String())
	}

	return io.ReadAll(resp.Body)
}

// GetProof fetches just the merkle proof from JungleBus
func (t *JunglebusBeefStorage) GetProof(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	select {
	case t.limiter <- struct{}{}:
		defer func() { <-t.limiter }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if t.junglebusURL == "" {
		return nil, ErrNotFound
	}

	url := fmt.Sprintf("%s/v1/transaction/proof/%s/bin", t.junglebusURL, txid.String())
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	} else if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http-err-%d-%s", resp.StatusCode, txid.String())
	}

	return io.ReadAll(resp.Body)
}

// Close is a no-op for JungleBus (no persistent connections)
func (j *JunglebusBeefStorage) Close() error {
	return nil
}
