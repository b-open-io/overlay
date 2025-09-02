package beef

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/joho/godotenv"
)

// MaxConcurrentRequests limits the number of concurrent BEEF fetch operations
const MaxConcurrentRequests = 16

type inflightRequest struct {
	wg     *sync.WaitGroup
	result []byte
	err    error
}

type JunglebusBeefStorage struct {
	junglebusURL string
	inflightMap  sync.Map
	fallback     BeefStorage
	limiter      chan struct{}
}

func NewJunglebusBeefStorage(junglebusURL string, fallback BeefStorage) *JunglebusBeefStorage {
	// If no URL provided, try to get from environment
	if junglebusURL == "" {
		godotenv.Load(".env")
		junglebusURL = os.Getenv("JUNGLEBUS")
	}

	return &JunglebusBeefStorage{
		junglebusURL: junglebusURL,
		fallback:     fallback,
		limiter:      make(chan struct{}, MaxConcurrentRequests),
	}
}

func (t *JunglebusBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	txidStr := txid.String()
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	// Check if there's already an in-flight request for this txid
	if inflight, loaded := t.inflightMap.LoadOrStore(txidStr, &inflightRequest{wg: &wg}); loaded {
		req := inflight.(*inflightRequest)
		req.wg.Wait()
		return req.result, req.err
	} else {
		req := inflight.(*inflightRequest)
		
		// Acquire limiter before making HTTP request
		select {
		case t.limiter <- struct{}{}:
			defer func() { <-t.limiter }()
		case <-ctx.Done():
			t.inflightMap.Delete(txidStr)
			return nil, ctx.Err()
		}

		req.result, req.err = t.fetchBeef(txid)

		// If not found, try fallback
		if req.err == ErrNotFound && t.fallback != nil {
			req.result, req.err = t.fallback.LoadBeef(ctx, txid)
		}

		// Clean up inflight map
		t.inflightMap.Delete(txidStr)
		return req.result, req.err
	}
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

func (t *JunglebusBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	// Junglebus is read-only, delegate to fallback if available
	if t.fallback != nil {
		return t.fallback.SaveBeef(ctx, txid, beefBytes)
	}
	return nil
}

// Close cleans up inflight requests (no persistent connections to close)
func (j *JunglebusBeefStorage) Close() error {
	// Clear any inflight requests
	j.inflightMap.Range(func(key, value interface{}) bool {
		j.inflightMap.Delete(key)
		return true
	})
	return nil
}
