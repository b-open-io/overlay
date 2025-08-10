package beef

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker/headers_client"
	"github.com/joho/godotenv"
)

var JUNGLEBUS string

var BeefKey = "beef"
var ErrNotFound = errors.New("not-found")

func init() {
	godotenv.Load(".env")
	JUNGLEBUS = os.Getenv("JUNGLEBUS")
}

type BeefStorage interface {
	LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error)
	SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error
	LoadTx(ctx context.Context, txid *chainhash.Hash, chaintracker *headers_client.Client) (*transaction.Transaction, error)
}

type inflightRequest struct {
	wg     *sync.WaitGroup
	result []byte
	err    error
}

type BaseBeefStorage struct {
	inflightMap sync.Map
}

func (t *BaseBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
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
		req.result, req.err = t.fetchBeef(txid)
		return req.result, req.err
	}
}

func (t *BaseBeefStorage) SaveBeef(ctx context.Context, beefBytes []byte) error {
	return nil
}

// LoadTxFromBeef is a helper function that loads a transaction from BEEF bytes
// and optionally validates its merkle path
func LoadTxFromBeef(ctx context.Context, beefBytes []byte, txid *chainhash.Hash, chaintracker *headers_client.Client) (*transaction.Transaction, error) {
	// Parse BEEF to get the transaction
	_, tx, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, err
	}

	// Validate merkle path if present and chaintracker is provided
	if tx.MerklePath != nil && chaintracker != nil {
		if err := validateMerklePath(ctx, tx, txid, chaintracker); err != nil {
			return nil, err
		}
	}

	return tx, nil
}

// LoadTx loads a transaction from BEEF storage with optional merkle path validation
// This method should not be overridden by implementations
func (t *BaseBeefStorage) LoadTx(ctx context.Context, txid *chainhash.Hash, chaintracker *headers_client.Client) (*transaction.Transaction, error) {
	// Load BEEF from storage
	beefBytes, err := t.LoadBeef(ctx, txid)
	if err != nil {
		return nil, err
	}

	return LoadTxFromBeef(ctx, beefBytes, txid, chaintracker)
}

// validateMerklePath validates the transaction's merkle path against the chain tracker
func validateMerklePath(ctx context.Context, tx *transaction.Transaction, txid *chainhash.Hash, chaintracker *headers_client.Client) error {
	root, err := tx.MerklePath.ComputeRoot(txid)
	if err != nil {
		return err
	}

	valid, err := chaintracker.IsValidRootForHeight(ctx, root, tx.MerklePath.BlockHeight)
	if err != nil {
		return err
	}

	if !valid {
		return errors.New("invalid merkle path")
	}

	return nil
}

func (t *BaseBeefStorage) fetchBeef(txid *chainhash.Hash) ([]byte, error) {
	if JUNGLEBUS == "" {
		return nil, ErrNotFound
	}
	txidStr := txid.String()
	url := fmt.Sprintf("%s/v1/transaction/beef/%s", JUNGLEBUS, txidStr)
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
