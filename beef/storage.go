package beef

import (
	"context"
	"errors"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker/headers_client"
)

var BeefKey = "beef"
var ErrNotFound = errors.New("not-found")

type BeefStorage interface {
	LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error)
	SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error
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

// LoadTx loads a transaction from any BeefStorage implementation with optional merkle path validation
func LoadTx(ctx context.Context, storage BeefStorage, txid *chainhash.Hash, chaintracker *headers_client.Client) (*transaction.Transaction, error) {
	// Load BEEF from storage
	beefBytes, err := storage.LoadBeef(ctx, txid)
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
