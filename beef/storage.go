package beef

import (
	"context"
	"errors"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

var BeefKey = "beef"
var ErrNotFound = errors.New("not-found")
var ErrInvalidMerkleProof = errors.New("invalid merkle proof")

type BeefStorage interface {
	LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error)
	SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error
	UpdateMerklePath(ctx context.Context, txid *chainhash.Hash, ct chaintracker.ChainTracker) ([]byte, error)
	Close() error
}

// LoadTxFromBeef is a helper function that loads a transaction from BEEF bytes
func LoadTxFromBeef(ctx context.Context, beefBytes []byte, txid *chainhash.Hash) (*transaction.Transaction, error) {
	// Parse BEEF to get the transaction
	beef, tx, parsedTxid, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, err
	}

	// If ParseBeef returned a txid, verify it matches what we requested
	if parsedTxid != nil && !parsedTxid.IsEqual(txid) {
		return nil, errors.New("txid mismatch: requested " + txid.String() + ", got " + parsedTxid.String())
	}

	// If no transaction was returned (e.g., BEEF_V2), find it in the BEEF document
	if tx == nil && beef != nil {
		tx = beef.FindTransaction(txid.String())
	}

	// Verify we got a transaction
	if tx == nil {
		return nil, errors.New("transaction " + txid.String() + " not found in BEEF")
	}

	// Verify the loaded transaction's txid matches what we requested
	loadedTxid := tx.TxID()
	if !loadedTxid.IsEqual(txid) {
		return nil, errors.New("loaded transaction txid mismatch: requested " + txid.String() + ", got " + loadedTxid.String())
	}

	return tx, nil
}

// LoadTx loads a transaction from any BeefStorage implementation
func LoadTx(ctx context.Context, storage BeefStorage, txid *chainhash.Hash) (*transaction.Transaction, error) {
	// Load BEEF from storage
	beefBytes, err := storage.LoadBeef(ctx, txid)
	if err != nil {
		return nil, err
	}

	return LoadTxFromBeef(ctx, beefBytes, txid)
}
