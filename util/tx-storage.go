package util

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
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

var JUNGLEBUS string

var BeefKey = "beef"
var ErrNotFound = errors.New("not-found")

func init() {
	godotenv.Load("../../.env")
	JUNGLEBUS = os.Getenv("JUNGLEBUS")
}

type TxStorage interface {
	LoadTx(ctx context.Context, txid *chainhash.Hash) (*transaction.Transaction, error)
	LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error)
	SaveBeef(ctx context.Context, beefBytes []byte) error
}

type RedisTxStorage struct {
	db          *redis.Client
	inflightMap sync.Map
	// chaintracker chaintracker.ChainTracker
}

func NewRedisTxStorage(connString string) (*RedisTxStorage, error) {
	r := &RedisTxStorage{
		// chaintracker: chaintracker,
	}
	if opts, err := redis.ParseURL(connString); err != nil {
		return nil, err
	} else {
		r.db = redis.NewClient(opts)
		return r, nil
	}
}

type inflightRequest struct {
	wg     *sync.WaitGroup
	result []byte
	err    error
}

func (t *RedisTxStorage) LoadTx(ctx context.Context, txid *chainhash.Hash) (*transaction.Transaction, error) {
	if beefBytes, err := t.LoadBeef(ctx, txid); err != nil {
		return nil, err
	} else {
		return transaction.NewTransactionFromBEEF(beefBytes)
	}

}

func (t *RedisTxStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	txidStr := txid.String()
	if beefBytes, err := t.db.HGet(ctx, BeefKey, txidStr).Bytes(); err != nil && err != redis.Nil {
		return nil, err
	} else if err == nil && beefBytes != nil {
		return beefBytes, nil
	}
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
		req.result, req.err = t.fetchTransaction(ctx, txid)
		return req.result, req.err
	}
}

func (t *RedisTxStorage) SaveTx(ctx context.Context, tx *transaction.Transaction) error {
	txid := tx.TxID()
	if beefBytes, err := tx.BEEF(); err != nil {
		return err
	} else {
		return t.db.HSet(ctx, BeefKey, txid.String(), beefBytes).Err()
	}
}

func (t *RedisTxStorage) SaveBeef(ctx context.Context, beefBytes []byte) error {
	if tx, err := transaction.NewTransactionFromBEEF(beefBytes); err != nil {
		return err
	} else {
		return t.db.HSet(ctx, BeefKey, tx.TxID().String(), beefBytes).Err()
	}
}

func (t *RedisTxStorage) fetchTransaction(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	if JUNGLEBUS == "" {
		return nil, ErrNotFound
	}
	txidStr := txid.String()
	resp, err := http.Get(fmt.Sprintf("%s/v1/transaction/beef/%s", JUNGLEBUS, txidStr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 404 {
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

	// if tx.MerklePath != nil {
	// 	root, err := tx.MerklePath.ComputeRoot(txid)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	valid, err := t.chaintracker.IsValidRootForHeight(root, tx.MerklePath.BlockHeight)
	// 	if err != nil || !valid {
	// 		return nil, errors.New("invalid-merkle-path")
	// 	}
	// }

	if err := t.db.HSet(ctx, BeefKey, txidStr, beefBytes).Err(); err != nil {
		return nil, err
	}
	return beefBytes, nil
}
