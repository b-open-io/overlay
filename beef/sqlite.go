package beef

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteBeefStorage struct {
	db *sql.DB
}

func NewSQLiteBeefStorage(dbPath string) (*SQLiteBeefStorage, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Single table with rawtx and optional proof columns
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS beef_storage (
		txid TEXT PRIMARY KEY,
		rawtx BLOB NOT NULL,
		proof BLOB,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_beef_created_at ON beef_storage(created_at);
	`
	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	db.SetMaxOpenConns(15)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0)

	return &SQLiteBeefStorage{
		db: db,
	}, nil
}

func (t *SQLiteBeefStorage) Close() error {
	return t.db.Close()
}

func (t *SQLiteBeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	var rawTx []byte
	var proof []byte
	err := t.db.QueryRowContext(ctx,
		"SELECT rawtx, proof FROM beef_storage WHERE txid = ?",
		txid.String()).Scan(&rawTx, &proof)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("database error: %w", err)
	}
	return assembleBEEF(txid, rawTx, proof)
}

func (t *SQLiteBeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	rawTx, proof, err := splitBEEF(beefBytes)
	if err != nil {
		return err
	}

	_, err = t.db.ExecContext(ctx,
		"INSERT OR REPLACE INTO beef_storage (txid, rawtx, proof) VALUES (?, ?, ?)",
		txid.String(), rawTx, proof)
	return err
}

// GetRawTx loads just the raw transaction bytes
func (t *SQLiteBeefStorage) GetRawTx(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	var rawTx []byte
	err := t.db.QueryRowContext(ctx,
		"SELECT rawtx FROM beef_storage WHERE txid = ?",
		txid.String()).Scan(&rawTx)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("database error: %w", err)
	}
	return rawTx, nil
}

// GetProof loads just the merkle proof bytes
func (t *SQLiteBeefStorage) GetProof(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	var proof []byte
	err := t.db.QueryRowContext(ctx,
		"SELECT proof FROM beef_storage WHERE txid = ? AND proof IS NOT NULL",
		txid.String()).Scan(&proof)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("database error: %w", err)
	}
	return proof, nil
}

// UpdateMerklePath is not supported by SQLite storage
func (t *SQLiteBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return nil, nil
}
