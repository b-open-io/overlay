package beef

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker/headers_client"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteBeefStorage struct {
	BaseBeefStorage
	db *sql.DB
}

func NewSQLiteBeefStorage(dbPath string) (*SQLiteBeefStorage, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Create table if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS beef_storage (
		txid TEXT PRIMARY KEY,
		beef BLOB NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_beef_created_at ON beef_storage(created_at);
	`
	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &SQLiteBeefStorage{
		db: db,
	}, nil
}

func (t *SQLiteBeefStorage) Close() error {
	return t.db.Close()
}

func (t *SQLiteBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	txidStr := txid.String()
	
	// Try to load from SQLite first
	var beefBytes []byte
	err := t.db.QueryRowContext(ctx, "SELECT beef FROM beef_storage WHERE txid = ?", txidStr).Scan(&beefBytes)
	
	if err == nil {
		return beefBytes, nil
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("database error: %w", err)
	}

	// If not found in SQLite, try to fetch from JungleBus
	beefBytes, err = t.BaseBeefStorage.LoadBeef(ctx, txid)
	if err == nil {
		// Save to SQLite for future use
		t.SaveBeef(ctx, txid, beefBytes)
	}
	return beefBytes, err
}

func (t *SQLiteBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	txidStr := txid.String()
	
	// Use INSERT OR REPLACE to handle duplicates
	_, err := t.db.ExecContext(ctx,
		"INSERT OR REPLACE INTO beef_storage (txid, beef) VALUES (?, ?)",
		txidStr, beefBytes)
	
	return err
}

// LoadTx loads a transaction from BEEF storage with optional merkle path validation
func (t *SQLiteBeefStorage) LoadTx(ctx context.Context, txid *chainhash.Hash, chaintracker *headers_client.Client) (*transaction.Transaction, error) {
	// Load BEEF from storage - this will use SQLiteBeefStorage.LoadBeef
	beefBytes, err := t.LoadBeef(ctx, txid)
	if err != nil {
		return nil, err
	}

	return LoadTxFromBeef(ctx, beefBytes, txid, chaintracker)
}