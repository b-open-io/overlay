package beef

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteBeefStorage struct {
	db       *sql.DB
	fallback BeefStorage
}

func NewSQLiteBeefStorage(dbPath string, fallback BeefStorage) (*SQLiteBeefStorage, error) {
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

	// Set connection pool limits to prevent goroutine explosion
	db.SetMaxOpenConns(15)   // BEEF storage needs more connections for concurrent access
	db.SetMaxIdleConns(5)    // Keep several idle connections for frequent access
	db.SetConnMaxLifetime(0) // No connection lifetime limit

	return &SQLiteBeefStorage{
		db:       db,
		fallback: fallback,
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

	// Not found in SQLite, try fallback
	if t.fallback != nil {
		beefBytes, err = t.fallback.LoadBeef(ctx, txid)
		if err == nil {
			// Save to SQLite for future use
			t.SaveBeef(ctx, txid, beefBytes)
		}
		return beefBytes, err
	}

	return nil, ErrNotFound
}

func (t *SQLiteBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	txidStr := txid.String()

	// Use INSERT OR REPLACE to handle duplicates
	_, err := t.db.ExecContext(ctx,
		"INSERT OR REPLACE INTO beef_storage (txid, beef) VALUES (?, ?)",
		txidStr, beefBytes)

	return err
}

// UpdateMerklePath updates the merkle path for a transaction by delegating to the fallback
func (t *SQLiteBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	if t.fallback != nil {
		beefBytes, err := t.fallback.UpdateMerklePath(ctx, txid)
		if err == nil && len(beefBytes) > 0 {
			// Update our own storage with the new beef
			t.SaveBeef(ctx, txid, beefBytes)
		}
		return beefBytes, err
	}
	return nil, ErrNotFound
}
