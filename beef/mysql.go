package beef

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLBeefStorage struct {
	db *sql.DB
}

// NewMySQLBeefStorage creates a new MySQL-backed BEEF storage.
// The connString can be either:
//   - URL format: mysql://user:password@host:port/database
//   - DSN format: user:password@tcp(host:port)/database
func NewMySQLBeefStorage(connString string) (*MySQLBeefStorage, error) {
	// Parse connection string and convert to DSN if needed
	dsn, err := parseMySQLConnectionString(connString)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL database: %w", err)
	}

	// Create table if it doesn't exist
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS beef_storage (
		txid VARCHAR(64) PRIMARY KEY,
		beef LONGBLOB NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_beef_created_at (created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Set connection pool limits
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(1 * time.Hour)
	db.SetConnMaxIdleTime(30 * time.Minute)

	return &MySQLBeefStorage{
		db: db,
	}, nil
}

// parseMySQLConnectionString converts a connection string to MySQL DSN format
func parseMySQLConnectionString(connString string) (string, error) {
	// If it doesn't start with mysql://, assume it's already a DSN
	if !strings.HasPrefix(connString, "mysql://") {
		return connString, nil
	}

	// Parse URL format: mysql://user:password@host:port/database?params
	u, err := url.Parse(connString)
	if err != nil {
		return "", fmt.Errorf("invalid MySQL URL format: %w", err)
	}

	// Extract user and password
	user := u.User.Username()
	password, _ := u.User.Password()

	// Extract host and port
	host := u.Host
	if host == "" {
		host = "localhost:3306"
	}

	// Extract database name
	database := strings.TrimPrefix(u.Path, "/")
	if database == "" {
		return "", fmt.Errorf("database name is required in MySQL connection string")
	}

	// Build DSN: user:password@tcp(host:port)/database
	var dsn string
	if password != "" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, host, database)
	} else {
		dsn = fmt.Sprintf("%s@tcp(%s)/%s", user, host, database)
	}

	// Append query parameters if present
	if u.RawQuery != "" {
		dsn += "?" + u.RawQuery
	}

	return dsn, nil
}

func (m *MySQLBeefStorage) Close() error {
	return m.db.Close()
}

func (m *MySQLBeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	txidStr := txid.String()

	var beefBytes []byte
	err := m.db.QueryRowContext(ctx, "SELECT beef FROM beef_storage WHERE txid = ?", txidStr).Scan(&beefBytes)

	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("database error: %w", err)
	}

	return beefBytes, nil
}

func (m *MySQLBeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	txidStr := txid.String()

	// Use REPLACE to handle duplicates (MySQL equivalent of INSERT OR REPLACE)
	_, err := m.db.ExecContext(ctx,
		"REPLACE INTO beef_storage (txid, beef) VALUES (?, ?)",
		txidStr, beefBytes)

	return err
}

// UpdateMerklePath is not supported by MySQL storage
func (m *MySQLBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return nil, nil
}