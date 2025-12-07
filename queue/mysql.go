package queue

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLQueueStorage struct {
	db *sql.DB
}

// Close closes the database connection
func (m *MySQLQueueStorage) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// NewMySQLQueueStorage creates a new MySQL-backed queue storage.
// The connString can be either:
//   - URL format: mysql://user:password@host:port/database
//   - DSN format: user:password@tcp(host:port)/database
func NewMySQLQueueStorage(connString string) (*MySQLQueueStorage, error) {
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

	// Set connection pool limits
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(1 * time.Hour)
	db.SetConnMaxIdleTime(30 * time.Minute)

	m := &MySQLQueueStorage{db: db}
	if err := m.createTables(); err != nil {
		db.Close()
		return nil, err
	}
	return m, nil
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

func (m *MySQLQueueStorage) createTables() error {
	queries := []string{
		// Sets table
		`CREATE TABLE IF NOT EXISTS queue_sets (
			key_name VARCHAR(255) NOT NULL,
			member TEXT NOT NULL,
			PRIMARY KEY (key_name, member(255))
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		// Hashes table
		`CREATE TABLE IF NOT EXISTS queue_hashes (
			key_name VARCHAR(255) NOT NULL,
			field VARCHAR(255) NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (key_name, field)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		// Sorted sets table
		`CREATE TABLE IF NOT EXISTS queue_sorted_sets (
			key_name VARCHAR(255) NOT NULL,
			member VARCHAR(255) NOT NULL,
			score DOUBLE NOT NULL,
			created_at BIGINT DEFAULT (UNIX_TIMESTAMP()),
			PRIMARY KEY (key_name, member)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

		// Indexes
		`CREATE INDEX IF NOT EXISTS idx_queue_sets_key ON queue_sets(key_name)`,
		`CREATE INDEX IF NOT EXISTS idx_queue_sorted_sets_key_score ON queue_sorted_sets(key_name, score)`,
	}

	for _, query := range queries {
		if _, err := m.db.Exec(query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	return nil
}

// Set Operations
func (m *MySQLQueueStorage) SAdd(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	query := "INSERT IGNORE INTO queue_sets (key_name, member) VALUES "
	args := make([]interface{}, 0, len(members)*2)
	placeholders := make([]string, len(members))

	for i, member := range members {
		placeholders[i] = "(?, ?)"
		args = append(args, key, member)
	}

	query += strings.Join(placeholders, ", ")
	_, err := m.db.ExecContext(ctx, query, args...)
	return err
}

func (m *MySQLQueueStorage) SMembers(ctx context.Context, key string) ([]string, error) {
	rows, err := m.db.QueryContext(ctx, "SELECT member FROM queue_sets WHERE key_name = ?", key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var member string
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	return members, rows.Err()
}

func (m *MySQLQueueStorage) SRem(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	placeholders := strings.Repeat("?,", len(members))
	placeholders = placeholders[:len(placeholders)-1] // Remove trailing comma

	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	for _, member := range members {
		args = append(args, member)
	}

	query := fmt.Sprintf("DELETE FROM queue_sets WHERE key_name = ? AND member IN (%s)", placeholders)
	_, err := m.db.ExecContext(ctx, query, args...)
	return err
}

func (m *MySQLQueueStorage) SIsMember(ctx context.Context, key, member string) (bool, error) {
	var exists int
	err := m.db.QueryRowContext(ctx, "SELECT 1 FROM queue_sets WHERE key_name = ? AND member = ?", key, member).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return exists == 1, err
}

// Hash Operations
func (m *MySQLQueueStorage) HSet(ctx context.Context, key, field, value string) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO queue_hashes (key_name, field, value)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE value = VALUES(value)`,
		key, field, value)
	return err
}

func (m *MySQLQueueStorage) HGet(ctx context.Context, key, field string) (string, error) {
	var value string
	err := m.db.QueryRowContext(ctx, "SELECT value FROM queue_hashes WHERE key_name = ? AND field = ?", key, field).Scan(&value)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("redis: nil") // Mimic Redis nil error
	}
	return value, err
}

func (m *MySQLQueueStorage) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	rows, err := m.db.QueryContext(ctx, "SELECT field, value FROM queue_hashes WHERE key_name = ?", key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var field, value string
		if err := rows.Scan(&field, &value); err != nil {
			return nil, err
		}
		result[field] = value
	}
	return result, rows.Err()
}

func (m *MySQLQueueStorage) HDel(ctx context.Context, key string, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}

	placeholders := strings.Repeat("?,", len(fields))
	placeholders = placeholders[:len(placeholders)-1] // Remove trailing comma

	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}

	query := fmt.Sprintf("DELETE FROM queue_hashes WHERE key_name = ? AND field IN (%s)", placeholders)
	_, err := m.db.ExecContext(ctx, query, args...)
	return err
}

// Sorted Set Operations
func (m *MySQLQueueStorage) ZAdd(ctx context.Context, key string, members ...ScoredMember) error {
	if len(members) == 0 {
		return nil
	}

	query := "INSERT INTO queue_sorted_sets (key_name, member, score) VALUES "
	args := make([]interface{}, 0, len(members)*3)
	placeholders := make([]string, len(members))

	for i, member := range members {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, key, member.Member, member.Score)
	}

	query += strings.Join(placeholders, ", ")
	query += " ON DUPLICATE KEY UPDATE score = VALUES(score)"

	_, err := m.db.ExecContext(ctx, query, args...)
	return err
}

func (m *MySQLQueueStorage) ZRem(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	placeholders := strings.Repeat("?,", len(members))
	placeholders = placeholders[:len(placeholders)-1] // Remove trailing comma

	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	for _, member := range members {
		args = append(args, member)
	}

	query := fmt.Sprintf("DELETE FROM queue_sorted_sets WHERE key_name = ? AND member IN (%s)", placeholders)
	_, err := m.db.ExecContext(ctx, query, args...)
	return err
}

func (m *MySQLQueueStorage) ZRange(ctx context.Context, key string, scoreRange ScoreRange) ([]ScoredMember, error) {
	query := "SELECT member, score FROM queue_sorted_sets WHERE key_name = ?"
	args := []interface{}{key}

	// Add score range conditions if specified
	if scoreRange.Min != nil {
		query += " AND score >= ?"
		args = append(args, *scoreRange.Min)
	}

	if scoreRange.Max != nil {
		query += " AND score <= ?"
		args = append(args, *scoreRange.Max)
	}

	query += " ORDER BY score ASC"

	// Add pagination if specified
	if scoreRange.Count > 0 {
		query += " LIMIT ? OFFSET ?"
		args = append(args, scoreRange.Count, scoreRange.Offset)
	} else if scoreRange.Offset > 0 {
		query += " LIMIT 18446744073709551615 OFFSET ?"
		args = append(args, scoreRange.Offset)
	}

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ScoredMember
	for rows.Next() {
		var member ScoredMember
		if err := rows.Scan(&member.Member, &member.Score); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	return members, rows.Err()
}

func (m *MySQLQueueStorage) ZScore(ctx context.Context, key, member string) (float64, error) {
	var score float64
	err := m.db.QueryRowContext(ctx,
		"SELECT score FROM queue_sorted_sets WHERE key_name = ? AND member = ?",
		key, member).Scan(&score)

	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("redis: nil") // Mimic Redis nil error
	}
	return score, err
}

func (m *MySQLQueueStorage) ZCard(ctx context.Context, key string) (int64, error) {
	var count int64
	err := m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM queue_sorted_sets WHERE key_name = ?", key).Scan(&count)
	return count, err
}

// ZIncrBy atomically increments the score of a member
func (m *MySQLQueueStorage) ZIncrBy(ctx context.Context, key, member string, increment float64) (float64, error) {
	// Use INSERT ... ON DUPLICATE KEY UPDATE for atomic increment
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO queue_sorted_sets (key_name, member, score)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE score = score + VALUES(score)`,
		key, member, increment)
	if err != nil {
		return 0, err
	}

	// Get the new value
	var newScore float64
	err = m.db.QueryRowContext(ctx,
		"SELECT score FROM queue_sorted_sets WHERE key_name = ? AND member = ?",
		key, member).Scan(&newScore)
	return newScore, err
}

func (m *MySQLQueueStorage) ZSum(ctx context.Context, key string) (float64, error) {
	var sum sql.NullFloat64
	err := m.db.QueryRowContext(ctx,
		"SELECT SUM(score) FROM queue_sorted_sets WHERE key_name = ?",
		key).Scan(&sum)

	if err != nil {
		return 0, err
	}

	if !sum.Valid {
		return 0, nil // No rows found, return 0
	}

	return sum.Float64, nil
}
