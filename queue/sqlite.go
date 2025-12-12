package queue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteQueueStorage struct {
	db *sql.DB
}

// Close closes the database connection
func (s *SQLiteQueueStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func NewSQLiteQueueStorage(connectionString string) (*SQLiteQueueStorage, error) {
	// Parse connection string to determine database path
	var dbPath string
	
	if connectionString == "" {
		// Default to ~/.1sat/queue.db
		homeDir, err := os.UserHomeDir()
		if err != nil {
			dbPath = "./queue.db" // Fallback
		} else {
			dotOneSatDir := filepath.Join(homeDir, ".1sat")
			if err := os.MkdirAll(dotOneSatDir, 0755); err != nil {
				dbPath = "./queue.db" // Fallback if can't create dir
			} else {
				dbPath = filepath.Join(dotOneSatDir, "queue.db")
			}
		}
	} else if strings.HasPrefix(connectionString, "sqlite://") {
		// Remove sqlite:// prefix (can be sqlite:// or sqlite:///path)
		dbPath = strings.TrimPrefix(connectionString, "sqlite://")
		dbPath = strings.TrimPrefix(dbPath, "/") // Handle sqlite:///path format
		if dbPath == "" {
			dbPath = "./queue.db"
		}
	} else {
		// Direct path
		dbPath = connectionString
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Configure SQLite for concurrent access
	if _, err = db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return nil, err
	}
	if _, err = db.Exec("PRAGMA synchronous=NORMAL;"); err != nil {
		return nil, err
	}
	if _, err = db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		return nil, err
	}

	// Set connection pool limits to prevent goroutine explosion
	db.SetMaxOpenConns(10)   // Shared queue database can handle more connections
	db.SetMaxIdleConns(3)    // Keep a few idle connections
	db.SetConnMaxLifetime(0) // No connection lifetime limit

	s := &SQLiteQueueStorage{db: db}
	if err := s.createTables(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SQLiteQueueStorage) createTables() error {
	queries := []string{
		// Sets table
		`CREATE TABLE IF NOT EXISTS sets (
			key_name TEXT NOT NULL,
			member TEXT NOT NULL,
			created_at INTEGER DEFAULT (unixepoch()),
			PRIMARY KEY (key_name, member)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_sets_key ON sets(key_name)`,

		// Hashes table
		`CREATE TABLE IF NOT EXISTS hashes (
			key_name TEXT NOT NULL,
			field TEXT NOT NULL,
			value TEXT NOT NULL,
			created_at INTEGER DEFAULT (unixepoch()),
			PRIMARY KEY (key_name, field)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_hashes_key ON hashes(key_name)`,

		// Sorted sets table
		`CREATE TABLE IF NOT EXISTS sorted_sets (
			key_name TEXT NOT NULL,
			member TEXT NOT NULL,
			score REAL NOT NULL,
			created_at INTEGER DEFAULT (unixepoch()),
			PRIMARY KEY (key_name, member)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_sorted_sets_key_score ON sorted_sets(key_name, score)`,
	}

	for _, query := range queries {
		if _, err := s.db.Exec(query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	return nil
}

// Set Operations
func (s *SQLiteQueueStorage) SAdd(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	query := "INSERT OR IGNORE INTO sets (key_name, member) VALUES "
	args := make([]interface{}, 0, len(members)*2)
	placeholders := make([]string, len(members))

	for i, member := range members {
		placeholders[i] = "(?, ?)"
		args = append(args, key, member)
	}

	query += strings.Join(placeholders, ", ")
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLiteQueueStorage) SMembers(ctx context.Context, key string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT member FROM sets WHERE key_name = ?", key)
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

func (s *SQLiteQueueStorage) SRem(ctx context.Context, key string, members ...string) error {
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

	query := fmt.Sprintf("DELETE FROM sets WHERE key_name = ? AND member IN (%s)", placeholders)
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLiteQueueStorage) SIsMember(ctx context.Context, key, member string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM sets WHERE key_name = ? AND member = ?", key, member).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return exists == 1, err
}

// Hash Operations
func (s *SQLiteQueueStorage) HSet(ctx context.Context, key, field, value string) error {
	_, err := s.db.ExecContext(ctx, "INSERT OR REPLACE INTO hashes (key_name, field, value) VALUES (?, ?, ?)", key, field, value)
	return err
}

func (s *SQLiteQueueStorage) HGet(ctx context.Context, key, field string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM hashes WHERE key_name = ? AND field = ?", key, field).Scan(&value)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("redis: nil") // Mimic Redis nil error
	}
	return value, err
}

func (s *SQLiteQueueStorage) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT field, value FROM hashes WHERE key_name = ?", key)
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

func (s *SQLiteQueueStorage) HDel(ctx context.Context, key string, fields ...string) error {
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

	query := fmt.Sprintf("DELETE FROM hashes WHERE key_name = ? AND field IN (%s)", placeholders)
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLiteQueueStorage) HMSet(ctx context.Context, key string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}

	query := "INSERT OR REPLACE INTO hashes (key_name, field, value) VALUES "
	args := make([]interface{}, 0, len(fields)*3)
	placeholders := make([]string, 0, len(fields))

	for field, value := range fields {
		placeholders = append(placeholders, "(?, ?, ?)")
		args = append(args, key, field, value)
	}

	query += strings.Join(placeholders, ", ")
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLiteQueueStorage) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	if len(fields) == 0 {
		return []string{}, nil
	}

	placeholders := strings.Repeat("?,", len(fields))
	placeholders = placeholders[:len(placeholders)-1]

	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}

	query := fmt.Sprintf("SELECT field, value FROM hashes WHERE key_name = ? AND field IN (%s)", placeholders)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build a map of field -> value
	resultMap := make(map[string]string)
	for rows.Next() {
		var field, value string
		if err := rows.Scan(&field, &value); err != nil {
			return nil, err
		}
		resultMap[field] = value
	}

	// Return values in the same order as requested fields
	values := make([]string, len(fields))
	for i, field := range fields {
		values[i] = resultMap[field] // Empty string if not found
	}
	return values, rows.Err()
}

// Sorted Set Operations
func (s *SQLiteQueueStorage) ZAdd(ctx context.Context, key string, members ...ScoredMember) error {
	if len(members) == 0 {
		return nil
	}

	query := "INSERT OR REPLACE INTO sorted_sets (key_name, member, score) VALUES "
	args := make([]interface{}, 0, len(members)*3)
	placeholders := make([]string, len(members))

	for i, member := range members {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, key, member.Member, member.Score)
	}

	query += strings.Join(placeholders, ", ")
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

func (s *SQLiteQueueStorage) ZRem(ctx context.Context, key string, members ...string) error {
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

	query := fmt.Sprintf("DELETE FROM sorted_sets WHERE key_name = ? AND member IN (%s)", placeholders)
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}


func (s *SQLiteQueueStorage) ZRange(ctx context.Context, key string, scoreRange ScoreRange) ([]ScoredMember, error) {
	return s.zRangeInternal(ctx, key, scoreRange, false)
}

func (s *SQLiteQueueStorage) ZRevRange(ctx context.Context, key string, scoreRange ScoreRange) ([]ScoredMember, error) {
	return s.zRangeInternal(ctx, key, scoreRange, true)
}

func (s *SQLiteQueueStorage) zRangeInternal(ctx context.Context, key string, scoreRange ScoreRange, reverse bool) ([]ScoredMember, error) {
	query := "SELECT member, score FROM sorted_sets WHERE key_name = ?"
	args := []interface{}{key}

	if scoreRange.Min != nil {
		query += " AND score >= ?"
		args = append(args, *scoreRange.Min)
	}

	if scoreRange.Max != nil {
		query += " AND score <= ?"
		args = append(args, *scoreRange.Max)
	}

	if reverse {
		query += " ORDER BY score DESC"
	} else {
		query += " ORDER BY score ASC"
	}

	if scoreRange.Count > 0 {
		query += " LIMIT ? OFFSET ?"
		args = append(args, scoreRange.Count, scoreRange.Offset)
	} else if scoreRange.Offset > 0 {
		query += " LIMIT -1 OFFSET ?"
		args = append(args, scoreRange.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
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

func (s *SQLiteQueueStorage) ZScore(ctx context.Context, key, member string) (float64, error) {
	var score float64
	err := s.db.QueryRowContext(ctx,
		"SELECT score FROM sorted_sets WHERE key_name = ? AND member = ?",
		key, member).Scan(&score)

	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("redis: nil") // Mimic Redis nil error
	}
	return score, err
}

func (s *SQLiteQueueStorage) ZCard(ctx context.Context, key string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sorted_sets WHERE key_name = ?", key).Scan(&count)
	return count, err
}

// Fee balance management
func (s *SQLiteQueueStorage) ZIncrBy(ctx context.Context, key, member string, increment float64) (float64, error) {
	// Use INSERT OR REPLACE with COALESCE for atomic increment
	_, err := s.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO sorted_sets (key_name, member, score) 
		VALUES (?, ?, COALESCE((SELECT score FROM sorted_sets WHERE key_name = ? AND member = ?), 0) + ?)
	`, key, member, key, member, increment)
	if err != nil {
		return 0, err
	}

	// Get the new value
	var newScore float64
	err = s.db.QueryRowContext(ctx,
		"SELECT score FROM sorted_sets WHERE key_name = ? AND member = ?",
		key, member).Scan(&newScore)
	return newScore, err
}

func (s *SQLiteQueueStorage) ZSum(ctx context.Context, key string) (float64, error) {
	var sum sql.NullFloat64
	err := s.db.QueryRowContext(ctx,
		"SELECT SUM(score) FROM sorted_sets WHERE key_name = ?",
		key).Scan(&sum)
	
	if err != nil {
		return 0, err
	}
	
	if !sum.Valid {
		return 0, nil // No rows found, return 0
	}
	
	return sum.Float64, nil
}