package queue

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteQueueStorage struct {
	db *sql.DB
}

func NewSQLiteQueueStorage(dbPath string) (*SQLiteQueueStorage, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

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

func (s *SQLiteQueueStorage) ZRangeByScore(ctx context.Context, key string, min, max float64, offset, count int64) ([]ScoredMember, error) {
	query := "SELECT member, score FROM sorted_sets WHERE key_name = ? AND score >= ? AND score <= ? ORDER BY score ASC"
	args := []interface{}{key, min, max}

	if count > 0 {
		query += " LIMIT ? OFFSET ?"
		args = append(args, count, offset)
	} else if offset > 0 {
		query += " LIMIT -1 OFFSET ?"
		args = append(args, offset)
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