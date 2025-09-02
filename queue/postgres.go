package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresQueueStorage implements QueueStorage using PostgreSQL tables
type PostgresQueueStorage struct {
	pool *pgxpool.Pool
}

// NewPostgresQueueStorage creates a new PostgreSQL queue storage
func NewPostgresQueueStorage(connectionString string) (*PostgresQueueStorage, error) {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Configure connection pool settings for queue operations - reduced for shared memory constraints
	config.MaxConns = 100
	config.MinConns = 1

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	storage := &PostgresQueueStorage{pool: pool}

	// Create tables
	if err := storage.createTables(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return storage, nil
}

// createTables creates the Redis-like tables for queue storage
func (q *PostgresQueueStorage) createTables(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS queue_hashes (
			key_name TEXT NOT NULL,
			field TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (key_name, field)
		)`,

		`CREATE TABLE IF NOT EXISTS queue_sets (
			key_name TEXT NOT NULL,
			member TEXT NOT NULL,
			PRIMARY KEY (key_name, member)
		)`,

		`CREATE TABLE IF NOT EXISTS queue_sorted_sets (
			key_name TEXT NOT NULL,
			member TEXT NOT NULL,
			score DOUBLE PRECISION NOT NULL,
			created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
			PRIMARY KEY (key_name, member)
		)`,
	}

	for _, query := range queries {
		if _, err := q.pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Create indexes
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_queue_sets_key ON queue_sets(key_name)`,
		`CREATE INDEX IF NOT EXISTS idx_queue_sorted_sets_key_score ON queue_sorted_sets(key_name, score)`,
	}

	for _, indexQuery := range indexes {
		if _, err := q.pool.Exec(ctx, indexQuery); err != nil {
			// Log but don't fail on index creation errors
			continue
		}
	}

	return nil
}

// Set Operations
func (q *PostgresQueueStorage) SAdd(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	// Build bulk insert statement
	valueStrings := make([]string, len(members))
	valueArgs := make([]interface{}, 0, len(members)*2)

	for i, member := range members {
		valueStrings[i] = fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2)
		valueArgs = append(valueArgs, key, member)
	}

	query := fmt.Sprintf(`
		INSERT INTO queue_sets (key_name, member) 
		VALUES %s 
		ON CONFLICT (key_name, member) DO NOTHING`,
		strings.Join(valueStrings, ","))

	_, err := q.pool.Exec(ctx, query, valueArgs...)
	return err
}

func (q *PostgresQueueStorage) SMembers(ctx context.Context, key string) ([]string, error) {
	rows, err := q.pool.Query(ctx, `
		SELECT member FROM queue_sets 
		WHERE key_name = $1 
		ORDER BY member`, key)
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

func (q *PostgresQueueStorage) SRem(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	placeholders := make([]string, len(members))
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)

	for i, member := range members {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, member)
	}

	query := fmt.Sprintf(`DELETE FROM queue_sets WHERE key_name = $1 AND member IN (%s)`,
		strings.Join(placeholders, ","))

	_, err := q.pool.Exec(ctx, query, args...)
	return err
}

func (q *PostgresQueueStorage) SIsMember(ctx context.Context, key, member string) (bool, error) {
	var exists bool
	err := q.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM queue_sets WHERE key_name = $1 AND member = $2)`,
		key, member).Scan(&exists)
	return exists, err
}

// Hash Operations
func (q *PostgresQueueStorage) HSet(ctx context.Context, key, field, value string) error {
	_, err := q.pool.Exec(ctx, `
		INSERT INTO queue_hashes (key_name, field, value) 
		VALUES ($1, $2, $3) 
		ON CONFLICT (key_name, field) DO UPDATE SET value = EXCLUDED.value`,
		key, field, value)
	return err
}

func (q *PostgresQueueStorage) HGet(ctx context.Context, key, field string) (string, error) {
	var value string
	err := q.pool.QueryRow(ctx, `
		SELECT value FROM queue_hashes 
		WHERE key_name = $1 AND field = $2`,
		key, field).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return value, err
}

func (q *PostgresQueueStorage) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	rows, err := q.pool.Query(ctx, `
		SELECT field, value FROM queue_hashes 
		WHERE key_name = $1`, key)
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

func (q *PostgresQueueStorage) HDel(ctx context.Context, key string, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}

	placeholders := make([]string, len(fields))
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)

	for i, field := range fields {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, field)
	}

	query := fmt.Sprintf(`DELETE FROM queue_hashes WHERE key_name = $1 AND field IN (%s)`,
		strings.Join(placeholders, ","))

	_, err := q.pool.Exec(ctx, query, args...)
	return err
}

// Sorted Set Operations
func (q *PostgresQueueStorage) ZAdd(ctx context.Context, key string, members ...ScoredMember) error {
	if len(members) == 0 {
		return nil
	}

	// Build bulk insert statement
	valueStrings := make([]string, len(members))
	valueArgs := make([]interface{}, 0, len(members)*3)

	for i, member := range members {
		valueStrings[i] = fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3)
		valueArgs = append(valueArgs, key, member.Member, member.Score)
	}

	query := fmt.Sprintf(`
		INSERT INTO queue_sorted_sets (key_name, member, score) 
		VALUES %s 
		ON CONFLICT (key_name, member) DO UPDATE SET score = EXCLUDED.score`,
		strings.Join(valueStrings, ","))

	_, err := q.pool.Exec(ctx, query, valueArgs...)
	return err
}

func (q *PostgresQueueStorage) ZRem(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	placeholders := make([]string, len(members))
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)

	for i, member := range members {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, member)
	}

	query := fmt.Sprintf(`DELETE FROM queue_sorted_sets WHERE key_name = $1 AND member IN (%s)`,
		strings.Join(placeholders, ","))

	_, err := q.pool.Exec(ctx, query, args...)
	return err
}


func (q *PostgresQueueStorage) ZRange(ctx context.Context, key string, scoreRange ScoreRange) ([]ScoredMember, error) {
	query := "SELECT member, score FROM queue_sorted_sets WHERE key_name = $1"
	args := []interface{}{key}
	argCount := 1
	
	// Add score range conditions if specified
	if scoreRange.Min != nil {
		argCount++
		query += fmt.Sprintf(" AND score >= $%d", argCount)
		args = append(args, *scoreRange.Min)
	}
	
	if scoreRange.Max != nil {
		argCount++
		query += fmt.Sprintf(" AND score <= $%d", argCount)
		args = append(args, *scoreRange.Max)
	}
	
	query += " ORDER BY score ASC"
	
	// Add pagination if specified
	if scoreRange.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", scoreRange.Offset)
	}
	
	if scoreRange.Count > 0 {
		query += fmt.Sprintf(" LIMIT %d", scoreRange.Count)
	}
	
	rows, err := q.pool.Query(ctx, query, args...)
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

func (q *PostgresQueueStorage) ZScore(ctx context.Context, key, member string) (float64, error) {
	var score float64
	err := q.pool.QueryRow(ctx, `
		SELECT score FROM queue_sorted_sets 
		WHERE key_name = $1 AND member = $2`,
		key, member).Scan(&score)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return score, err
}

func (q *PostgresQueueStorage) ZCard(ctx context.Context, key string) (int64, error) {
	var count int64
	err := q.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM queue_sorted_sets WHERE key_name = $1`,
		key).Scan(&count)
	return count, err
}

func (q *PostgresQueueStorage) ZIncrBy(ctx context.Context, key, member string, increment float64) (float64, error) {
	var newScore float64
	err := q.pool.QueryRow(ctx, `
		INSERT INTO queue_sorted_sets (key_name, member, score) 
		VALUES ($1, $2, $3) 
		ON CONFLICT (key_name, member) DO UPDATE SET score = queue_sorted_sets.score + EXCLUDED.score
		RETURNING score`,
		key, member, increment).Scan(&newScore)
	return newScore, err
}

func (q *PostgresQueueStorage) ZSum(ctx context.Context, key string) (float64, error) {
	var sum float64
	err := q.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(score), 0) FROM queue_sorted_sets WHERE key_name = $1`,
		key).Scan(&sum)
	return sum, err
}

func (q *PostgresQueueStorage) Close() error {
	q.pool.Close()
	return nil
}