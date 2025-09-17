package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSharedPool manages a single connection pool shared across all topics
type PostgresSharedPool struct {
	pool        *pgxpool.Pool
	initialized bool
	refCount    int
	mutex       sync.RWMutex
}

// Global shared pool instance
var sharedPool = &PostgresSharedPool{}

// PostgresTopicDataStorage implements TopicDataStorage using PostgreSQL with shared connection pool
type PostgresTopicDataStorage struct {
	BaseEventDataStorage
	pool  *pgxpool.Pool
	topic string
}

// NewPostgresTopicDataStorage creates a new PostgreSQL topic storage using shared connection pool
func NewPostgresTopicDataStorage(topic string, connectionString string, beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubsub pubsub.PubSub) (TopicDataStorage, error) {
	// Get or create shared connection pool
	pool, err := getSharedPool(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to get shared pool: %w", err)
	}

	storage := &PostgresTopicDataStorage{
		BaseEventDataStorage: NewBaseEventDataStorage(beefStore, queueStorage, pubsub),
		pool:                 pool,
		topic:                topic,
	}

	// Ensure parent tables exist (idempotent)
	if err := storage.createTables(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	// Ensure partitions exist for this topic (idempotent)
	if err := storage.ensureTopicPartitions(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to create topic partitions: %w", err)
	}

	return storage, nil
}

// getSharedPool returns the shared connection pool, creating it if necessary
func getSharedPool(connectionString string) (*pgxpool.Pool, error) {
	sharedPool.mutex.Lock()
	defer sharedPool.mutex.Unlock()

	if sharedPool.initialized {
		sharedPool.refCount++
		return sharedPool.pool, nil
	}

	// Parse connection string and configure pool
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Configure connection pool settings - reduced for shared memory constraints
	config.MaxConns = 100
	config.MinConns = 2
	config.MaxConnIdleTime = 30 * time.Minute
	config.MaxConnLifetime = 1 * time.Hour

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	sharedPool.pool = pool
	sharedPool.initialized = true
	sharedPool.refCount = 1

	return pool, nil
}

// GetTopic returns the topic this storage handles
func (s *PostgresTopicDataStorage) GetTopic() string {
	return s.topic
}

// createTables creates all required partitioned tables with proper indexes (idempotent)
func (s *PostgresTopicDataStorage) createTables(ctx context.Context) error {
	// Create parent tables with partitioning
	parentTables := []string{
		// Outputs table - partitioned by topic
		`CREATE TABLE IF NOT EXISTS outputs (
			outpoint TEXT NOT NULL,
			txid TEXT NOT NULL,
			script BYTEA NOT NULL,
			satoshis BIGINT NOT NULL,
			spend TEXT,
			block_height INTEGER,
			block_idx INTEGER,
			score DOUBLE PRECISION,
			ancillary_beef BYTEA,
			data JSONB,
			topic TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (topic, outpoint)
		) PARTITION BY LIST (topic)`,

		// Events table - partitioned by topic
		`CREATE TABLE IF NOT EXISTS events (
			event TEXT NOT NULL,
			outpoint TEXT NOT NULL,
			score DOUBLE PRECISION NOT NULL,
			spend TEXT,
			topic TEXT NOT NULL,
			PRIMARY KEY (topic, event, outpoint)
		) PARTITION BY LIST (topic)`,

		// Transactions table - partitioned by topic
		`CREATE TABLE IF NOT EXISTS transactions (
			txid TEXT NOT NULL,
			score DOUBLE PRECISION,
			topic TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (topic, txid)
		) PARTITION BY LIST (topic)`,

		// Output relationships table - partitioned by topic
		`CREATE TABLE IF NOT EXISTS output_relationships (
			consuming_outpoint TEXT NOT NULL,
			consumed_outpoint TEXT NOT NULL,
			topic TEXT NOT NULL,
			PRIMARY KEY (topic, consuming_outpoint, consumed_outpoint)
		) PARTITION BY LIST (topic)`,
	}

	// Create parent tables
	for _, query := range parentTables {
		if _, err := s.pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create parent table: %w", err)
		}
	}

	return nil
}

// ensureTopicPartitions creates partitions for the given topic if they don't exist
func (s *PostgresTopicDataStorage) ensureTopicPartitions(ctx context.Context) error {
	// Sanitize topic name for use as table suffix (replace non-alphanumeric with underscore)
	sanitizedTopic := strings.ReplaceAll(s.topic, "-", "_")
	sanitizedTopic = strings.ReplaceAll(sanitizedTopic, ".", "_")

	partitions := []struct {
		parentTable string
		partName    string
		indexes     []string
	}{
		{
			parentTable: "outputs",
			partName:    fmt.Sprintf("outputs_%s", sanitizedTopic),
			indexes: []string{
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_outpoint ON %[1]s(outpoint)`,
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_txid ON %[1]s(txid)`,
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_score ON %[1]s(score)`,
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_spend_score ON %[1]s(spend, score)`,
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_block_height ON %[1]s(block_height)`,
			},
		},
		{
			parentTable: "events",
			partName:    fmt.Sprintf("events_%s", sanitizedTopic),
			indexes: []string{
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_event_score_spend ON %[1]s(event, score, spend)`,
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_outpoint ON %[1]s(outpoint)`,
			},
		},
		{
			parentTable: "transactions",
			partName:    fmt.Sprintf("transactions_%s", sanitizedTopic),
			indexes: []string{
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_txid ON %[1]s(txid)`,
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_score ON %[1]s(score)`,
			},
		},
		{
			parentTable: "output_relationships",
			partName:    fmt.Sprintf("output_relationships_%s", sanitizedTopic),
			indexes: []string{
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_consuming ON %[1]s(consuming_outpoint)`,
				`CREATE INDEX IF NOT EXISTS idx_%[1]s_consumed ON %[1]s(consumed_outpoint)`,
			},
		},
	}

	for _, partition := range partitions {
		// Create partition
		createPartitionSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s 
			PARTITION OF %s 
			FOR VALUES IN ('%s')`,
			partition.partName, partition.parentTable, s.topic)

		if _, err := s.pool.Exec(ctx, createPartitionSQL); err != nil {
			return fmt.Errorf("failed to create partition %s: %w", partition.partName, err)
		}

		// Create indexes on the partition
		for _, indexTemplate := range partition.indexes {
			indexSQL := fmt.Sprintf(indexTemplate, partition.partName)
			if _, err := s.pool.Exec(ctx, indexSQL); err != nil {
				log.Printf("Index creation warning on partition %s (may be normal): %v", partition.partName, err)
			}
		}
	}

	return nil
}

// InsertOutput inserts a new output into the database
func (s *PostgresTopicDataStorage) InsertOutput(ctx context.Context, utxo *engine.Output) error {
	// Save BEEF to BEEF storage
	if err := s.beefStore.SaveBeef(ctx, &utxo.Outpoint.Txid, utxo.Beef); err != nil {
		return err
	}

	// Calculate score
	utxo.Score = float64(time.Now().UnixNano())

	// Insert output
	_, err := s.pool.Exec(ctx, `
		INSERT INTO outputs (outpoint, txid, script, satoshis, block_height, block_idx, score, ancillary_beef, topic)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (topic, outpoint) DO UPDATE SET
			script = EXCLUDED.script,
			satoshis = EXCLUDED.satoshis,
			block_height = EXCLUDED.block_height,
			block_idx = EXCLUDED.block_idx,
			score = EXCLUDED.score,
			ancillary_beef = EXCLUDED.ancillary_beef`,
		utxo.Outpoint.String(),
		utxo.Outpoint.Txid.String(),
		utxo.Script.Bytes(),
		utxo.Satoshis,
		utxo.BlockHeight,
		utxo.BlockIdx,
		utxo.Score,
		utxo.AncillaryBeef,
		s.topic,
	)
	if err != nil {
		return err
	}

	// Batch insert output relationships
	type relationship struct {
		consuming string
		consumed  string
	}

	var relationships []relationship

	// Collect all relationships
	for _, consumed := range utxo.OutputsConsumed {
		relationships = append(relationships, relationship{
			consuming: utxo.Outpoint.String(),
			consumed:  consumed.String(),
		})
	}

	for _, consumedBy := range utxo.ConsumedBy {
		relationships = append(relationships, relationship{
			consuming: consumedBy.String(),
			consumed:  utxo.Outpoint.String(),
		})
	}

	// Batch insert all relationships in single statement
	if len(relationships) > 0 {
		valueStrings := make([]string, len(relationships))
		valueArgs := make([]interface{}, 0, len(relationships)*3)

		for i, rel := range relationships {
			valueStrings[i] = fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3)
			valueArgs = append(valueArgs, rel.consuming, rel.consumed, s.topic)
		}

		query := fmt.Sprintf(`
			INSERT INTO output_relationships (consuming_outpoint, consumed_outpoint, topic)
			VALUES %s
			ON CONFLICT (topic, consuming_outpoint, consumed_outpoint) DO NOTHING`,
			strings.Join(valueStrings, ","))

		_, err = s.pool.Exec(ctx, query, valueArgs...)
		if err != nil {
			return err
		}
	}

	// // Add topic as an event using SaveEvents (handles pubsub publishing)
	// if err := s.SaveEvents(ctx, &utxo.Outpoint, []string{s.topic}, utxo.Score, nil); err != nil {
	// 	return err
	// }

	return nil
}

// FindOutput finds a single output by outpoint
func (s *PostgresTopicDataStorage) FindOutput(ctx context.Context, outpoint *transaction.Outpoint, spent *bool, includeBEEF bool) (*engine.Output, error) {
	query := `SELECT outpoint, txid, script, satoshis, spend, block_height, block_idx, score, ancillary_beef 
		FROM outputs WHERE topic = $1 AND outpoint = $2`
	args := []interface{}{s.topic, outpoint.String()}

	if spent != nil {
		if *spent {
			query += " AND spend IS NOT NULL"
		} else {
			query += " AND spend IS NULL"
		}
	}

	var output engine.Output
	var outpointStr, txidStr string
	var scriptBytes []byte
	var ancillaryBeef []byte
	var spendTxid *string

	err := s.pool.QueryRow(ctx, query, args...).Scan(
		&outpointStr,
		&txidStr,
		&scriptBytes,
		&output.Satoshis,
		&spendTxid,
		&output.BlockHeight,
		&output.BlockIdx,
		&output.Score,
		&ancillaryBeef,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	// Parse outpoint
	op, err := transaction.OutpointFromString(outpointStr)
	if err != nil {
		return nil, err
	}
	output.Outpoint = *op

	// Set topic from storage
	output.Topic = s.topic

	// Set spent status based on whether spend field has a value
	output.Spent = spendTxid != nil

	// Parse script
	output.Script = script.NewFromBytes(scriptBytes)
	output.AncillaryBeef = ancillaryBeef

	// Load BEEF if requested
	if includeBEEF {
		beef, err := s.beefStore.LoadBeef(ctx, &output.Outpoint.Txid)
		if err != nil {
			return nil, err
		}
		output.Beef = beef
	}

	// Load consumed outputs
	if err = s.loadOutputRelations(ctx, &output); err != nil {
		return nil, err
	}

	return &output, nil
}

// FindOutputs finds multiple outputs by outpoints
func (s *PostgresTopicDataStorage) FindOutputs(ctx context.Context, outpoints []*transaction.Outpoint, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
	if len(outpoints) == 0 {
		return nil, nil
	}

	// Build query with placeholders
	placeholders := make([]string, len(outpoints))
	args := make([]interface{}, 0, len(outpoints)+2)
	args = append(args, s.topic)

	for i, op := range outpoints {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, op.String())
	}

	query := fmt.Sprintf(`SELECT outpoint, txid, script, satoshis, spend, block_height, block_idx, score, ancillary_beef 
		FROM outputs WHERE topic = $1 AND outpoint IN (%s)`, strings.Join(placeholders, ","))

	if spent != nil {
		if *spent {
			query += " AND spend IS NOT NULL"
		} else {
			query += " AND spend IS NULL"
		}
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Create map to store found outputs by outpoint
	resultsByOutpoint := make(map[transaction.Outpoint]*engine.Output)
	for rows.Next() {
		output, err := s.scanOutput(rows, includeBEEF)
		if err != nil {
			return nil, err
		}
		resultsByOutpoint[output.Outpoint] = output
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Build result slice in same order as input, with nils for missing outputs
	outputs := make([]*engine.Output, len(outpoints))
	for i, outpoint := range outpoints {
		outputs[i] = resultsByOutpoint[*outpoint]
	}

	return outputs, nil
}

// Helper method to scan output from database row
func (s *PostgresTopicDataStorage) scanOutput(rows pgx.Rows, includeBEEF bool) (*engine.Output, error) {
	var output engine.Output
	var outpointStr, txidStr string
	var scriptBytes []byte
	var ancillaryBeef []byte
	var spendTxid *string

	err := rows.Scan(
		&outpointStr,
		&txidStr,
		&scriptBytes,
		&output.Satoshis,
		&spendTxid,
		&output.BlockHeight,
		&output.BlockIdx,
		&output.Score,
		&ancillaryBeef,
	)
	if err != nil {
		return nil, err
	}

	// Parse outpoint
	op, err := transaction.OutpointFromString(outpointStr)
	if err != nil {
		return nil, err
	}
	output.Outpoint = *op

	// Set topic from storage
	output.Topic = s.topic

	// Set spent status based on whether spend field has a value
	output.Spent = spendTxid != nil

	// Parse script
	output.Script = script.NewFromBytes(scriptBytes)
	output.AncillaryBeef = ancillaryBeef

	// Load BEEF if requested
	if includeBEEF {
		beef, err := s.beefStore.LoadBeef(context.Background(), &output.Outpoint.Txid)
		if err != nil {
			log.Printf("Failed to load BEEF for %s: %v", output.Outpoint.Txid, err)
		} else {
			output.Beef = beef
		}
	}

	// Load relations
	if err = s.loadOutputRelations(context.Background(), &output); err != nil {
		return nil, err
	}

	return &output, nil
}

// loadOutputRelations loads the input/output relationships for an output
func (s *PostgresTopicDataStorage) loadOutputRelations(ctx context.Context, output *engine.Output) error {
	// Load outputs consumed by this output
	rows, err := s.pool.Query(ctx,
		"SELECT consumed_outpoint FROM output_relationships WHERE topic = $1 AND consuming_outpoint = $2",
		s.topic, output.Outpoint.String())
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var opStr string
		if err := rows.Scan(&opStr); err != nil {
			return err
		}
		op, err := transaction.OutpointFromString(opStr)
		if err != nil {
			return err
		}
		output.OutputsConsumed = append(output.OutputsConsumed, op)
	}

	// Load outputs that consume this output
	rows2, err := s.pool.Query(ctx,
		"SELECT consuming_outpoint FROM output_relationships WHERE topic = $1 AND consumed_outpoint = $2",
		s.topic, output.Outpoint.String())
	if err != nil {
		return err
	}
	defer rows2.Close()

	for rows2.Next() {
		var opStr string
		if err := rows2.Scan(&opStr); err != nil {
			return err
		}
		op, err := transaction.OutpointFromString(opStr)
		if err != nil {
			return err
		}
		output.ConsumedBy = append(output.ConsumedBy, op)
	}

	return nil
}

// Close closes the storage and decrements shared pool reference count
func (s *PostgresTopicDataStorage) Close() error {
	sharedPool.mutex.Lock()
	defer sharedPool.mutex.Unlock()

	if sharedPool.initialized {
		sharedPool.refCount--
		if sharedPool.refCount == 0 {
			// No more references, close the shared pool
			sharedPool.pool.Close()
			sharedPool.pool = nil
			sharedPool.initialized = false
		}
	}
	return nil
}


// FindOutputsForTransaction finds all outputs for a given transaction
func (s *PostgresTopicDataStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
	query := `SELECT outpoint, txid, script, satoshis, spend, block_height, block_idx, score, ancillary_beef 
		FROM outputs WHERE topic = $1 AND txid = $2`

	rows, err := s.pool.Query(ctx, query, s.topic, txid.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []*engine.Output
	for rows.Next() {
		output, err := s.scanOutput(rows, includeBEEF)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, output)
	}

	return outputs, rows.Err()
}

// FindUTXOsForTopic finds unspent outputs for the topic since a given score
func (s *PostgresTopicDataStorage) FindUTXOsForTopic(ctx context.Context, since float64, limit uint32, includeBEEF bool) ([]*engine.Output, error) {
	query := `SELECT outpoint, txid, script, satoshis, spend, block_height, block_idx, score, ancillary_beef 
		FROM outputs 
		WHERE topic = $1 AND score >= $2 AND spend IS NULL
		ORDER BY score`

	args := []interface{}{s.topic, since}
	if limit > 0 {
		query += " LIMIT $3"
		args = append(args, limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []*engine.Output
	for rows.Next() {
		output, err := s.scanOutput(rows, includeBEEF)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, output)
	}

	return outputs, rows.Err()
}

// DeleteOutput deletes an output from the database
func (s *PostgresTopicDataStorage) DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint) error {
	_, err := s.pool.Exec(ctx,
		"DELETE FROM outputs WHERE topic = $1 AND outpoint = $2",
		s.topic, outpoint.String())
	return err
}

// MarkUTXOAsSpent marks a UTXO as spent
func (s *PostgresTopicDataStorage) MarkUTXOAsSpent(ctx context.Context, outpoint *transaction.Outpoint, beef []byte) error {
	// Parse the beef to get the spending txid
	_, _, spendTxid, err := transaction.ParseBeef(beef)
	if err != nil {
		return err
	}

	_, err = s.pool.Exec(ctx,
		"UPDATE outputs SET spend = $1 WHERE topic = $2 AND outpoint = $3",
		spendTxid.String(), s.topic, outpoint.String())
	return err
}

// MarkUTXOsAsSpent marks multiple UTXOs as spent
func (s *PostgresTopicDataStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*transaction.Outpoint, spendTxid *chainhash.Hash) error {
	if len(outpoints) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	spendTxidStr := spendTxid.String()

	for _, op := range outpoints {
		opStr := op.String()

		// Update outputs table
		_, err = tx.Exec(ctx,
			"UPDATE outputs SET spend = $1 WHERE topic = $2 AND outpoint = $3",
			spendTxidStr, s.topic, opStr)
		if err != nil {
			return err
		}

		// Update events table for all events associated with this outpoint
		_, err = tx.Exec(ctx,
			"UPDATE events SET spend = $1 WHERE topic = $2 AND outpoint = $3",
			spendTxidStr, s.topic, opStr)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateConsumedBy updates consumed-by relationships (no-op as managed atomically)
func (s *PostgresTopicDataStorage) UpdateConsumedBy(ctx context.Context, outpoint *transaction.Outpoint, consumedBy []*transaction.Outpoint) error {
	// No-op: Output relationships are managed by InsertOutput method atomically.
	return nil
}

// UpdateTransactionBEEF updates BEEF data for a transaction
func (s *PostgresTopicDataStorage) UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error {
	return s.beefStore.SaveBeef(ctx, txid, beef)
}

// UpdateOutputBlockHeight updates block height information for an output
func (s *PostgresTopicDataStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *transaction.Outpoint, blockHeight uint32, blockIndex uint64, ancillaryBeef []byte) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Update outputs table
	_, err = tx.Exec(ctx, `
		UPDATE outputs 
		SET block_height = $1, block_idx = $2, ancillary_beef = $3, score = $4
		WHERE topic = $5 AND outpoint = $6`,
		blockHeight,
		blockIndex,
		ancillaryBeef,
		float64(time.Now().UnixNano()),
		s.topic,
		outpoint.String(),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// InsertAppliedTransaction inserts an applied transaction record
func (s *PostgresTopicDataStorage) InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error {
	score := float64(time.Now().UnixNano())

	_, err := s.pool.Exec(ctx, `
		INSERT INTO transactions (topic, txid, score, created_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
		ON CONFLICT (topic, txid) DO UPDATE SET
			score = EXCLUDED.score,
			created_at = EXCLUDED.created_at`,
		s.topic, tx.Txid.String(), score)

	if err != nil {
		return err
	}

	// Publish transaction to topic via PubSub (if available)
	if s.pubsub != nil {
		if err := s.pubsub.Publish(ctx, s.topic, tx.Txid.String(), score); err != nil {
			log.Printf("Failed to publish transaction to topic %s: %v", s.topic, err)
		}
	}

	return nil
}

// DoesAppliedTransactionExist checks if an applied transaction exists
func (s *PostgresTopicDataStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM transactions WHERE topic = $1 AND txid = $2)",
		s.topic, tx.Txid.String()).Scan(&exists)
	return exists, err
}

// GetTransactionsByHeight returns all transactions for a topic at a specific block height
func (s *PostgresTopicDataStorage) GetTransactionsByHeight(ctx context.Context, height uint32) ([]*TransactionData, error) {
	// Query outputs for the topic and block height
	query := `SELECT outpoint, txid, script, satoshis, spend, data 
	         FROM outputs 
	         WHERE topic = $1 AND block_height = $2
	         ORDER BY txid, outpoint`

	rows, err := s.pool.Query(ctx, query, s.topic, height)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Group outputs by transaction ID
	txOutputMap := make(map[chainhash.Hash][]*OutputData)

	for rows.Next() {
		var outpointStr, txidStr string
		var scriptBytes []byte
		var satoshis uint64
		var spendTxid *string
		var dataJSON *string

		err := rows.Scan(&outpointStr, &txidStr, &scriptBytes, &satoshis, &spendTxid, &dataJSON)
		if err != nil {
			return nil, err
		}

		// Parse outpoint
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			return nil, err
		}

		// Parse data if present
		var data interface{}
		if dataJSON != nil && *dataJSON != "" {
			if err := json.Unmarshal([]byte(*dataJSON), &data); err == nil {
				// Successfully parsed JSON data
			}
		}

		// Create OutputData
		outputData := &OutputData{
			Vout:     outpoint.Index,
			Data:     data,
			Script:   scriptBytes,
			Satoshis: satoshis,
		}

		// Parse txid
		txid, err := chainhash.NewHashFromHex(txidStr)
		if err != nil {
			return nil, err
		}

		txOutputMap[*txid] = append(txOutputMap[*txid], outputData)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Get all transaction IDs for batch input query
	txids := make([]string, 0, len(txOutputMap))
	for txid := range txOutputMap {
		txids = append(txids, txid.String())
	}

	if len(txids) == 0 {
		return []*TransactionData{}, nil
	}

	// Query inputs for these transactions
	placeholders := make([]string, len(txids))
	args := make([]interface{}, 0, len(txids)+1)
	args = append(args, s.topic)
	for i, txid := range txids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, txid)
	}

	inputQuery := fmt.Sprintf(`SELECT outpoint, txid, script, satoshis, spend, data 
	                          FROM outputs WHERE topic = $1 AND spend IN (%s)`, strings.Join(placeholders, ","))

	inputRows, err := s.pool.Query(ctx, inputQuery, args...)
	if err != nil {
		return nil, err
	}
	defer inputRows.Close()

	// Group inputs by spending transaction
	txInputMap := make(map[chainhash.Hash][]*OutputData)
	for inputRows.Next() {
		var outpointStr, txidStr string
		var scriptBytes []byte
		var satoshis uint64
		var spendTxidStr *string
		var dataJSON *string

		err := inputRows.Scan(&outpointStr, &txidStr, &scriptBytes, &satoshis, &spendTxidStr, &dataJSON)
		if err != nil {
			return nil, err
		}

		// Parse outpoint
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			return nil, err
		}

		// Parse source txid
		sourceTxid, err := chainhash.NewHashFromHex(txidStr)
		if err != nil {
			return nil, err
		}

		// Parse data if present
		var data interface{}
		if dataJSON != nil && *dataJSON != "" {
			if err := json.Unmarshal([]byte(*dataJSON), &data); err == nil {
				// Successfully parsed JSON data
			}
		}

		// Create OutputData for input
		inputData := &OutputData{
			TxID:     sourceTxid,
			Vout:     outpoint.Index,
			Data:     data,
			Script:   scriptBytes,
			Satoshis: satoshis,
		}

		// Add to the spending transaction's inputs
		if spendTxidStr != nil {
			if spendTxid, err := chainhash.NewHashFromHex(*spendTxidStr); err == nil {
				txInputMap[*spendTxid] = append(txInputMap[*spendTxid], inputData)
			}
		}
	}

	// Build TransactionData for each transaction
	var transactions []*TransactionData
	for txid, outputs := range txOutputMap {
		txData := &TransactionData{
			TxID:    txid,
			Outputs: outputs,
			Inputs:  txInputMap[txid],
		}
		if txData.Inputs == nil {
			txData.Inputs = make([]*OutputData, 0)
		}

		transactions = append(transactions, txData)
	}

	return transactions, nil
}

// GetTransactionByTxid returns a single transaction by txid
func (s *PostgresTopicDataStorage) GetTransactionByTxid(ctx context.Context, txid *chainhash.Hash, includeBeef ...bool) (*TransactionData, error) {
	// Query outputs for the specific transaction and topic
	query := `SELECT outpoint, txid, script, satoshis, spend, data, block_height, block_idx
	         FROM outputs 
	         WHERE topic = $1 AND txid = $2
	         ORDER BY outpoint`

	rows, err := s.pool.Query(ctx, query, s.topic, txid.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []*OutputData
	var blockHeight uint32
	var blockIndex uint32

	// Process outputs
	for rows.Next() {
		var outpointStr, txidStr string
		var scriptBytes []byte
		var satoshis uint64
		var spendStr *string
		var dataJSON *string

		err := rows.Scan(&outpointStr, &txidStr, &scriptBytes, &satoshis, &spendStr, &dataJSON, &blockHeight, &blockIndex)
		if err != nil {
			return nil, err
		}

		// Parse outpoint to get vout
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			return nil, err
		}

		// Parse spend if exists
		var spend *chainhash.Hash
		if spendStr != nil && *spendStr != "" {
			if spendHash, err := chainhash.NewHashFromHex(*spendStr); err == nil {
				spend = spendHash
			}
		}

		// Parse data if exists
		var data interface{}
		if dataJSON != nil && *dataJSON != "" {
			json.Unmarshal([]byte(*dataJSON), &data)
		}

		output := &OutputData{
			Vout:     outpoint.Index,
			Data:     data,
			Script:   scriptBytes,
			Satoshis: satoshis,
			Spend:    spend,
		}

		outputs = append(outputs, output)
	}

	// If no outputs found, transaction doesn't exist in this topic
	if len(outputs) == 0 {
		return nil, fmt.Errorf("transaction not found")
	}

	// Query inputs for this transaction
	var inputs []*OutputData
	inputQuery := `SELECT outpoint, txid, script, satoshis, data 
	              FROM outputs 
	              WHERE topic = $1 AND spend = $2`

	inputRows, err := s.pool.Query(ctx, inputQuery, s.topic, txid.String())
	if err != nil {
		return nil, err
	}
	defer inputRows.Close()

	for inputRows.Next() {
		var outpointStr, txidStr string
		var script []byte
		var satoshis uint64
		var dataJSON *string

		err := inputRows.Scan(&outpointStr, &txidStr, &script, &satoshis, &dataJSON)
		if err != nil {
			return nil, err
		}

		// Parse outpoint to get vout
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			return nil, err
		}

		// Parse data if exists
		var data interface{}
		if dataJSON != nil && *dataJSON != "" {
			if err := json.Unmarshal([]byte(*dataJSON), &data); err != nil {
				return nil, err
			}
		}

		// Create OutputData for input with source txid
		sourceTxid := outpoint.Txid
		input := &OutputData{
			TxID:     &sourceTxid,
			Vout:     outpoint.Index,
			Data:     data,
			Script:   script,
			Satoshis: satoshis,
		}

		inputs = append(inputs, input)
	}

	// Build TransactionData
	txData := &TransactionData{
		TxID:        *txid,
		Outputs:     outputs,
		Inputs:      inputs,
		BlockHeight: blockHeight,
		BlockIndex:  blockIndex,
	}

	// Load BEEF if requested
	if len(includeBeef) > 0 && includeBeef[0] {
		beef, err := s.LoadBeefByTxid(ctx, txid)
		if err != nil {
			return nil, err
		} else {
			txData.Beef = beef
		}
	}

	return txData, nil
}

// LoadBeefByTxid loads merged BEEF for a transaction within a topic context
func (s *PostgresTopicDataStorage) LoadBeefByTxid(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// Find any output for this txid in the specified topic
	var ancillaryBeef []byte
	err := s.pool.QueryRow(ctx,
		"SELECT ancillary_beef FROM outputs WHERE topic = $1 AND txid = $2 LIMIT 1",
		s.topic, txid.String(),
	).Scan(&ancillaryBeef)

	if err != nil {
		return nil, fmt.Errorf("transaction %s not found in topic %s: %w", txid.String(), s.topic, err)
	}

	// Get BEEF from beef storage
	beefBytes, err := s.beefStore.LoadBeef(ctx, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to load BEEF: %w", err)
	}

	// Parse the main BEEF
	beef, _, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse main BEEF: %w", err)
	}

	// Merge AncillaryBeef if present
	if len(ancillaryBeef) > 0 {
		if err := beef.MergeBeefBytes(ancillaryBeef); err != nil {
			return nil, fmt.Errorf("failed to merge AncillaryBeef: %w", err)
		}
	}

	// Get atomic BEEF bytes for the specific transaction
	completeBeef, err := beef.AtomicBytes(txid)
	if err != nil {
		return nil, fmt.Errorf("failed to generate atomic BEEF: %w", err)
	}

	return completeBeef, nil
}

// SaveEvents associates multiple events with a single output, storing arbitrary data
func (s *PostgresTopicDataStorage) SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, score float64, data interface{}) error {
	if len(events) == 0 && data == nil {
		return nil
	}
	outpointStr := outpoint.String()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Insert events into events table
	for _, event := range events {
		_, err = tx.Exec(ctx, `
			INSERT INTO events (event, outpoint, score, topic)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (topic, event, outpoint) DO UPDATE SET
				score = EXCLUDED.score`,
			event, outpointStr, score, s.topic)
		if err != nil {
			return err
		}
	}

	// Update data in outputs table if provided
	if data != nil {
		dataJSON, err := json.Marshal(data)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
			UPDATE outputs 
			SET data = CASE 
				WHEN data IS NULL THEN $1::jsonb
				ELSE data || $1::jsonb
			END
			WHERE topic = $2 AND outpoint = $3`,
			string(dataJSON), s.topic, outpointStr)
		if err != nil {
			return err
		}
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Publish events if publisher is available
	if s.pubsub != nil {
		for _, event := range events {
			if err := s.pubsub.Publish(ctx, event, outpointStr); err != nil {
				// Log error but don't fail the operation
				continue
			}
		}
	}

	return nil
}

// FindEvents returns all events associated with a given outpoint
func (s *PostgresTopicDataStorage) FindEvents(ctx context.Context, outpoint *transaction.Outpoint) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT event FROM events WHERE topic = $1 AND outpoint = $2",
		s.topic, outpoint.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []string
	for rows.Next() {
		var event string
		if err := rows.Scan(&event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, rows.Err()
}

// LookupOutpoints returns outpoints matching the given query criteria
func (s *PostgresTopicDataStorage) LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error) {
	withData := len(includeData) > 0 && includeData[0]

	// Build the query
	var query strings.Builder
	var args []interface{}

	// Handle event filtering using the events table
	if question.Event != "" {
		// Single event query
		query.WriteString("SELECT e.outpoint, e.score")
		if withData {
			query.WriteString(", o.data")
		}
		query.WriteString(" FROM events e")
		if withData {
			query.WriteString(" LEFT JOIN outputs o ON e.topic = o.topic AND e.outpoint = o.outpoint")
		}
		query.WriteString(" WHERE e.topic = $1 AND e.event = $2")
		args = append(args, s.topic, question.Event)
	} else if len(question.Events) > 0 {
		if question.JoinType == nil || *question.JoinType == JoinTypeUnion {
			// Union: find outputs with ANY of the events
			query.WriteString("SELECT DISTINCT e.outpoint, e.score")
			if withData {
				query.WriteString(", o.data")
			}
			query.WriteString(" FROM events e")
			if withData {
				query.WriteString(" LEFT JOIN outputs o ON e.topic = o.topic AND e.outpoint = o.outpoint")
			}
			query.WriteString(" WHERE e.topic = $1 AND e.event IN (")
			args = append(args, s.topic)
			placeholders := make([]string, len(question.Events))
			for i, event := range question.Events {
				placeholders[i] = fmt.Sprintf("$%d", i+2)
				args = append(args, event)
			}
			query.WriteString(strings.Join(placeholders, ","))
			query.WriteString(")")
		} else if *question.JoinType == JoinTypeIntersect {
			// Intersection: find outputs that have ALL events
			query.WriteString("SELECT e1.outpoint, e1.score")
			if withData {
				query.WriteString(", o.data")
			}
			query.WriteString(" FROM events e1")
			for i := 1; i < len(question.Events); i++ {
				query.WriteString(fmt.Sprintf(" INNER JOIN events e%d ON e1.topic = e%d.topic AND e1.outpoint = e%d.outpoint", i+1, i+1, i+1))
			}
			if withData {
				query.WriteString(" LEFT JOIN outputs o ON e1.topic = o.topic AND e1.outpoint = o.outpoint")
			}
			query.WriteString(" WHERE e1.topic = $1 AND e1.event = $2")
			args = append(args, s.topic, question.Events[0])
			for i := 1; i < len(question.Events); i++ {
				query.WriteString(fmt.Sprintf(" AND e%d.event = $%d", i+1, i+2))
				args = append(args, question.Events[i])
			}
		} else if *question.JoinType == JoinTypeDifference {
			// Difference: first event but not others
			query.WriteString("SELECT e.outpoint, e.score")
			if withData {
				query.WriteString(", o.data")
			}
			query.WriteString(" FROM events e")
			if withData {
				query.WriteString(" LEFT JOIN outputs o ON e.topic = o.topic AND e.outpoint = o.outpoint")
			}
			query.WriteString(" WHERE e.topic = $1 AND e.event = $2 AND NOT EXISTS (SELECT 1 FROM events e2 WHERE e2.topic = e.topic AND e2.outpoint = e.outpoint AND e2.event IN (")
			args = append(args, s.topic, question.Events[0])
			placeholders := make([]string, len(question.Events)-1)
			for i := 1; i < len(question.Events); i++ {
				placeholders[i-1] = fmt.Sprintf("$%d", i+2)
				args = append(args, question.Events[i])
			}
			query.WriteString(strings.Join(placeholders, ","))
			query.WriteString("))")
		}
	} else {
		// No event filter - return all outputs
		query.WriteString("SELECT outpoint, score")
		if withData {
			query.WriteString(", data")
		}
		query.WriteString(" FROM outputs WHERE topic = $1")
		args = append(args, s.topic)
	}

	// Score filter
	if question.Event != "" || len(question.Events) > 0 {
		// Querying events table
		if question.Reverse {
			query.WriteString(" AND e.score < $" + fmt.Sprintf("%d", len(args)+1))
		} else {
			query.WriteString(" AND e.score > $" + fmt.Sprintf("%d", len(args)+1))
		}
	} else {
		// Querying outputs table
		if question.Reverse {
			query.WriteString(" AND score < $" + fmt.Sprintf("%d", len(args)+1))
		} else {
			query.WriteString(" AND score > $" + fmt.Sprintf("%d", len(args)+1))
		}
	}
	args = append(args, question.From)

	// Until filter
	if question.Until > 0 {
		if question.Event != "" || len(question.Events) > 0 {
			// Querying events table
			if question.Reverse {
				query.WriteString(" AND e.score > $" + fmt.Sprintf("%d", len(args)+1))
			} else {
				query.WriteString(" AND e.score < $" + fmt.Sprintf("%d", len(args)+1))
			}
		} else {
			// Querying outputs table
			if question.Reverse {
				query.WriteString(" AND score > $" + fmt.Sprintf("%d", len(args)+1))
			} else {
				query.WriteString(" AND score < $" + fmt.Sprintf("%d", len(args)+1))
			}
		}
		args = append(args, question.Until)
	}

	// Unspent filter
	if question.UnspentOnly {
		if question.Event != "" || len(question.Events) > 0 {
			// Querying events table - use its spend column
			query.WriteString(" AND (e.spend IS NULL OR e.spend = '')")
		} else {
			// Querying outputs directly
			query.WriteString(" AND (spend IS NULL OR spend = '')")
		}
	}

	// Sort order
	if question.Reverse {
		if question.Event != "" || len(question.Events) > 0 {
			query.WriteString(" ORDER BY e.score DESC")
		} else {
			query.WriteString(" ORDER BY score DESC")
		}
	} else {
		if question.Event != "" || len(question.Events) > 0 {
			query.WriteString(" ORDER BY e.score ASC")
		} else {
			query.WriteString(" ORDER BY score ASC")
		}
	}

	// Limit
	if question.Limit > 0 {
		query.WriteString(" LIMIT $" + fmt.Sprintf("%d", len(args)+1))
		args = append(args, question.Limit)
	}

	// Execute query
	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*OutpointResult
	for rows.Next() {
		var outpointStr string
		var score float64
		var dataJSON *string

		if withData {
			err = rows.Scan(&outpointStr, &score, &dataJSON)
		} else {
			err = rows.Scan(&outpointStr, &score)
		}
		if err != nil {
			return nil, err
		}

		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			return nil, err
		}

		result := &OutpointResult{
			Outpoint: outpoint,
			Score:    score,
		}

		// Include data if requested and available
		if withData && dataJSON != nil && *dataJSON != "" {
			var data interface{}
			if err := json.Unmarshal([]byte(*dataJSON), &data); err == nil {
				result.Data = data
			}
		}

		results = append(results, result)
	}

	return results, rows.Err()
}

// GetOutputData retrieves the data associated with a specific output
func (s *PostgresTopicDataStorage) GetOutputData(ctx context.Context, outpoint *transaction.Outpoint) (interface{}, error) {
	var dataJSON *string
	err := s.pool.QueryRow(ctx,
		"SELECT data FROM outputs WHERE topic = $1 AND outpoint = $2 LIMIT 1",
		s.topic, outpoint.String()).Scan(&dataJSON)

	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("outpoint not found")
	}
	if err != nil {
		return nil, err
	}

	if dataJSON == nil || *dataJSON == "" {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(*dataJSON), &data); err != nil {
		return nil, err
	}

	return data, nil
}

// FindOutputData returns outputs matching the given query criteria as OutputData objects
func (s *PostgresTopicDataStorage) FindOutputData(ctx context.Context, question *EventQuestion) ([]*OutputData, error) {
	var query strings.Builder
	var args []interface{}

	// Base query selecting OutputData fields
	query.WriteString(`
		SELECT DISTINCT o.outpoint, o.script, o.satoshis, o.data, o.spend, o.score
		FROM outputs o
	`)

	// Add event filtering if needed
	if question.Event != "" || len(question.Events) > 0 {
		query.WriteString(" JOIN events oe ON o.topic = oe.topic AND o.outpoint = oe.outpoint")
	}

	query.WriteString(" WHERE o.topic = $1")
	args = append(args, s.topic)

	// Add event filters
	if question.Event != "" {
		query.WriteString(" AND oe.event = $2")
		args = append(args, question.Event)
	} else if len(question.Events) > 0 {
		placeholders := make([]string, len(question.Events))
		for i, event := range question.Events {
			placeholders[i] = fmt.Sprintf("$%d", len(args)+1)
			args = append(args, event)
		}

		if question.JoinType != nil && *question.JoinType == JoinTypeIntersect {
			// For intersection, we need all events to be present
			query.WriteString(fmt.Sprintf(" AND oe.event IN (%s) GROUP BY o.outpoint, o.script, o.satoshis, o.data, o.spend, o.score HAVING COUNT(DISTINCT oe.event) = %d",
				strings.Join(placeholders, ","), len(question.Events)))
		} else {
			// Default to union
			query.WriteString(fmt.Sprintf(" AND oe.event IN (%s)", strings.Join(placeholders, ",")))
		}
	}

	// Add unspent filter if needed
	if question.UnspentOnly {
		query.WriteString(" AND o.spend IS NULL")
	}

	// Add score range filtering
	if question.From > 0 {
		if question.Reverse {
			query.WriteString(" AND o.score <= $" + fmt.Sprintf("%d", len(args)+1))
		} else {
			query.WriteString(" AND o.score >= $" + fmt.Sprintf("%d", len(args)+1))
		}
		args = append(args, question.From)
	}

	if question.Until > 0 {
		if question.Reverse {
			query.WriteString(" AND o.score >= $" + fmt.Sprintf("%d", len(args)+1))
		} else {
			query.WriteString(" AND o.score <= $" + fmt.Sprintf("%d", len(args)+1))
		}
		args = append(args, question.Until)
	}

	// Add ordering
	if question.Reverse {
		query.WriteString(" ORDER BY o.score DESC")
	} else {
		query.WriteString(" ORDER BY o.score ASC")
	}

	// Add limit
	if question.Limit > 0 {
		query.WriteString(" LIMIT $" + fmt.Sprintf("%d", len(args)+1))
		args = append(args, question.Limit)
	}

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*OutputData
	for rows.Next() {
		var outpointStr string
		var script []byte
		var satoshis uint64
		var dataJSON *string
		var spendingTxidStr *string
		var score float64

		if err := rows.Scan(&outpointStr, &script, &satoshis, &dataJSON, &spendingTxidStr, &score); err != nil {
			return nil, err
		}

		// Parse outpoint to get txid
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			continue // Skip invalid outpoints
		}

		// Parse spending txid if present
		var spendTxid *chainhash.Hash
		if spendingTxidStr != nil && *spendingTxidStr != "" {
			if parsedSpendTxid, err := chainhash.NewHashFromHex(*spendingTxidStr); err == nil {
				spendTxid = parsedSpendTxid
			}
		}

		result := &OutputData{
			TxID:     &outpoint.Txid,
			Vout:     outpoint.Index,
			Script:   script,
			Satoshis: satoshis,
			Spend:    spendTxid,
			Score:    score,
		}

		// Parse data if present
		if dataJSON != nil && *dataJSON != "" {
			var data interface{}
			if err := json.Unmarshal([]byte(*dataJSON), &data); err == nil {
				result.Data = data
			}
		}

		results = append(results, result)
	}

	return results, rows.Err()
}

// LookupEventScores returns lightweight event scores for simple queries
func (s *PostgresTopicDataStorage) LookupEventScores(ctx context.Context, event string, fromScore float64) ([]queue.ScoredMember, error) {
	// Query the events table directly
	rows, err := s.pool.Query(ctx, `
		SELECT outpoint, score 
		FROM events 
		WHERE topic = $1 AND event = $2 AND score > $3 
		ORDER BY score ASC`,
		s.topic, event, fromScore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []queue.ScoredMember
	for rows.Next() {
		var outpointStr string
		var score float64
		if err := rows.Scan(&outpointStr, &score); err != nil {
			return nil, err
		}
		members = append(members, queue.ScoredMember{
			Member: outpointStr,
			Score:  score,
		})
	}

	return members, rows.Err()
}

// CountOutputs returns the total count of outputs in a given topic
func (s *PostgresTopicDataStorage) CountOutputs(ctx context.Context) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, "SELECT COUNT(1) FROM outputs WHERE topic = $1", s.topic).Scan(&count)
	return count, err
}
