package storage

import (
	"context"
	"database/sql"
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
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
	_ "github.com/go-sql-driver/mysql"
)

// MySQLSharedPool manages a single connection pool shared across all topics
type MySQLSharedPool struct {
	db          *sql.DB
	initialized bool
	refCount    int
	mutex       sync.RWMutex
}

// Global shared pool instance
var mysqlSharedPool = &MySQLSharedPool{}

// MySQLTopicDataStorage implements TopicDataStorage using MySQL with shared connection pool
type MySQLTopicDataStorage struct {
	BaseEventDataStorage
	db    *sql.DB
	topic string
}

// NewMySQLTopicDataStorage creates a new MySQL topic storage using shared connection pool
func NewMySQLTopicDataStorage(topic string, connectionString string, beefStore *beef.Storage, queueStorage queue.QueueStorage, pubsub pubsub.PubSub, chainTracker chaintracker.ChainTracker) (TopicDataStorage, error) {
	// Get or create shared connection pool
	db, err := getMySQLSharedPool(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to get shared pool: %w", err)
	}

	storage := &MySQLTopicDataStorage{
		BaseEventDataStorage: NewBaseEventDataStorage(beefStore, queueStorage, pubsub, chainTracker),
		db:                   db,
		topic:                topic,
	}

	// Ensure tables exist (idempotent)
	if err := storage.createTables(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return storage, nil
}

// getMySQLSharedPool returns the shared connection pool, creating it if necessary
func getMySQLSharedPool(connectionString string) (*sql.DB, error) {
	mysqlSharedPool.mutex.Lock()
	defer mysqlSharedPool.mutex.Unlock()

	if mysqlSharedPool.initialized {
		mysqlSharedPool.refCount++
		return mysqlSharedPool.db, nil
	}

	// Open database connection
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// Configure connection pool settings
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(1 * time.Hour)
	db.SetConnMaxIdleTime(30 * time.Minute)

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	mysqlSharedPool.db = db
	mysqlSharedPool.initialized = true
	mysqlSharedPool.refCount = 1

	return db, nil
}

// GetTopic returns the topic this storage handles
func (s *MySQLTopicDataStorage) GetTopic() string {
	return s.topic
}

// createTables creates all required tables with proper indexes (idempotent)
func (s *MySQLTopicDataStorage) createTables(ctx context.Context) error {
	tables := []string{
		// Outputs table
		`CREATE TABLE IF NOT EXISTS outputs (
			outpoint VARCHAR(255) NOT NULL,
			txid VARCHAR(64) NOT NULL,
			script LONGBLOB NOT NULL,
			satoshis BIGINT UNSIGNED NOT NULL,
			spend VARCHAR(64),
			block_height INT UNSIGNED,
			block_idx INT UNSIGNED,
			score DOUBLE NOT NULL,
			ancillary_beef LONGBLOB,
			data JSON,
			topic VARCHAR(255) NOT NULL,
			merkle_root VARCHAR(64),
			merkle_validation_state SMALLINT DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (topic, outpoint),
			INDEX idx_outputs_outpoint (outpoint),
			INDEX idx_outputs_txid (txid),
			INDEX idx_outputs_score (score),
			INDEX idx_outputs_spend_score (spend, score),
			INDEX idx_outputs_block_height (block_height),
			INDEX idx_outputs_merkle_validation_state (merkle_validation_state),
			INDEX idx_outputs_topic_score (topic, score)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`,

		// Events table
		`CREATE TABLE IF NOT EXISTS events (
			event VARCHAR(255) NOT NULL,
			outpoint VARCHAR(255) NOT NULL,
			score DOUBLE NOT NULL,
			spend VARCHAR(64),
			topic VARCHAR(255) NOT NULL,
			PRIMARY KEY (topic, event, outpoint),
			INDEX idx_events_event_score_spend (event, score, spend),
			INDEX idx_events_outpoint (outpoint),
			INDEX idx_events_topic_score (topic, score)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`,

		// Transactions table
		`CREATE TABLE IF NOT EXISTS transactions (
			txid VARCHAR(64) NOT NULL,
			score DOUBLE,
			topic VARCHAR(255) NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (topic, txid),
			INDEX idx_transactions_txid (txid),
			INDEX idx_transactions_score (score)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`,

		// Output relationships table
		`CREATE TABLE IF NOT EXISTS output_relationships (
			consuming_outpoint VARCHAR(255) NOT NULL,
			consumed_outpoint VARCHAR(255) NOT NULL,
			topic VARCHAR(255) NOT NULL,
			PRIMARY KEY (topic, consuming_outpoint, consumed_outpoint),
			INDEX idx_relationships_consuming (consuming_outpoint),
			INDEX idx_relationships_consumed (consumed_outpoint)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`,
	}

	// Create tables
	for _, query := range tables {
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

// InsertOutput inserts a new output into the database
func (s *MySQLTopicDataStorage) InsertOutput(ctx context.Context, utxo *engine.Output) error {
	// Calculate score
	utxo.Score = float64(time.Now().UnixNano())

	// Save BEEF to storage first
	if err := s.beefStore.SaveBeef(ctx, &utxo.Outpoint.Txid, utxo.Beef); err != nil {
		return err
	}

	// Parse BEEF to extract script and satoshis
	_, tx, _, err := transaction.ParseBeef(utxo.Beef)
	if err != nil {
		return fmt.Errorf("failed to parse BEEF: %w", err)
	}
	if tx == nil {
		return fmt.Errorf("no transaction in BEEF")
	}
	if int(utxo.Outpoint.Index) >= len(tx.Outputs) {
		return fmt.Errorf("output index %d out of range", utxo.Outpoint.Index)
	}
	txOutput := tx.Outputs[utxo.Outpoint.Index]

	// Extract merkle information from BEEF
	blockHeight, blockIndex, merkleRoot, validationState, err := s.ExtractMerkleInfoFromBEEF(ctx, &utxo.Outpoint.Txid, utxo.Beef)
	if err != nil {
		return fmt.Errorf("failed to extract merkle info: %w", err)
	}

	// Always use the block height and index from BEEF extraction (0 if no merkle path)
	utxo.BlockHeight = blockHeight
	utxo.BlockIdx = blockIndex

	// Prepare merkle root string
	merkleRootStr := sql.NullString{}
	if merkleRoot != nil {
		merkleRootStr.Valid = true
		merkleRootStr.String = merkleRoot.String()
	}

	// Insert output with merkle info
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO outputs (outpoint, txid, script, satoshis, block_height, block_idx, score, ancillary_beef,
			merkle_root, merkle_validation_state, topic)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			script = VALUES(script),
			satoshis = VALUES(satoshis),
			block_height = VALUES(block_height),
			block_idx = VALUES(block_idx),
			score = VALUES(score),
			ancillary_beef = VALUES(ancillary_beef),
			merkle_root = VALUES(merkle_root),
			merkle_validation_state = VALUES(merkle_validation_state)`,
		utxo.Outpoint.String(),
		utxo.Outpoint.Txid.String(),
		txOutput.LockingScript.Bytes(),
		txOutput.Satoshis,
		utxo.BlockHeight,
		utxo.BlockIdx,
		utxo.Score,
		nil, // ancillary_beef no longer on Output struct
		merkleRootStr,
		validationState,
		s.topic,
	)
	if err != nil {
		return err
	}

	// Batch insert output relationships
	var relationships [][]interface{}

	// Collect all relationships
	for _, consumed := range utxo.OutputsConsumed {
		relationships = append(relationships, []interface{}{
			utxo.Outpoint.String(),
			consumed.String(),
			s.topic,
		})
	}

	for _, consumedBy := range utxo.ConsumedBy {
		relationships = append(relationships, []interface{}{
			consumedBy.String(),
			utxo.Outpoint.String(),
			s.topic,
		})
	}

	// Batch insert all relationships
	if len(relationships) > 0 {
		valueStrings := make([]string, len(relationships))
		valueArgs := make([]interface{}, 0, len(relationships)*3)

		for i, rel := range relationships {
			valueStrings[i] = "(?, ?, ?)"
			valueArgs = append(valueArgs, rel...)
		}

		query := fmt.Sprintf(`
			INSERT IGNORE INTO output_relationships (consuming_outpoint, consumed_outpoint, topic)
			VALUES %s`,
			strings.Join(valueStrings, ","))

		_, err = s.db.ExecContext(ctx, query, valueArgs...)
		if err != nil {
			return err
		}
	}

	return nil
}

// FindOutput finds a single output by outpoint
func (s *MySQLTopicDataStorage) FindOutput(ctx context.Context, outpoint *transaction.Outpoint, spent *bool, includeBEEF bool) (*engine.Output, error) {
	query := `SELECT outpoint, txid, script, satoshis, spend, block_height, block_idx, score, ancillary_beef
		FROM outputs WHERE topic = ? AND outpoint = ?`
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
	var satoshis uint64
	var ancillaryBeef []byte
	var spendTxid sql.NullString

	err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&outpointStr,
		&txidStr,
		&scriptBytes,
		&satoshis,
		&spendTxid,
		&output.BlockHeight,
		&output.BlockIdx,
		&output.Score,
		&ancillaryBeef,
	)
	if err == sql.ErrNoRows {
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

	// Set spent status
	output.Spent = spendTxid.Valid

	// Note: scriptBytes, satoshis, and ancillaryBeef are read from DB but not stored
	// on Output struct (those fields no longer exist). Data is in BEEF.
	_ = scriptBytes
	_ = satoshis
	_ = ancillaryBeef

	// Load BEEF if requested
	if includeBEEF {
		beef, err := s.beefStore.LoadBeef(ctx, &output.Outpoint.Txid, nil)
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
func (s *MySQLTopicDataStorage) FindOutputs(ctx context.Context, outpoints []*transaction.Outpoint, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
	if len(outpoints) == 0 {
		return nil, nil
	}

	// Build query with placeholders
	placeholders := make([]string, len(outpoints))
	args := make([]interface{}, 0, len(outpoints)+1)
	args = append(args, s.topic)

	for i, op := range outpoints {
		placeholders[i] = "?"
		args = append(args, op.String())
	}

	query := fmt.Sprintf(`SELECT outpoint, txid, script, satoshis, spend, block_height, block_idx, score, ancillary_beef
		FROM outputs WHERE topic = ? AND outpoint IN (%s)`, strings.Join(placeholders, ","))

	if spent != nil {
		if *spent {
			query += " AND spend IS NOT NULL"
		} else {
			query += " AND spend IS NULL"
		}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
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
func (s *MySQLTopicDataStorage) scanOutput(rows *sql.Rows, includeBEEF bool) (*engine.Output, error) {
	var output engine.Output
	var outpointStr, txidStr string
	var scriptBytes []byte
	var satoshis uint64
	var ancillaryBeef []byte
	var spendTxid sql.NullString

	err := rows.Scan(
		&outpointStr,
		&txidStr,
		&scriptBytes,
		&satoshis,
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

	// Set spent status
	output.Spent = spendTxid.Valid

	// Note: scriptBytes, satoshis, and ancillaryBeef are read from DB but not stored
	// on Output struct (those fields no longer exist). Data is in BEEF.
	_ = scriptBytes
	_ = satoshis
	_ = ancillaryBeef

	// Load BEEF if requested
	if includeBEEF {
		beef, err := s.beefStore.LoadBeef(context.Background(), &output.Outpoint.Txid, nil)
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
func (s *MySQLTopicDataStorage) loadOutputRelations(ctx context.Context, output *engine.Output) error {
	// Load outputs consumed by this output
	rows, err := s.db.QueryContext(ctx,
		"SELECT consumed_outpoint FROM output_relationships WHERE topic = ? AND consuming_outpoint = ?",
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
	rows2, err := s.db.QueryContext(ctx,
		"SELECT consuming_outpoint FROM output_relationships WHERE topic = ? AND consumed_outpoint = ?",
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
func (s *MySQLTopicDataStorage) Close() error {
	mysqlSharedPool.mutex.Lock()
	defer mysqlSharedPool.mutex.Unlock()

	if mysqlSharedPool.initialized {
		mysqlSharedPool.refCount--
		if mysqlSharedPool.refCount == 0 {
			// No more references, close the shared pool
			mysqlSharedPool.db.Close()
			mysqlSharedPool.db = nil
			mysqlSharedPool.initialized = false
		}
	}
	return nil
}

// FindOutputsForTransaction finds all outputs for a given transaction
func (s *MySQLTopicDataStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
	query := `SELECT outpoint, txid, script, satoshis, spend, block_height, block_idx, score, ancillary_beef
		FROM outputs WHERE topic = ? AND txid = ?`

	rows, err := s.db.QueryContext(ctx, query, s.topic, txid.String())
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
func (s *MySQLTopicDataStorage) FindUTXOsForTopic(ctx context.Context, since float64, limit uint32, includeBEEF bool) ([]*engine.Output, error) {
	query := `SELECT outpoint, txid, script, satoshis, spend, block_height, block_idx, score, ancillary_beef
		FROM outputs
		WHERE topic = ? AND score >= ?
		ORDER BY score`

	args := []interface{}{s.topic, since}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
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
func (s *MySQLTopicDataStorage) DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM outputs WHERE topic = ? AND outpoint = ?",
		s.topic, outpoint.String())
	return err
}

// MarkUTXOAsSpent marks a UTXO as spent
func (s *MySQLTopicDataStorage) MarkUTXOAsSpent(ctx context.Context, outpoint *transaction.Outpoint, beef []byte) error {
	// Parse the beef to get the spending txid
	_, _, spendTxid, err := transaction.ParseBeef(beef)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx,
		"UPDATE outputs SET spend = ? WHERE topic = ? AND outpoint = ?",
		spendTxid.String(), s.topic, outpoint.String())
	return err
}

// MarkUTXOsAsSpent marks multiple UTXOs as spent
func (s *MySQLTopicDataStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*transaction.Outpoint, spendTxid *chainhash.Hash) error {
	if len(outpoints) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	spendTxidStr := spendTxid.String()

	for _, op := range outpoints {
		opStr := op.String()

		// Update outputs table
		_, err = tx.ExecContext(ctx,
			"UPDATE outputs SET spend = ? WHERE topic = ? AND outpoint = ?",
			spendTxidStr, s.topic, opStr)
		if err != nil {
			return err
		}

		// Update events table for all events associated with this outpoint
		_, err = tx.ExecContext(ctx,
			"UPDATE events SET spend = ? WHERE topic = ? AND outpoint = ?",
			spendTxidStr, s.topic, opStr)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// UpdateConsumedBy updates consumed-by relationships (no-op as managed atomically)
func (s *MySQLTopicDataStorage) UpdateConsumedBy(ctx context.Context, outpoint *transaction.Outpoint, consumedBy []*transaction.Outpoint) error {
	// No-op: Output relationships are managed by InsertOutput method atomically.
	return nil
}

// UpdateTransactionBEEF updates BEEF data for a transaction
func (s *MySQLTopicDataStorage) UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error {
	// Save BEEF to storage
	if err := s.beefStore.SaveBeef(ctx, txid, beef); err != nil {
		return err
	}

	// Extract merkle information from BEEF
	blockHeight, blockIndex, merkleRoot, validationState, err := s.ExtractMerkleInfoFromBEEF(ctx, txid, beef)
	if err != nil {
		return fmt.Errorf("failed to extract merkle info: %w", err)
	}

	if merkleRoot == nil {
		// No merkle path in this BEEF, nothing more to do
		return nil
	}

	// Update all outputs for this transaction with the merkle root, validation state, block height AND index
	_, err = s.db.ExecContext(ctx, `
		UPDATE outputs
		SET merkle_root = ?,
		    merkle_validation_state = ?,
		    block_height = ?,
		    block_idx = ?
		WHERE topic = ? AND txid = ?`,
		merkleRoot.String(),
		validationState,
		blockHeight,
		blockIndex,
		s.topic,
		txid.String())

	if err != nil {
		return fmt.Errorf("failed to update outputs with merkle info: %w", err)
	}

	return nil
}

// UpdateOutputBlockHeight updates block height information for an output
func (s *MySQLTopicDataStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *transaction.Outpoint, blockHeight uint32, blockIndex uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		UPDATE outputs
		SET block_height = ?, block_idx = ?, score = ?
		WHERE topic = ? AND outpoint = ?`,
		blockHeight,
		blockIndex,
		float64(time.Now().UnixNano()),
		s.topic,
		outpoint.String(),
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// InsertAppliedTransaction inserts an applied transaction record
func (s *MySQLTopicDataStorage) InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error {
	score := float64(time.Now().UnixNano())

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO transactions (topic, txid, score, created_at)
		VALUES (?, ?, ?, NOW())
		ON DUPLICATE KEY UPDATE
			score = VALUES(score),
			created_at = VALUES(created_at)`,
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
func (s *MySQLTopicDataStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM transactions WHERE topic = ? AND txid = ?)",
		s.topic, tx.Txid.String()).Scan(&exists)
	return exists == 1, err
}

// GetTransactionsByHeight returns all transactions for a topic at a specific block height
func (s *MySQLTopicDataStorage) GetTransactionsByHeight(ctx context.Context, height uint32) ([]*TransactionData, error) {
	// Query outputs for the topic and block height
	query := `SELECT outpoint, txid, script, satoshis, spend, data
	         FROM outputs
	         WHERE topic = ? AND block_height = ?
	         ORDER BY txid, outpoint`

	rows, err := s.db.QueryContext(ctx, query, s.topic, height)
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
		var spendTxid sql.NullString
		var dataJSON sql.NullString

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
		if dataJSON.Valid && dataJSON.String != "" {
			if err := json.Unmarshal([]byte(dataJSON.String), &data); err == nil {
				// Successfully parsed JSON data
			}
		}

		// Create OutputData
		outputData := &OutputData{
			Vout: outpoint.Index,
			Data: data,
		}

		// Note: scriptBytes and satoshis are read from DB but not stored on OutputData
		// (those fields no longer exist)
		_ = scriptBytes
		_ = satoshis

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
		placeholders[i] = "?"
		args = append(args, txid)
	}

	inputQuery := fmt.Sprintf(`SELECT outpoint, txid, script, satoshis, spend, data
	                          FROM outputs WHERE topic = ? AND spend IN (%s)`, strings.Join(placeholders, ","))

	inputRows, err := s.db.QueryContext(ctx, inputQuery, args...)
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
		var spendTxidStr sql.NullString
		var dataJSON sql.NullString

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
		if dataJSON.Valid && dataJSON.String != "" {
			if err := json.Unmarshal([]byte(dataJSON.String), &data); err == nil {
				// Successfully parsed JSON data
			}
		}

		// Create OutputData for input
		inputData := &OutputData{
			TxID: sourceTxid,
			Vout: outpoint.Index,
			Data: data,
		}

		// Note: scriptBytes and satoshis are read from DB but not stored on OutputData
		_ = scriptBytes
		_ = satoshis

		// Add to the spending transaction's inputs
		if spendTxidStr.Valid {
			if spendTxid, err := chainhash.NewHashFromHex(spendTxidStr.String); err == nil {
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
func (s *MySQLTopicDataStorage) GetTransactionByTxid(ctx context.Context, txid *chainhash.Hash, includeBeef ...bool) (*TransactionData, error) {
	// Query outputs for the specific transaction and topic
	query := `SELECT outpoint, txid, script, satoshis, spend, data, block_height, block_idx
	         FROM outputs
	         WHERE topic = ? AND txid = ?
	         ORDER BY outpoint`

	rows, err := s.db.QueryContext(ctx, query, s.topic, txid.String())
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
		var spendStr sql.NullString
		var dataJSON sql.NullString

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
		if spendStr.Valid && spendStr.String != "" {
			if spendHash, err := chainhash.NewHashFromHex(spendStr.String); err == nil {
				spend = spendHash
			}
		}

		// Parse data if exists
		var data interface{}
		if dataJSON.Valid && dataJSON.String != "" {
			json.Unmarshal([]byte(dataJSON.String), &data)
		}

		output := &OutputData{
			Vout:  outpoint.Index,
			Data:  data,
			Spend: spend,
		}

		// Note: scriptBytes and satoshis are read from DB but not stored on OutputData
		_ = scriptBytes
		_ = satoshis

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
	              WHERE topic = ? AND spend = ?`

	inputRows, err := s.db.QueryContext(ctx, inputQuery, s.topic, txid.String())
	if err != nil {
		return nil, err
	}
	defer inputRows.Close()

	for inputRows.Next() {
		var outpointStr, txidStr string
		var script []byte
		var satoshis uint64
		var dataJSON sql.NullString

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
		if dataJSON.Valid && dataJSON.String != "" {
			if err := json.Unmarshal([]byte(dataJSON.String), &data); err != nil {
				return nil, err
			}
		}

		// Create OutputData for input with source txid
		sourceTxid := outpoint.Txid
		input := &OutputData{
			TxID: &sourceTxid,
			Vout: outpoint.Index,
			Data: data,
		}

		// Note: script and satoshis are read from DB but not stored on OutputData
		_ = script
		_ = satoshis

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
func (s *MySQLTopicDataStorage) LoadBeefByTxid(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// Find any output for this txid in the specified topic
	var ancillaryBeef []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT ancillary_beef FROM outputs WHERE topic = ? AND txid = ? LIMIT 1",
		s.topic, txid.String(),
	).Scan(&ancillaryBeef)

	if err != nil {
		return nil, fmt.Errorf("transaction %s not found in topic %s: %w", txid.String(), s.topic, err)
	}

	// Get BEEF from beef storage
	beefBytes, err := s.beefStore.LoadBeef(ctx, txid, nil)
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
func (s *MySQLTopicDataStorage) SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, score float64, data interface{}) error {
	if len(events) == 0 && data == nil {
		return nil
	}
	outpointStr := outpoint.String()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert events into events table
	for _, event := range events {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO events (event, outpoint, score, topic)
			VALUES (?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				score = VALUES(score)`,
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

		_, err = tx.ExecContext(ctx, `
			UPDATE outputs
			SET data = IF(data IS NULL, ?, JSON_MERGE_PATCH(data, ?))
			WHERE topic = ? AND outpoint = ?`,
			string(dataJSON), string(dataJSON), s.topic, outpointStr)
		if err != nil {
			return err
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
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
func (s *MySQLTopicDataStorage) FindEvents(ctx context.Context, outpoint *transaction.Outpoint) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT event FROM events WHERE topic = ? AND outpoint = ?",
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
func (s *MySQLTopicDataStorage) LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error) {
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
		query.WriteString(" WHERE e.topic = ? AND e.event = ?")
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
			query.WriteString(" WHERE e.topic = ? AND e.event IN (")
			args = append(args, s.topic)
			placeholders := make([]string, len(question.Events))
			for i, event := range question.Events {
				placeholders[i] = "?"
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
			query.WriteString(" WHERE e1.topic = ? AND e1.event = ?")
			args = append(args, s.topic, question.Events[0])
			for i := 1; i < len(question.Events); i++ {
				query.WriteString(fmt.Sprintf(" AND e%d.event = ?", i+1))
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
			query.WriteString(" WHERE e.topic = ? AND e.event = ? AND NOT EXISTS (SELECT 1 FROM events e2 WHERE e2.topic = e.topic AND e2.outpoint = e.outpoint AND e2.event IN (")
			args = append(args, s.topic, question.Events[0])
			placeholders := make([]string, len(question.Events)-1)
			for i := 1; i < len(question.Events); i++ {
				placeholders[i-1] = "?"
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
		query.WriteString(" FROM outputs WHERE topic = ?")
		args = append(args, s.topic)
	}

	// Score filter
	if question.Event != "" || len(question.Events) > 0 {
		// Querying events table
		if question.Reverse {
			query.WriteString(" AND e.score < ?")
		} else {
			query.WriteString(" AND e.score > ?")
		}
	} else {
		// Querying outputs table
		if question.Reverse {
			query.WriteString(" AND score < ?")
		} else {
			query.WriteString(" AND score > ?")
		}
	}
	args = append(args, question.From)

	// Until filter
	if question.Until > 0 {
		if question.Event != "" || len(question.Events) > 0 {
			// Querying events table
			if question.Reverse {
				query.WriteString(" AND e.score > ?")
			} else {
				query.WriteString(" AND e.score < ?")
			}
		} else {
			// Querying outputs table
			if question.Reverse {
				query.WriteString(" AND score > ?")
			} else {
				query.WriteString(" AND score < ?")
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
		query.WriteString(" LIMIT ?")
		args = append(args, question.Limit)
	}

	// Execute query
	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*OutpointResult
	for rows.Next() {
		var outpointStr string
		var score float64
		var dataJSON sql.NullString

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
		if withData && dataJSON.Valid && dataJSON.String != "" {
			var data interface{}
			if err := json.Unmarshal([]byte(dataJSON.String), &data); err == nil {
				result.Data = data
			}
		}

		results = append(results, result)
	}

	return results, rows.Err()
}

// GetOutputData retrieves the data associated with a specific output
func (s *MySQLTopicDataStorage) GetOutputData(ctx context.Context, outpoint *transaction.Outpoint) (interface{}, error) {
	var dataJSON sql.NullString
	err := s.db.QueryRowContext(ctx,
		"SELECT data FROM outputs WHERE topic = ? AND outpoint = ? LIMIT 1",
		s.topic, outpoint.String()).Scan(&dataJSON)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("outpoint not found")
	}
	if err != nil {
		return nil, err
	}

	if !dataJSON.Valid || dataJSON.String == "" {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(dataJSON.String), &data); err != nil {
		return nil, err
	}

	return data, nil
}

// FindOutputData returns outputs matching the given query criteria as OutputData objects
func (s *MySQLTopicDataStorage) FindOutputData(ctx context.Context, question *EventQuestion) ([]*OutputData, error) {
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

	query.WriteString(" WHERE o.topic = ?")
	args = append(args, s.topic)

	// Add event filters
	if question.Event != "" {
		query.WriteString(" AND oe.event = ?")
		args = append(args, question.Event)
	} else if len(question.Events) > 0 {
		placeholders := make([]string, len(question.Events))
		for i, event := range question.Events {
			placeholders[i] = "?"
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
			query.WriteString(" AND o.score <= ?")
		} else {
			query.WriteString(" AND o.score >= ?")
		}
		args = append(args, question.From)
	}

	if question.Until > 0 {
		if question.Reverse {
			query.WriteString(" AND o.score >= ?")
		} else {
			query.WriteString(" AND o.score <= ?")
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
		query.WriteString(" LIMIT ?")
		args = append(args, question.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*OutputData
	for rows.Next() {
		var outpointStr string
		var script []byte
		var satoshis uint64
		var dataJSON sql.NullString
		var spendingTxidStr sql.NullString
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
		if spendingTxidStr.Valid && spendingTxidStr.String != "" {
			if parsedSpendTxid, err := chainhash.NewHashFromHex(spendingTxidStr.String); err == nil {
				spendTxid = parsedSpendTxid
			}
		}

		result := &OutputData{
			TxID:  &outpoint.Txid,
			Vout:  outpoint.Index,
			Spend: spendTxid,
			Score: score,
		}

		// Note: script and satoshis are read from DB but not stored on OutputData
		_ = script
		_ = satoshis

		// Parse data if present
		if dataJSON.Valid && dataJSON.String != "" {
			var data interface{}
			if err := json.Unmarshal([]byte(dataJSON.String), &data); err == nil {
				result.Data = data
			}
		}

		results = append(results, result)
	}

	return results, rows.Err()
}

// LookupEventScores returns lightweight event scores for simple queries
func (s *MySQLTopicDataStorage) LookupEventScores(ctx context.Context, event string, fromScore float64) ([]queue.ScoredMember, error) {
	// Query the events table directly
	rows, err := s.db.QueryContext(ctx, `
		SELECT outpoint, score
		FROM events
		WHERE topic = ? AND event = ? AND score > ?
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
func (s *MySQLTopicDataStorage) CountOutputs(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM outputs WHERE topic = ?", s.topic).Scan(&count)
	return count, err
}

// FindOutputsByMerkleState finds outputs by their merkle validation state
func (s *MySQLTopicDataStorage) FindOutpointsByMerkleState(ctx context.Context, state engine.MerkleState, limit uint32) ([]*transaction.Outpoint, error) {
	query := `
		SELECT outpoint
		FROM outputs
		WHERE topic = ? AND merkle_validation_state = ?
		ORDER BY score DESC`

	args := []interface{}{s.topic, state}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outpoints []*transaction.Outpoint
	for rows.Next() {
		var outpointStr string

		err := rows.Scan(&outpointStr)
		if err != nil {
			return nil, err
		}

		// Parse outpoint
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			return nil, err
		}
		outpoints = append(outpoints, outpoint)
	}

	return outpoints, rows.Err()
}

// ReconcileValidatedMerkleRoots finds all Validated outputs and reconciles those that need updating
// This includes promoting to Immutable at sufficient depth or Invalidating if merkle root changed
// Assumes merkle roots cache is already current via SyncMerkleRoots
func (s *MySQLTopicDataStorage) ReconcileValidatedMerkleRoots(ctx context.Context) error {
	if s.chainTracker == nil {
		return fmt.Errorf("chain tracker not configured")
	}

	// Get current chain tip for immutability calculations
	currentHeight, err := s.chainTracker.CurrentHeight(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current height: %w", err)
	}

	// Find all unique block heights with Validated outputs
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT block_height, merkle_root
		FROM outputs
		WHERE topic = ? AND merkle_validation_state = ?
		ORDER BY block_height`,
		s.topic, engine.MerkleStateValidated)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var height uint32
		var storedRootStr sql.NullString

		if err := rows.Scan(&height, &storedRootStr); err != nil {
			return err
		}

		// Check if we need to reconcile:
		// 1. Height has reached immutable depth, OR
		// 2. Merkle root has changed (indicating reorg)
		needsReconcile := currentHeight-height >= IMMUTABILITY_DEPTH

		// Check if merkle root has changed (if we have a stored root)
		var correctRoot *chainhash.Hash
		if !needsReconcile && storedRootStr.Valid && storedRootStr.String != "" {
			storedRoot, err := chainhash.NewHashFromHex(storedRootStr.String)
			if err != nil {
				needsReconcile = true // If we can't parse it, reconcile it
			} else {
				// Validate if this root is correct for this height
				isValid, err := s.chainTracker.IsValidRootForHeight(ctx, storedRoot, height)
				if err != nil || !isValid {
					needsReconcile = true
				}
			}
		}

		// Reconcile if needed
		// Note: correctRoot is nil here since we're using IsValidRootForHeight
		// The ReconcileMerkleRoot will need to handle validation differently
		if needsReconcile {
			if err := s.ReconcileMerkleRoot(ctx, height, correctRoot); err != nil {
				return fmt.Errorf("failed to reconcile height %d: %w", height, err)
			}
		}
	}

	return rows.Err()
}

// ReconcileMerkleRoot reconciles validation state for all outputs at a given block height
func (s *MySQLTopicDataStorage) ReconcileMerkleRoot(ctx context.Context, blockHeight uint32, merkleRoot *chainhash.Hash) error {
	merkleRootStr := ""
	if merkleRoot != nil {
		merkleRootStr = merkleRoot.String()
	}

	// Determine if this height should be immutable
	isImmutable := false
	if s.chainTracker != nil {
		currentHeight, err := s.chainTracker.CurrentHeight(ctx)
		if err == nil {
			depth := currentHeight - blockHeight
			isImmutable = depth >= IMMUTABILITY_DEPTH
		}
	}

	// Determine the validation state for matching roots
	validState := engine.MerkleStateValidated
	if isImmutable {
		validState = engine.MerkleStateImmutable
	}

	// Update outputs based on merkle root comparison
	_, err := s.db.ExecContext(ctx, `
		UPDATE outputs
		SET merkle_validation_state = CASE
			WHEN merkle_root = ? THEN ?
			WHEN merkle_root IS NULL THEN merkle_validation_state
			ELSE ?
		END
		WHERE topic = ?
		  AND block_height = ?
		  AND merkle_validation_state < ?`,
		merkleRootStr, validState, engine.MerkleStateInvalidated,
		s.topic, blockHeight, engine.MerkleStateImmutable)

	return err
}