package storage

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteEventDataStorage struct {
	BaseEventDataStorage
	wdb *sql.DB
	rdb *sql.DB
}

// GetBeefStorage is inherited from BaseEventDataStorage

func NewSQLiteEventDataStorage(dbPath string, beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubsub pubsub.PubSub) (*SQLiteEventDataStorage, error) {
	var err error
	s := &SQLiteEventDataStorage{
		BaseEventDataStorage: NewBaseEventDataStorage(beefStore, queueStorage, pubsub),
	}

	// Write database connection
	if s.wdb, err = sql.Open("sqlite3", dbPath); err != nil {
		return nil, err
	}

	// Configure write database
	if _, err = s.wdb.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return nil, err
	}
	if _, err = s.wdb.Exec("PRAGMA synchronous=NORMAL;"); err != nil {
		return nil, err
	}
	if _, err = s.wdb.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		return nil, err
	}
	if _, err = s.wdb.Exec("PRAGMA temp_store=MEMORY;"); err != nil {
		return nil, err
	}
	if _, err = s.wdb.Exec("PRAGMA mmap_size=30000000000;"); err != nil {
		return nil, err
	}

	// Create tables
	if err = s.createTables(); err != nil {
		return nil, err
	}

	// Set connection pool settings
	s.wdb.SetMaxOpenConns(10)
	s.wdb.SetMaxIdleConns(5)

	// Read database connection
	if s.rdb, err = sql.Open("sqlite3", dbPath); err != nil {
		return nil, err
	}

	// Configure read database
	if _, err = s.rdb.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return nil, err
	}
	if _, err = s.rdb.Exec("PRAGMA synchronous=NORMAL;"); err != nil {
		return nil, err
	}
	if _, err = s.rdb.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		return nil, err
	}
	if _, err = s.rdb.Exec("PRAGMA temp_store=MEMORY;"); err != nil {
		return nil, err
	}
	if _, err = s.rdb.Exec("PRAGMA mmap_size=30000000000;"); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *SQLiteEventDataStorage) createTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS outputs (
			outpoint TEXT NOT NULL,
			topic TEXT NOT NULL,
			txid TEXT NOT NULL,
			script BLOB NOT NULL,
			satoshis INTEGER NOT NULL,
			spend TEXT, -- Changed from spent BOOLEAN to spend TEXT (spending transaction ID)
			block_height INTEGER,
			block_idx INTEGER,
			score REAL,
			ancillary_beef BLOB,
			data TEXT,   -- JSON data associated with the output
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (outpoint, topic)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_outputs_txid_topic ON outputs(txid, topic)`,
		`DROP INDEX IF EXISTS idx_outputs_txid`,
		`CREATE INDEX IF NOT EXISTS idx_outputs_topic_score ON outputs(topic, score)`,
		`CREATE INDEX IF NOT EXISTS idx_outputs_topic_spend_score ON outputs(topic, spend, score)`,

		`CREATE TABLE IF NOT EXISTS events (
			event TEXT NOT NULL,
			outpoint TEXT NOT NULL,
			topic TEXT NOT NULL,
			score REAL NOT NULL,
			spend TEXT,  -- Denormalized from outputs table for query performance
			PRIMARY KEY (event, outpoint, topic)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_topic_event_score_spend ON events(topic, event, score, spend)`,
		`CREATE INDEX IF NOT EXISTS idx_events_outpoint ON events(outpoint)`,

		`CREATE TABLE IF NOT EXISTS applied_transactions (
			txid TEXT NOT NULL,
			topic TEXT NOT NULL,
			merklepath TEXT,
			block_height INTEGER,
			block_idx INTEGER,
			score REAL,
			status TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (txid, topic)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_applied_tx_topic_score ON applied_transactions(topic, score)`,

		`CREATE TABLE IF NOT EXISTS interactions (
			host TEXT NOT NULL,
			topic TEXT NOT NULL,
			last_score REAL NOT NULL,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (host, topic)
		)`,

		`CREATE TABLE IF NOT EXISTS output_relationships (
			consuming_outpoint TEXT NOT NULL,  -- The output doing the consuming (spender)
			consumed_outpoint TEXT NOT NULL,   -- The output being consumed (spent)
			topic TEXT NOT NULL,
			PRIMARY KEY (consuming_outpoint, consumed_outpoint, topic)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_output_rel_consuming ON output_relationships(consuming_outpoint, topic)`,
		`CREATE INDEX IF NOT EXISTS idx_output_rel_consumed ON output_relationships(consumed_outpoint, topic)`,

		`CREATE TABLE IF NOT EXISTS hashes (
			key_name TEXT NOT NULL,
			field TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (key_name, field)
		)`,

		`CREATE TABLE IF NOT EXISTS sets (
			key_name TEXT NOT NULL,
			member TEXT NOT NULL,
			PRIMARY KEY (key_name, member)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_sets_key ON sets(key_name)`,

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
		if _, err := s.wdb.Exec(query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

func (s *SQLiteEventDataStorage) InsertOutput(ctx context.Context, utxo *engine.Output) error {
	// Save BEEF to BEEF storage
	if err := s.beefStore.SaveBeef(ctx, &utxo.Outpoint.Txid, utxo.Beef); err != nil {
		return err
	}

	// Calculate score
	var score float64
	if utxo.BlockHeight > 0 {
		score = float64(utxo.BlockHeight) + float64(utxo.BlockIdx)/1e9
	} else {
		score = float64(time.Now().Unix())
	}

	// Begin transaction
	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert output (spend field is omitted - defaults to NULL for unspent)
	_, err = tx.ExecContext(ctx, `
		INSERT OR REPLACE INTO outputs (outpoint, topic, txid, script, satoshis,
			block_height, block_idx, score, ancillary_beef)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		utxo.Outpoint.String(),
		utxo.Topic,
		utxo.Outpoint.Txid.String(),
		utxo.Script.Bytes(),
		utxo.Satoshis,
		utxo.BlockHeight,
		utxo.BlockIdx,
		score,
		utxo.AncillaryBeef,
	)
	if err != nil {
		return err
	}

	// Insert output relationships
	// For OutputsConsumed: this output consumes others
	for _, consumed := range utxo.OutputsConsumed {
		_, err = tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO output_relationships (consuming_outpoint, consumed_outpoint, topic)
			VALUES (?, ?, ?)`,
			utxo.Outpoint.String(), // This output is the consumer
			consumed.String(),      // This is what it consumes
			utxo.Topic,
		)
		if err != nil {
			return err
		}
	}

	// For ConsumedBy: other outputs consume this one
	for _, consumedBy := range utxo.ConsumedBy {
		_, err = tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO output_relationships (consuming_outpoint, consumed_outpoint, topic)
			VALUES (?, ?, ?)`,
			consumedBy.String(),    // The other output is the consumer
			utxo.Outpoint.String(), // This output is being consumed
			utxo.Topic,
		)
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	// Add topic as an event using SaveEvents (handles pubsub publishing)
	if err := s.SaveEvents(ctx, &utxo.Outpoint, []string{utxo.Topic}, utxo.Topic, utxo.BlockHeight, utxo.BlockIdx, nil); err != nil {
		return err
	}

	return nil
}

func (s *SQLiteEventDataStorage) FindOutput(ctx context.Context, outpoint *transaction.Outpoint, topic *string, spent *bool, includeBEEF bool) (*engine.Output, error) {
	query := `SELECT outpoint, topic, txid, script, satoshis, spend, 
		block_height, block_idx, score, ancillary_beef 
		FROM outputs WHERE outpoint = ?`
	args := []interface{}{outpoint.String()}

	if topic != nil {
		query += " AND topic = ?"
		args = append(args, *topic)
	}
	if spent != nil {
		if *spent {
			// Looking for spent outputs (spend field is not null)
			query += " AND spend IS NOT NULL"
		} else {
			// Looking for unspent outputs (spend field is null)
			query += " AND spend IS NULL"
		}
	}

	var output engine.Output
	var outpointStr string
	var scriptBytes []byte
	var ancillaryBeef []byte
	var spendTxid *string

	err := s.rdb.QueryRowContext(ctx, query, args...).Scan(
		&outpointStr,
		&output.Topic,
		&output.Outpoint.Txid,
		&scriptBytes,
		&output.Satoshis,
		&spendTxid, // Now scanning spend field as *string
		&output.BlockHeight,
		&output.BlockIdx,
		&output.Score,
		&ancillaryBeef,
	)
	if err == sql.ErrNoRows {
		return nil, engine.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Parse outpoint
	op, err := transaction.OutpointFromString(outpointStr)
	if err != nil {
		return nil, err
	}
	output.Outpoint = *op

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

func (s *SQLiteEventDataStorage) FindOutputs(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
	if len(outpoints) == 0 {
		return nil, nil
	}

	// Build query with placeholders
	placeholders := make([]string, len(outpoints))
	args := make([]interface{}, 0, len(outpoints)+2)

	for i, op := range outpoints {
		placeholders[i] = "?"
		args = append(args, op.String())
	}

	query := fmt.Sprintf(`SELECT outpoint, topic, txid, script, satoshis, spend, 
		block_height, block_idx, score, ancillary_beef 
		FROM outputs WHERE outpoint IN (%s) AND topic = ?`, strings.Join(placeholders, ","))
	args = append(args, topic)

	if spent != nil {
		if *spent {
			// Looking for spent outputs (spend field is not null)
			query += " AND spend IS NOT NULL"
		} else {
			// Looking for unspent outputs (spend field is null)
			query += " AND spend IS NULL"
		}
	}

	rows, err := s.rdb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	outputs := make([]*engine.Output, 0, len(outpoints))
	for rows.Next() {
		output, err := s.scanOutput(rows, includeBEEF)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, output)
	}

	return outputs, rows.Err()
}

func (s *SQLiteEventDataStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
	query := `SELECT outpoint, topic, txid, script, satoshis, spend, 
		block_height, block_idx, score, ancillary_beef 
		FROM outputs WHERE txid = ?`

	rows, err := s.rdb.QueryContext(ctx, query, txid.String())
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

func (s *SQLiteEventDataStorage) FindUTXOsForTopic(ctx context.Context, topic string, since float64, limit uint32, includeBEEF bool) ([]*engine.Output, error) {
	query := `SELECT outpoint, topic, txid, script, satoshis, spend, 
		block_height, block_idx, score, ancillary_beef 
		FROM outputs 
		WHERE topic = ? AND spend IS NULL AND score >= ?
		ORDER BY score
		LIMIT ?`

	rows, err := s.rdb.QueryContext(ctx, query, topic, since, limit)
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

func (s *SQLiteEventDataStorage) DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint, topic string) error {
	_, err := s.wdb.ExecContext(ctx,
		"DELETE FROM outputs WHERE outpoint = ? AND topic = ?",
		outpoint.String(), topic)
	return err
}

func (s *SQLiteEventDataStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spendTxid *chainhash.Hash) error {
	if len(outpoints) == 0 {
		return nil
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	spendTxidStr := spendTxid.String()

	for _, op := range outpoints {
		opStr := op.String()

		// Update outputs table
		_, err = tx.ExecContext(ctx,
			"UPDATE outputs SET spend = ? WHERE outpoint = ? AND topic = ?",
			spendTxidStr, opStr, topic)
		if err != nil {
			return err
		}

		// Update events table for all events associated with this outpoint
		_, err = tx.ExecContext(ctx,
			"UPDATE events SET spend = ? WHERE outpoint = ?",
			spendTxidStr, opStr)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteEventDataStorage) UpdateConsumedBy(ctx context.Context, outpoint *transaction.Outpoint, topic string, consumedBy []*transaction.Outpoint) error {
	// No-op: Output relationships are already managed by InsertOutput method.
	// InsertOutput handles both OutputsConsumed and ConsumedBy relationships atomically
	// using INSERT OR IGNORE, which is safe for concurrent access.
	// This method was causing UNIQUE constraint violations due to DELETE->INSERT race conditions.
	return nil
}

func (s *SQLiteEventDataStorage) UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error {
	return s.beefStore.SaveBeef(ctx, txid, beef)
}

func (s *SQLiteEventDataStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *transaction.Outpoint, topic string, blockHeight uint32, blockIndex uint64, ancillaryBeef []byte) error {
	score := float64(blockHeight) + float64(blockIndex)/1e9
	outpointStr := outpoint.String()

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Update outputs table
	_, err = tx.ExecContext(ctx, `
		UPDATE outputs 
		SET block_height = ?, block_idx = ?, score = ?, ancillary_beef = ?
		WHERE outpoint = ? AND topic = ?`,
		blockHeight, blockIndex, score, ancillaryBeef,
		outpointStr, topic)
	if err != nil {
		return err
	}

	// Update score in events table for all events associated with this outpoint
	_, err = tx.ExecContext(ctx, `
		UPDATE events 
		SET score = ?
		WHERE outpoint = ?`,
		score, outpointStr)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *SQLiteEventDataStorage) InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error {
	score := float64(time.Now().Unix())

	_, err := s.wdb.ExecContext(ctx, `
		INSERT OR REPLACE INTO applied_transactions 
		(txid, topic, score, created_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
		tx.Txid.String(),
		tx.Topic,
		score,
	)

	if err != nil {
		return err
	}

	// Publish transaction to topic via PubSub (if available)
	if s.pubsub != nil {
		// For topic events (tm_*), publish the txid with the score
		if err := s.pubsub.Publish(ctx, tx.Topic, tx.Txid.String(), score); err != nil {
			// Log error but don't fail the transaction insertion
			log.Printf("Failed to publish transaction to topic %s: %v", tx.Topic, err)
		}
	}

	return nil
}

func (s *SQLiteEventDataStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	var exists bool
	err := s.rdb.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM applied_transactions WHERE txid = ? AND topic = ?)",
		tx.Txid.String(), tx.Topic).Scan(&exists)
	return exists, err
}

func (s *SQLiteEventDataStorage) UpdateLastInteraction(ctx context.Context, host string, topic string, since float64) error {
	_, err := s.wdb.ExecContext(ctx, `
		INSERT OR REPLACE INTO interactions (host, topic, last_score, updated_at)
		VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
		host, topic, since)
	return err
}

func (s *SQLiteEventDataStorage) GetLastInteraction(ctx context.Context, host string, topic string) (float64, error) {
	var score float64
	err := s.rdb.QueryRowContext(ctx,
		"SELECT last_score FROM interactions WHERE host = ? AND topic = ?",
		host, topic).Scan(&score)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return score, err
}

func (s *SQLiteEventDataStorage) Close() error {
	if s.wdb != nil {
		s.wdb.Close()
	}
	if s.rdb != nil {
		s.rdb.Close()
	}
	return nil
}

// Helper functions

func (s *SQLiteEventDataStorage) scanOutput(rows *sql.Rows, includeBEEF bool) (*engine.Output, error) {
	var output engine.Output
	var outpointStr, txidStr string
	var scriptBytes []byte
	var ancillaryBeef []byte
	var spendTxid *string // Changed from Spent bool to spendTxid *string

	err := rows.Scan(
		&outpointStr,
		&output.Topic,
		&txidStr,
		&scriptBytes,
		&output.Satoshis,
		&spendTxid, // Now scanning the spend field as *string
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

func (s *SQLiteEventDataStorage) loadOutputRelations(ctx context.Context, output *engine.Output) error {
	// Load outputs consumed by this output (this output is the consumer)
	rows, err := s.rdb.QueryContext(ctx,
		"SELECT consumed_outpoint FROM output_relationships WHERE consuming_outpoint = ? AND topic = ?",
		output.Outpoint.String(), output.Topic)
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

	// Load outputs that consume this output (this output is being consumed)
	rows2, err := s.rdb.QueryContext(ctx,
		"SELECT consuming_outpoint FROM output_relationships WHERE consumed_outpoint = ? AND topic = ?",
		output.Outpoint.String(), output.Topic)
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

// GetTransactionsByTopicAndHeight returns all transactions for a topic at a specific block height
func (s *SQLiteEventDataStorage) GetTransactionsByTopicAndHeight(ctx context.Context, topic string, height uint32) ([]*TransactionData, error) {
	// Query 1: Get all outputs for the topic and block height, including data field
	query := `SELECT outpoint, txid, script, satoshis, spend, data 
	         FROM outputs 
	         WHERE topic = ? AND block_height = ?
	         ORDER BY txid, outpoint`

	rows, err := s.rdb.QueryContext(ctx, query, topic, height)
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

	// Query 2: Get all inputs for these transactions in one batch
	placeholders := make([]string, len(txids))
	args := make([]interface{}, len(txids)+1)
	args[0] = topic
	for i, txid := range txids {
		placeholders[i] = "?"
		args[i+1] = txid
	}

	inputQuery := fmt.Sprintf(`SELECT outpoint, txid, script, satoshis, spend, data 
	                          FROM outputs WHERE topic = ? AND spend IN (%s)`, strings.Join(placeholders, ","))

	inputRows, err := s.rdb.QueryContext(ctx, inputQuery, args...)
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

		// Create OutputData for input (includes source txid)
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

// GetTransactionByTopic returns a single transaction for a topic by txid
func (s *SQLiteEventDataStorage) GetTransactionByTopic(ctx context.Context, topic string, txid *chainhash.Hash, includeBeef ...bool) (*TransactionData, error) {
	// Query 1: Get all outputs for the specific transaction and topic
	query := `SELECT outpoint, txid, script, satoshis, spend, data 
	         FROM outputs 
	         WHERE topic = ? AND txid = ?
	         ORDER BY outpoint`

	rows, err := s.rdb.QueryContext(ctx, query, topic, txid.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var outputs []*OutputData

	// Process outputs
	for rows.Next() {
		var outpointStr, txidStr string
		var scriptBytes []byte
		var satoshis uint64
		var spendStr sql.NullString
		var dataJSON sql.NullString

		err := rows.Scan(&outpointStr, &txidStr, &scriptBytes, &satoshis, &spendStr, &dataJSON)
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
			TxID:     txid,
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

	// Query 2: Get all inputs for this transaction using spend references
	var inputs []*OutputData
	inputQuery := `SELECT outpoint, txid, script, satoshis, data 
	              FROM outputs 
	              WHERE spend = ? AND topic = ?`

	inputRows, err := s.rdb.QueryContext(ctx, inputQuery, txid.String(), topic)
	if err != nil {
		return nil, err
	}
	defer inputRows.Close()

	for inputRows.Next() {
		var outpointStr, txidStr, scriptHex string
		var satoshis uint64
		var dataJSON sql.NullString

		err := inputRows.Scan(&outpointStr, &txidStr, &scriptHex, &satoshis, &dataJSON)
		if err != nil {
			continue
		}

		// Parse outpoint to get vout
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			continue
		}

		// Parse script from hex
		script, err := hex.DecodeString(scriptHex)
		if err != nil {
			continue
		}

		// Parse data if exists
		var data interface{}
		if dataJSON.Valid && dataJSON.String != "" {
			json.Unmarshal([]byte(dataJSON.String), &data)
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
		TxID:    *txid,
		Outputs: outputs,
		Inputs:  inputs,
	}

	// Load BEEF if requested
	if len(includeBeef) > 0 && includeBeef[0] {
		beef, err := s.LoadBeefByTxidAndTopic(ctx, txid, topic)
		if err != nil {
			// Log but don't fail - BEEF is optional
		} else {
			txData.Beef = beef
		}
	}

	return txData, nil
}

// SaveEvents associates multiple events with a single output, storing arbitrary data
func (s *SQLiteEventDataStorage) SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, topic string, height uint32, idx uint64, data interface{}) error {
	if len(events) == 0 && data == nil {
		return nil
	}

	var score float64
	if height > 0 {
		score = float64(height) + float64(idx)/1e9
	} else {
		score = float64(time.Now().Unix())
	}

	outpointStr := outpoint.String()

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert events into events table
	for _, event := range events {
		_, err = tx.ExecContext(ctx, `
			INSERT OR REPLACE INTO events (event, outpoint, topic, score)
			VALUES (?, ?, ?, ?)`,
			event, outpointStr, topic, score)
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

		// Get topic from outputs table first
		var topic string
		err = tx.QueryRowContext(ctx, `SELECT topic FROM outputs WHERE outpoint = ? LIMIT 1`, outpointStr).Scan(&topic)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE outputs 
			SET data = CASE 
				WHEN data IS NULL THEN ?
				ELSE json_patch(data, ?)
			END
			WHERE outpoint = ? AND topic = ?`,
			string(dataJSON), string(dataJSON),
			outpointStr, topic)
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
			// Publish event with outpoint string as the message
			if err := s.pubsub.Publish(ctx, event, outpointStr); err != nil {
				// Log error but don't fail the operation
				// Publishing is best-effort
				continue
			}
		}
	}

	return nil
}

// FindEvents returns all events associated with a given outpoint
func (s *SQLiteEventDataStorage) FindEvents(ctx context.Context, outpoint *transaction.Outpoint) ([]string, error) {
	rows, err := s.rdb.QueryContext(ctx,
		"SELECT event FROM events WHERE outpoint = ?",
		outpoint.String())
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
func (s *SQLiteEventDataStorage) LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error) {
	withData := len(includeData) > 0 && includeData[0]

	// Build the query
	var query strings.Builder
	var args []interface{}

	// Handle event filtering using the events table
	if question.Event != "" {
		// Single event query - use the events table directly
		query.WriteString("SELECT e.outpoint, e.score")
		if withData {
			query.WriteString(", o.data")
		}
		query.WriteString(" FROM events e")
		if withData {
			query.WriteString(" LEFT JOIN outputs o ON e.outpoint = o.outpoint")
		}
		query.WriteString(" WHERE e.event = ?")
		args = append(args, question.Event)

		// Add topic filtering if specified
		if question.Topic != "" {
			query.WriteString(" AND e.topic = ?")
			args = append(args, question.Topic)
		}
	} else if len(question.Events) > 0 {
		if question.JoinType == nil || *question.JoinType == JoinTypeUnion {
			// Union: find outputs with ANY of the events
			query.WriteString("SELECT DISTINCT e.outpoint, e.score")
			if withData {
				query.WriteString(", o.data")
			}
			query.WriteString(" FROM events e")
			if withData {
				query.WriteString(" LEFT JOIN outputs o ON e.outpoint = o.outpoint")
			}
			query.WriteString(" WHERE e.event IN (")
			placeholders := make([]string, len(question.Events))
			for i, event := range question.Events {
				placeholders[i] = "?"
				args = append(args, event)
			}
			query.WriteString(strings.Join(placeholders, ","))
			query.WriteString(")")

			// Add topic filtering if specified
			if question.Topic != "" {
				query.WriteString(" AND e.topic = ?")
				args = append(args, question.Topic)
			}
		} else if *question.JoinType == JoinTypeIntersect {
			// Intersection: find outputs that have ALL events
			query.WriteString("SELECT e1.outpoint, e1.score")
			if withData {
				query.WriteString(", o.data")
			}
			query.WriteString(" FROM events e1")
			for i := 1; i < len(question.Events); i++ {
				query.WriteString(fmt.Sprintf(" INNER JOIN events e%d ON e1.outpoint = e%d.outpoint", i+1, i+1))
			}
			if withData {
				query.WriteString(" LEFT JOIN outputs o ON e1.outpoint = o.outpoint")
			}
			query.WriteString(" WHERE e1.event = ?")
			args = append(args, question.Events[0])
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
				query.WriteString(" LEFT JOIN outputs o ON e.outpoint = o.outpoint")
			}
			query.WriteString(" WHERE e.event = ? AND NOT EXISTS (SELECT 1 FROM events e2 WHERE e2.outpoint = e.outpoint AND e2.event IN (")
			args = append(args, question.Events[0])
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
		query.WriteString(" FROM outputs WHERE 1=1")
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

	// Unspent filter - now we can use the denormalized spend column in events table
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
	rows, err := s.rdb.QueryContext(ctx, query.String(), args...)
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
func (s *SQLiteEventDataStorage) GetOutputData(ctx context.Context, outpoint *transaction.Outpoint) (interface{}, error) {
	var dataJSON *string
	err := s.rdb.QueryRowContext(ctx,
		"SELECT data FROM outputs WHERE outpoint = ? LIMIT 1",
		outpoint.String()).Scan(&dataJSON)

	if err == sql.ErrNoRows {
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

// LoadBeefByTxidAndTopic loads merged BEEF for a transaction within a topic context
func (s *SQLiteEventDataStorage) LoadBeefByTxidAndTopic(ctx context.Context, txid *chainhash.Hash, topic string) ([]byte, error) {
	// Find any output for this txid in the specified topic
	var ancillaryBeef []byte
	err := s.rdb.QueryRowContext(ctx,
		"SELECT ancillary_beef FROM outputs WHERE txid = ? AND topic = ? LIMIT 1",
		txid.String(), topic,
	).Scan(&ancillaryBeef)

	if err != nil {
		return nil, fmt.Errorf("transaction %s not found in topic %s: %w", txid.String(), topic, err)
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

	// Merge AncillaryBeef if present (field is optional)
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

// FindOutputData returns outputs matching the given query criteria as OutputData objects
func (s *SQLiteEventDataStorage) FindOutputData(ctx context.Context, question *EventQuestion) ([]*OutputData, error) {
	var query strings.Builder
	var args []interface{}

	// Base query selecting OutputData fields including txid and spending txid
	query.WriteString(`
		SELECT DISTINCT o.outpoint, o.script, o.satoshis, o.data, o.spend, o.score
		FROM outputs o
	`)
	// LEFT JOIN outputs s ON s.spend = o.txid AND s.topic = o.topic

	// Add event filtering if needed
	if question.Event != "" || len(question.Events) > 0 {
		query.WriteString(" JOIN events oe ON o.outpoint = oe.outpoint")
	}

	query.WriteString(" WHERE 1=1")

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
			query.WriteString(fmt.Sprintf(" AND oe.event IN (%s) GROUP BY o.outpoint HAVING COUNT(DISTINCT oe.event) = %d",
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

	rows, err := s.rdb.QueryContext(ctx, query.String(), args...)
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
func (s *SQLiteEventDataStorage) LookupEventScores(ctx context.Context, topic string, event string, fromScore float64) ([]queue.ScoredMember, error) {
	// Query the events table directly without joining to outputs
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT outpoint, score 
		FROM events 
		WHERE topic = ? AND event = ? AND score > ? 
		ORDER BY score ASC`,
		topic, event, fromScore)
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
func (s *SQLiteEventDataStorage) CountOutputs(ctx context.Context, topic string) (int64, error) {
	var count int64
	err := s.rdb.QueryRowContext(ctx, "SELECT COUNT(1) FROM outputs WHERE topic = ?", topic).Scan(&count)
	return count, err
}
