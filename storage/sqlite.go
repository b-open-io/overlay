package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
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

// Methods GetBeefStorage and GetPublisher are inherited from BaseEventDataStorage

func NewSQLiteEventDataStorage(dbPath string, beefStore beef.BeefStorage, pub publish.Publisher) (*SQLiteEventDataStorage, error) {
	var err error
	s := &SQLiteEventDataStorage{
		BaseEventDataStorage: NewBaseEventDataStorage(beefStore, pub),
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
		`CREATE INDEX IF NOT EXISTS idx_outputs_txid ON outputs(txid)`,
		`CREATE INDEX IF NOT EXISTS idx_outputs_topic_score ON outputs(topic, score)`,
		`CREATE INDEX IF NOT EXISTS idx_outputs_topic_spend_score ON outputs(topic, spend, score)`,

		`CREATE TABLE IF NOT EXISTS events (
			event TEXT NOT NULL,
			outpoint TEXT NOT NULL,
			score REAL NOT NULL,
			spend TEXT,  -- Denormalized from outputs table for query performance
			PRIMARY KEY (event, outpoint)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_event_score ON events(event, score)`,
		`CREATE INDEX IF NOT EXISTS idx_events_event_spend_score ON events(event, spend, score)`,
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

		// Queue Management Tables
		`CREATE TABLE IF NOT EXISTS sorted_sets (
			key TEXT NOT NULL,
			member TEXT NOT NULL,
			score REAL NOT NULL,
			PRIMARY KEY (key, member)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_sorted_sets_score ON sorted_sets(key, score)`,

		`CREATE TABLE IF NOT EXISTS sets (
			key TEXT NOT NULL,
			member TEXT NOT NULL,
			PRIMARY KEY (key, member)
		)`,

		`CREATE TABLE IF NOT EXISTS hashes (
			key TEXT NOT NULL,
			field TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (key, field)
		)`,
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

	// Publish if configured
	if s.pub != nil {
		s.pub.Publish(ctx, utxo.Topic, base64.StdEncoding.EncodeToString(utxo.Beef))
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
	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete existing relationships where this output is being consumed
	_, err = tx.ExecContext(ctx,
		"DELETE FROM output_relationships WHERE consumed_outpoint = ? AND topic = ?",
		outpoint.String(), topic)
	if err != nil {
		return err
	}

	// Insert new relationships where other outputs consume this one
	for _, cb := range consumedBy {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO output_relationships (consuming_outpoint, consumed_outpoint, topic) VALUES (?, ?, ?)",
			cb.String(),       // The other output is the consumer
			outpoint.String(), // This output is being consumed
			topic)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
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
	return err
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

// SaveEvents associates multiple events with a single output, storing arbitrary data
func (s *SQLiteEventDataStorage) SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, height uint32, idx uint64, data interface{}) error {
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
			INSERT OR REPLACE INTO events (event, outpoint, score)
			VALUES (?, ?, ?)`,
			event, outpointStr, score)
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
	if s.pub != nil {
		for _, event := range events {
			// Publish event with outpoint string as the message
			if err := s.pub.Publish(ctx, event, outpointStr); err != nil {
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

// Queue Management Methods Implementation for SQLite

// ZAdd adds members with scores to a sorted set
func (s *SQLiteEventDataStorage) ZAdd(ctx context.Context, key string, members ...ZMember) error {
	if len(members) == 0 {
		return nil
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO sorted_sets (key, member, score) 
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, m := range members {
		if _, err := stmt.Exec(key, m.Member, m.Score); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ZRem removes members from a sorted set
func (s *SQLiteEventDataStorage) ZRem(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`DELETE FROM sorted_sets WHERE key = ? AND member = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, member := range members {
		if _, err := stmt.Exec(key, member); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ZScore returns the score of a member in a sorted set
func (s *SQLiteEventDataStorage) ZScore(ctx context.Context, key string, member string) (float64, error) {
	var score float64
	err := s.rdb.QueryRowContext(ctx, `
		SELECT score FROM sorted_sets WHERE key = ? AND member = ?
	`, key, member).Scan(&score)

	if err == sql.ErrNoRows {
		return 0, nil // Return 0 for non-existent member
	}

	return score, err
}

// ZRange returns members in a sorted set by score range (ascending)
func (s *SQLiteEventDataStorage) ZRange(ctx context.Context, key string, min, max float64, offset, count int64) ([]ZMember, error) {
	var query string
	var args []interface{}

	if count > 0 {
		query = `
			SELECT member, score FROM sorted_sets 
			WHERE key = ? AND score >= ? AND score <= ?
			ORDER BY score ASC, member ASC
			LIMIT ? OFFSET ?
		`
		args = []interface{}{key, min, max, count, offset}
	} else {
		query = `
			SELECT member, score FROM sorted_sets 
			WHERE key = ? AND score >= ? AND score <= ?
			ORDER BY score ASC, member ASC
		`
		args = []interface{}{key, min, max}
	}

	rows, err := s.rdb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ZMember
	for rows.Next() {
		var member string
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		members = append(members, ZMember{
			Score:  score,
			Member: member,
		})
	}

	return members, rows.Err()
}

// ZRevRange returns members in a sorted set by score range (descending)
func (s *SQLiteEventDataStorage) ZRevRange(ctx context.Context, key string, max, min float64, offset, count int64) ([]ZMember, error) {
	var query string
	var args []interface{}

	if count > 0 {
		query = `
			SELECT member, score FROM sorted_sets 
			WHERE key = ? AND score >= ? AND score <= ?
			ORDER BY score DESC, member DESC
			LIMIT ? OFFSET ?
		`
		args = []interface{}{key, min, max, count, offset}
	} else {
		query = `
			SELECT member, score FROM sorted_sets 
			WHERE key = ? AND score >= ? AND score <= ?
			ORDER BY score DESC, member DESC
		`
		args = []interface{}{key, min, max}
	}

	rows, err := s.rdb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []ZMember
	for rows.Next() {
		var member string
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		members = append(members, ZMember{
			Score:  score,
			Member: member,
		})
	}

	return members, rows.Err()
}

// SAdd adds members to a set
func (s *SQLiteEventDataStorage) SAdd(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO sets (key, member) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, member := range members {
		if _, err := stmt.Exec(key, member); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// SRem removes members from a set
func (s *SQLiteEventDataStorage) SRem(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`DELETE FROM sets WHERE key = ? AND member = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, member := range members {
		if _, err := stmt.Exec(key, member); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// SMembers returns all members of a set
func (s *SQLiteEventDataStorage) SMembers(ctx context.Context, key string) ([]string, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT member FROM sets WHERE key = ?`, key)
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

// HSet sets a field in a hash
func (s *SQLiteEventDataStorage) HSet(ctx context.Context, key string, field string, value interface{}) error {
	// Convert value to string
	var valStr string
	switch v := value.(type) {
	case string:
		valStr = v
	case int, int64, uint64:
		valStr = fmt.Sprintf("%v", v)
	default:
		// For complex types, use JSON encoding
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		valStr = string(data)
	}

	_, err := s.wdb.ExecContext(ctx, `
		INSERT OR REPLACE INTO hashes (key, field, value) 
		VALUES (?, ?, ?)
	`, key, field, valStr)

	return err
}

// HGet gets a field from a hash
func (s *SQLiteEventDataStorage) HGet(ctx context.Context, key string, field string) (string, error) {
	var value string
	err := s.rdb.QueryRowContext(ctx, `
		SELECT value FROM hashes WHERE key = ? AND field = ?
	`, key, field).Scan(&value)

	if err == sql.ErrNoRows {
		return "", nil // Return empty string for non-existent field
	}

	return value, err
}

// HMSet sets multiple fields in a hash
func (s *SQLiteEventDataStorage) HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	if len(fields) == 0 {
		return nil
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR REPLACE INTO hashes (key, field, value) 
		VALUES (?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for field, value := range fields {
		// Convert value to string
		var valStr string
		switch v := value.(type) {
		case string:
			valStr = v
		case int, int64, uint64, float64, bool:
			valStr = fmt.Sprintf("%v", v)
		default:
			// For complex types, use JSON encoding
			data, err := json.Marshal(value)
			if err != nil {
				return err
			}
			valStr = string(data)
		}

		if _, err := stmt.ExecContext(ctx, key, field, valStr); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// HGetAll gets all fields from a hash
func (s *SQLiteEventDataStorage) HGetAll(ctx context.Context, key string) (map[string]interface{}, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT field, value FROM hashes WHERE key = ?
	`, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]interface{})
	for rows.Next() {
		var field, value string
		if err := rows.Scan(&field, &value); err != nil {
			return nil, err
		}

		// Try to parse as JSON first (for complex types)
		var parsed interface{}
		if err := json.Unmarshal([]byte(value), &parsed); err == nil {
			result[field] = parsed
		} else {
			// If not JSON, use the string value directly
			result[field] = value
		}
	}

	return result, rows.Err()
}
