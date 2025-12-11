package beef

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/b-open-io/overlay/dedup"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

var BeefKey = "beef"
var ErrNotFound = errors.New("not-found")
var ErrInvalidMerkleProof = errors.New("invalid merkle proof")
var ErrMissingInputs = errors.New("missing required input transactions")

// splitBEEF extracts raw transaction and merkle proof from BEEF bytes
func splitBEEF(beefBytes []byte) (rawTx []byte, proof []byte, err error) {
	_, tx, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, nil, err
	}
	if tx == nil {
		return nil, nil, errors.New("no transaction in BEEF")
	}

	rawTx = tx.Bytes()
	if tx.MerklePath != nil {
		proof = tx.MerklePath.Bytes()
	}
	return rawTx, proof, nil
}

// assembleBEEF creates BEEF bytes from raw transaction and optional merkle proof
func assembleBEEF(txid *chainhash.Hash, rawTx []byte, proof []byte) ([]byte, error) {
	tx, err := transaction.NewTransactionFromBytes(rawTx)
	if err != nil {
		return nil, err
	}

	if len(proof) > 0 {
		mp, err := transaction.NewMerklePathFromBinary(proof)
		if err != nil {
			return nil, err
		}
		tx.MerklePath = mp
	}

	// Create a minimal BEEF containing just this transaction
	beef := &transaction.Beef{
		Version:      transaction.BEEF_V2,
		BUMPs:        []*transaction.MerklePath{},
		Transactions: make(map[chainhash.Hash]*transaction.BeefTx),
	}

	beefTx := &transaction.BeefTx{
		Transaction: tx,
		BumpIndex:   -1,
	}

	if tx.MerklePath != nil {
		beef.BUMPs = append(beef.BUMPs, tx.MerklePath)
		beefTx.BumpIndex = 0
		beefTx.DataFormat = transaction.RawTxAndBumpIndex
	} else {
		beefTx.DataFormat = transaction.RawTx
	}

	beef.Transactions[*txid] = beefTx

	return beef.AtomicBytes(txid)
}

// BaseBeefStorage is a simple key/value storage interface for BEEF data
// Implementations should be simple and not include fallback or deduplication logic
type BaseBeefStorage interface {
	// Get loads a complete BEEF (assembled from tx + proof if stored separately)
	Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error)
	// Put stores a BEEF (may split into tx + proof for separate storage)
	Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error
	// UpdateMerklePath fetches a fresh BEEF with merkle proof for the transaction
	// Only JungleBus implements this; other storages return ErrNotFound
	UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error)
	Close() error

	// Separate tx/proof read methods for selective loading
	// GetRawTx loads just the raw transaction bytes (without proof)
	GetRawTx(ctx context.Context, txid *chainhash.Hash) ([]byte, error)
	// GetProof loads just the merkle proof bytes
	GetProof(ctx context.Context, txid *chainhash.Hash) ([]byte, error)
}

// Storage is the main implementation that handles all high-level BEEF operations
// including fallback chains, deduplication, recursive storage, and dynamic BEEF building
type Storage struct {
	storages     []BaseBeefStorage
	loader       *dedup.Loader[chainhash.Hash, []byte]
	saver        *dedup.Saver[chainhash.Hash, []byte]
	chainTracker chaintracker.ChainTracker // Optional: if set, validates merkle proofs on load
}

// NewStorage creates a new Storage from a connection string with optional SPV validation
//
// Parameters:
//   - connectionString: Storage configuration (see below for formats)
//   - chainTracker: Optional. If provided, validates merkle proofs on load and updates invalid ones
//
// The connection string can be:
//   - A single connection string: "redis://localhost:6379"
//   - A JSON array of connection strings: `["lru://100mb", "redis://localhost:6379", "junglebus://"]`
//   - A comma-separated list: "lru://100mb,redis://localhost:6379,junglebus://"
//   - Empty string: defaults to ~/.1sat/beef/ (falls back to ./beef/)
//
// Note: If your connection strings contain commas, use the JSON array format.
//
// Supported storage formats:
//   - lru://?size=100mb or lru://?size=1gb (in-memory LRU cache with size limit)
//   - redis://localhost:6379?ttl=24h (Redis with optional TTL parameter)
//   - sqlite:///path/to/beef.db or sqlite://beef.db
//   - file:///path/to/storage/dir
//   - s3://bucket-name/?region=us-west-2&endpoint=https://s3.amazonaws.com
//   - s3://access-key:secret-key@bucket-name/?endpoint=https://minio.example.com
//   - junglebus:// (fetches from JungleBus API, provides merkle proofs)
//   - ./beef.db (inferred as SQLite)
//   - ./beef/ (inferred as filesystem)
//
// Example:
//
//	NewStorage(`["lru://?size=100mb", "redis://localhost:6379", "sqlite://beef.db", "junglebus://"]`, chainTracker)
//	Creates: LRU -> Redis -> SQLite -> JungleBus with SPV validation
func NewStorage(connectionString string, chainTracker chaintracker.ChainTracker) (*Storage, error) {
	storages, err := parseConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		storages:     storages,
		chainTracker: chainTracker,
	}

	// Create loader with deduplication
	s.loader = dedup.NewLoader(func(txid chainhash.Hash) ([]byte, error) {
		return s.loadBeefInternal(context.Background(), &txid)
	})

	// Create saver with deduplication
	s.saver = dedup.NewSaver(func(txid chainhash.Hash, beefBytes []byte) error {
		return s.saveBeefInternal(context.Background(), &txid, beefBytes)
	})

	return s, nil
}

// LoadBeef loads a BEEF from storage.
func (s *Storage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return s.loader.Load(*txid)
}

// MergeBeef takes an existing Beef object and merges in additional transactions by their txids.
// Returns the merged Beef object.
func (s *Storage) MergeBeef(ctx context.Context, beef *transaction.Beef, txids []*chainhash.Hash) (*transaction.Beef, error) {
	if len(txids) == 0 {
		return beef, nil
	}

	for _, txid := range txids {
		additionalBeef, err := s.loader.Load(*txid)
		if err != nil {
			return nil, errors.New("failed to load tx for merge " + txid.String() + ": " + err.Error())
		}
		if err := beef.MergeBeefBytes(additionalBeef); err != nil {
			return nil, err
		}
	}

	return beef, nil
}

// loadBeefInternal is the actual load implementation
func (s *Storage) loadBeefInternal(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// Try to load from each storage sequentially
	var beefBytes []byte
	var err error

	for i, storage := range s.storages {
		beefBytes, err = storage.Get(ctx, txid)
		if err == nil {
			// Cache back to earlier storages
			for j := 0; j < i; j++ {
				s.storages[j].Put(ctx, txid, beefBytes)
			}
			break
		}
		if err != ErrNotFound {
			return nil, err
		}
	}

	if beefBytes == nil {
		return nil, errors.New("transaction " + txid.String() + " not found in BEEF")
	}

	// Parse the BEEF
	beef, tx, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, err
	}

	if tx == nil {
		return nil, errors.New("transaction " + txid.String() + " not found in BEEF")
	}

	// If no merkle path, recursively load inputs
	if tx.MerklePath == nil {
		for _, input := range tx.Inputs {
			inputBeef, err := s.loader.Load(*input.SourceTXID)
			if err != nil {
				return nil, errors.New(ErrMissingInputs.Error() + ": " + input.SourceTXID.String())
			}
			if err := beef.MergeBeefBytes(inputBeef); err != nil {
				return nil, err
			}
		}
	}

	// SPV validation if chainTracker is configured (after inputs are loaded)
	if s.chainTracker != nil {
		needsUpdate := false

		if tx.MerklePath == nil {
			// No merkle path, need to update
			needsUpdate = true
		} else {
			// Validate existing merkle proof
			valid, err := tx.MerklePath.Verify(ctx, txid, s.chainTracker)
			if err != nil || !valid {
				needsUpdate = true
			}
		}

		if needsUpdate {
			// Use UpdateMerklePath to fetch and verify updated proof
			updatedBeef, err := s.UpdateMerklePath(ctx, txid, s.chainTracker)
			if err != nil {
				return nil, err
			}
			// Merge the updated proof into our BEEF
			if err := beef.MergeBeefBytes(updatedBeef); err != nil {
				return nil, err
			}
		}
	}

	return beef.Bytes()
}

// SaveBeef saves a BEEF by decomposing it into individual transactions
func (s *Storage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	return s.saver.Save(*txid, beefBytes)
}

// saveBeefInternal is the actual save implementation
func (s *Storage) saveBeefInternal(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	// Parse the BEEF
	beef, tx, parsedTxid, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return errors.New("failed to parse BEEF: " + err.Error())
	}

	// Determine the main transaction
	var mainTx *transaction.Transaction
	if tx != nil {
		mainTx = tx
	} else if beef != nil && parsedTxid != nil {
		mainTx = beef.FindTransactionByHash(parsedTxid)
	} else if beef != nil && txid != nil {
		mainTx = beef.FindTransactionByHash(txid)
	}

	if mainTx == nil {
		return errors.New("could not find transaction in BEEF")
	}

	// Save all transactions in the BEEF individually
	if beef != nil {
		for txHash, beefTx := range beef.Transactions {
			if beefTx.Transaction == nil {
				continue
			}

			// Create individual BEEF for this transaction
			individualBeef, err := s.createIndividualBEEF(&txHash, beefTx.Transaction)
			if err != nil {
				return errors.New("failed to create individual BEEF for " + txHash.String() + ": " + err.Error())
			}

			// Save to all storages
			for _, storage := range s.storages {
				if err := storage.Put(ctx, &txHash, individualBeef); err != nil {
					return errors.New("failed to save to storage: " + err.Error())
				}
			}
		}
	} else {
		// Single transaction BEEF - save as-is
		for _, storage := range s.storages {
			if err := storage.Put(ctx, txid, beefBytes); err != nil {
				return errors.New("failed to save to storage: " + err.Error())
			}
		}
	}

	return nil
}

// createIndividualBEEF creates a BEEF for a single transaction with its merkle proof if available
func (s *Storage) createIndividualBEEF(txid *chainhash.Hash, tx *transaction.Transaction) ([]byte, error) {
	// Create a new BEEF with just this transaction
	beef := &transaction.Beef{
		Version:      transaction.BEEF_V2,
		BUMPs:        []*transaction.MerklePath{},
		Transactions: make(map[chainhash.Hash]*transaction.BeefTx),
	}

	// Add the transaction
	beefTx := &transaction.BeefTx{
		Transaction: tx,
		BumpIndex:   -1,
	}

	// If transaction has a merkle proof, include it
	if tx.MerklePath != nil {
		beef.BUMPs = append(beef.BUMPs, tx.MerklePath)
		beefTx.BumpIndex = 0
		beefTx.DataFormat = transaction.RawTxAndBumpIndex
	} else {
		beefTx.DataFormat = transaction.RawTx
	}

	beef.Transactions[*txid] = beefTx

	// Use AtomicBytes to create a BEEF that marks this transaction as the main one
	// This ensures ParseBeef will correctly return this transaction
	return beef.AtomicBytes(txid)
}

// UpdateMerklePath attempts to fetch an updated BEEF with merkle proof from the storage chain
func (s *Storage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash, ct chaintracker.ChainTracker) ([]byte, error) {
	// Try to get updated BEEF from each storage (only JungleBus can provide this)
	for _, storage := range s.storages {
		updatedBeef, err := storage.UpdateMerklePath(ctx, txid)
		if err != nil {
			continue
		}
		if updatedBeef != nil && len(updatedBeef) > 0 {
			// Verify the merkle proof if chaintracker is provided
			if ct != nil {
				_, tx, _, parseErr := transaction.ParseBeef(updatedBeef)
				if parseErr == nil && tx != nil && tx.MerklePath != nil {
					valid, verifyErr := tx.MerklePath.Verify(ctx, txid, ct)
					if verifyErr == nil && valid {
						// Save the valid proof and return
						s.saveBeefInternal(ctx, txid, updatedBeef)
						return updatedBeef, nil
					}
				}
			} else {
				// No verification needed, save and return
				s.saveBeefInternal(ctx, txid, updatedBeef)
				return updatedBeef, nil
			}
		}
	}
	return nil, errors.New("unable to fetch updated merkle proof for " + txid.String())
}

// Close closes all storages in the chain
func (s *Storage) Close() error {
	if s.loader != nil {
		s.loader.Clear()
	}
	if s.saver != nil {
		s.saver.Clear()
	}

	var errs []error
	for _, storage := range s.storages {
		if err := storage.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.New("errors closing storages")
	}
	return nil
}

// LoadTx loads a transaction from the storage
func (s *Storage) LoadTx(ctx context.Context, txid *chainhash.Hash) (*transaction.Transaction, error) {
	beefBytes, err := s.LoadBeef(ctx, txid)
	if err != nil {
		return nil, err
	}

	return s.LoadTxFromBeef(ctx, beefBytes, txid)
}

// LoadTxFromBeef loads a transaction from BEEF bytes
func (s *Storage) LoadTxFromBeef(ctx context.Context, beefBytes []byte, txid *chainhash.Hash) (*transaction.Transaction, error) {
	// Parse BEEF to get the transaction
	beef, tx, parsedTxid, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, err
	}

	// If ParseBeef returned a txid, verify it matches what we requested
	if parsedTxid != nil && !parsedTxid.IsEqual(txid) {
		return nil, errors.New("txid mismatch: requested " + txid.String() + ", got " + parsedTxid.String())
	}

	// If no transaction was returned (e.g., BEEF_V2), find it in the BEEF document
	if tx == nil && beef != nil {
		tx = beef.FindTransaction(txid.String())
	}

	// Verify we got a transaction
	if tx == nil {
		return nil, errors.New("transaction " + txid.String() + " not found in BEEF")
	}

	// Verify the loaded transaction's txid matches what we requested
	loadedTxid := tx.TxID()
	if !loadedTxid.IsEqual(txid) {
		return nil, errors.New("loaded transaction txid mismatch: requested " + txid.String() + ", got " + loadedTxid.String())
	}

	return tx, nil
}

// LoadRawTx loads just the raw transaction bytes from the storage chain
func (s *Storage) LoadRawTx(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	for i, storage := range s.storages {
		rawTx, err := storage.GetRawTx(ctx, txid)
		if err == nil {
			// Cache back to earlier storages using Put with assembled BEEF
			if i > 0 {
				if beefBytes, err := assembleBEEF(txid, rawTx, nil); err == nil {
					for j := 0; j < i; j++ {
						s.storages[j].Put(ctx, txid, beefBytes)
					}
				}
			}
			return rawTx, nil
		}
		if err != ErrNotFound {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

// LoadProof loads just the merkle proof bytes from the storage chain
func (s *Storage) LoadProof(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	for i, storage := range s.storages {
		proof, err := storage.GetProof(ctx, txid)
		if err == nil {
			// Cache back to earlier storages - need rawTx to assemble BEEF
			if i > 0 {
				if rawTx, err := storage.GetRawTx(ctx, txid); err == nil {
					if beefBytes, err := assembleBEEF(txid, rawTx, proof); err == nil {
						for j := 0; j < i; j++ {
							s.storages[j].Put(ctx, txid, beefBytes)
						}
					}
				}
			}
			return proof, nil
		}
		if err != ErrNotFound {
			return nil, err
		}
	}
	return nil, ErrNotFound
}

// expandHomePath expands ~ to home directory if the path starts with ~/
func expandHomePath(path string) (string, error) {
	if strings.HasPrefix(path, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get home directory: %w", err)
		}
		return filepath.Join(homeDir, path[2:]), nil
	}
	return path, nil
}

// parseConnectionString parses a connection string and creates the storage chain
func parseConnectionString(connectionString string) ([]BaseBeefStorage, error) {
	// Parse connection string to determine if it's a single string or array
	var connectionStrings []string

	if connectionString != "" {
		// First try to parse as JSON array
		if strings.HasPrefix(strings.TrimSpace(connectionString), "[") {
			if err := json.Unmarshal([]byte(connectionString), &connectionStrings); err != nil {
				return nil, fmt.Errorf("invalid JSON array for BEEF storage: %w", err)
			}
		} else if strings.Contains(connectionString, ",") {
			// If it contains commas, split it
			connectionStrings = strings.Split(connectionString, ",")
			// Trim whitespace from each element
			for i, s := range connectionStrings {
				connectionStrings[i] = strings.TrimSpace(s)
			}
		} else {
			// Single connection string
			connectionStrings = []string{connectionString}
		}
	}

	// Create BEEF storage from connection strings (defaults to ~/.1sat/beef/ if not set)
	if len(connectionStrings) == 0 {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			connectionStrings = []string{"./beef/"} // Fallback
		} else {
			dotOneSatDir := filepath.Join(homeDir, ".1sat")
			if err := os.MkdirAll(dotOneSatDir, 0755); err != nil {
				connectionStrings = []string{"./beef/"} // Fallback if can't create dir
			} else {
				connectionStrings = []string{filepath.Join(dotOneSatDir, "beef")}
			}
		}
	}

	// Build the storage array
	var storages []BaseBeefStorage

	for _, connectionString := range connectionStrings {
		connectionString = strings.TrimSpace(connectionString)

		var storage BaseBeefStorage
		var err error

		// Create the appropriate storage
		switch {
		case strings.HasPrefix(connectionString, "lru://"):
			// Parse size from query parameter: lru://?size=100mb
			u, err := url.Parse(connectionString)
			if err != nil {
				return nil, fmt.Errorf("invalid LRU URL format: %w", err)
			}

			sizeStr := u.Query().Get("size")
			if sizeStr == "" {
				return nil, fmt.Errorf("LRU size not specified, use format: lru://?size=100mb")
			}

			size, err := ParseSize(sizeStr)
			if err != nil {
				return nil, fmt.Errorf("invalid LRU size format %s: %w", sizeStr, err)
			}
			storage = NewLRUBeefStorage(size)

		case strings.HasPrefix(connectionString, "redis://"):
			storage, err = NewRedisBeefStorage(connectionString)
			if err != nil {
				return nil, err
			}

		case strings.HasPrefix(connectionString, "s3://"):
			// Parse S3 URL: s3://[access-key:secret-key@]bucket-name/?region=us-west-2&endpoint=https://s3.amazonaws.com
			u, err := url.Parse(connectionString)
			if err != nil {
				return nil, fmt.Errorf("invalid S3 URL format: %w", err)
			}

			// Extract credentials if present
			var accessKey, secretKey string
			if u.User != nil {
				accessKey = u.User.Username()
				secretKey, _ = u.User.Password()
			}

			// Extract bucket from host
			bucket := u.Host

			// Check for custom endpoint (for MinIO/S3-compatible)
			endpoint := u.Query().Get("endpoint")
			region := u.Query().Get("region")

			// Create S3 client configuration
			if endpoint != "" || region != "" || accessKey != "" {
				// Custom configuration needed
				cfg, err := CreateS3Config(endpoint, region, accessKey, secretKey)
				if err != nil {
					return nil, fmt.Errorf("failed to create S3 config: %w", err)
				}
				client := NewS3ClientFromConfig(cfg)
				storage = NewS3BeefStorageWithClient(client, bucket)
			} else {
				// Use default AWS configuration
				storage, err = NewS3BeefStorage(bucket)
				if err != nil {
					return nil, err
				}
			}

		case strings.HasPrefix(connectionString, "junglebus"):
			// Supports: junglebus://, junglebus+http://, junglebus+https://
			parts := strings.SplitN(connectionString, "://", 2)
			schemeParts := strings.SplitN(parts[0], "+", 2)

			scheme := "https"
			if len(schemeParts) > 1 && schemeParts[1] == "http" {
				scheme = "http"
			}

			var junglebusURL string
			if len(parts) > 1 && parts[1] != "" {
				junglebusURL = scheme + "://" + parts[1]
			}
			// Empty junglebusURL will use env var or default in NewJunglebusBeefStorage
			storage = NewJunglebusBeefStorage(junglebusURL)

		case strings.HasPrefix(connectionString, "sqlite://"):
			// Remove sqlite:// prefix (can be sqlite:// or sqlite:///path)
			path := strings.TrimPrefix(connectionString, "sqlite://")
			path = strings.TrimPrefix(path, "/") // Handle sqlite:///path format
			if path == "" {
				path = "./beef.db"
			}
			// Expand ~ to home directory
			expandedPath, err := expandHomePath(path)
			if err != nil {
				return nil, err
			}
			storage, err = NewSQLiteBeefStorage(expandedPath)
			if err != nil {
				return nil, err
			}

		case strings.HasPrefix(connectionString, "file://"):
			// Remove file:// prefix
			path := strings.TrimPrefix(connectionString, "file://")
			if path == "" {
				path = "./beef"
			}
			// Expand ~ to home directory
			expandedPath, err := expandHomePath(path)
			if err != nil {
				return nil, err
			}
			storage, err = NewFilesystemBeefStorage(expandedPath)
			if err != nil {
				return nil, err
			}

		case strings.HasSuffix(connectionString, ".db"), strings.HasSuffix(connectionString, ".sqlite"):
			// Looks like a SQLite database file
			// Expand ~ to home directory
			expandedPath, err := expandHomePath(connectionString)
			if err != nil {
				return nil, err
			}
			storage, err = NewSQLiteBeefStorage(expandedPath)
			if err != nil {
				return nil, err
			}

		case filepath.IsAbs(connectionString) || strings.HasPrefix(connectionString, "./") || strings.HasPrefix(connectionString, "../") || strings.HasPrefix(connectionString, "~/"):
			// Looks like a filesystem path
			// Expand ~ to home directory
			expandedPath, err := expandHomePath(connectionString)
			if err != nil {
				return nil, err
			}

			// If it ends with a known DB extension, treat as SQLite
			if strings.HasSuffix(expandedPath, ".db") || strings.HasSuffix(expandedPath, ".sqlite") {
				storage, err = NewSQLiteBeefStorage(expandedPath)
				if err != nil {
					return nil, err
				}
			} else {
				// Otherwise treat as filesystem storage directory
				storage, err = NewFilesystemBeefStorage(expandedPath)
				if err != nil {
					return nil, err
				}
			}

		default:
			return nil, fmt.Errorf("unable to determine storage type from connection string: %s", connectionString)
		}

		storages = append(storages, storage)
	}

	if len(storages) == 0 {
		return nil, fmt.Errorf("no valid storage configurations provided")
	}

	return storages, nil
}
