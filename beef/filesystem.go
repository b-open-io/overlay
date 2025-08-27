package beef

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

type FilesystemBeefStorage struct {
	basePath string
	fallback BeefStorage
}

func NewFilesystemBeefStorage(basePath string, fallback BeefStorage) (*FilesystemBeefStorage, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &FilesystemBeefStorage{
		basePath: basePath,
		fallback: fallback,
	}, nil
}

// getFilePath returns the file path for a given txid
// Uses subdirectories based on first 2 chars of txid to avoid too many files in one directory
func (t *FilesystemBeefStorage) getFilePath(txid *chainhash.Hash) string {
	txidStr := txid.String()
	// Create subdirectory structure: basePath/xx/xxxxxxxxxxxx.beef
	subDir := txidStr[:2]
	fileName := txidStr + ".beef"
	return filepath.Join(t.basePath, subDir, fileName)
}

func (t *FilesystemBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	filePath := t.getFilePath(txid)

	// Try to load from filesystem first
	beefBytes, err := os.ReadFile(filePath)
	if err == nil {
		return beefBytes, nil
	}

	// If file doesn't exist, try fallback
	if os.IsNotExist(err) {
		if t.fallback != nil {
			beefBytes, err = t.fallback.LoadBeef(ctx, txid)
			if err == nil {
				// Save to filesystem for future use
				t.SaveBeef(ctx, txid, beefBytes)
			}
			return beefBytes, err
		}
		return nil, ErrNotFound
	}

	// For other errors, return them
	return nil, fmt.Errorf("failed to read file: %w", err)
}

func (t *FilesystemBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	filePath := t.getFilePath(txid)

	// Create subdirectory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write file atomically by writing to temp file first then renaming
	tempFile := filePath + ".tmp"
	if err := os.WriteFile(tempFile, beefBytes, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Rename temp file to final name (atomic on most filesystems)
	if err := os.Rename(tempFile, filePath); err != nil {
		// Clean up temp file if rename fails
		os.Remove(tempFile)
		return fmt.Errorf("failed to rename file: %w", err)
	}

	return nil
}

// Close closes the fallback storage (filesystem storage itself needs no cleanup)
func (f *FilesystemBeefStorage) Close() error {
	if f.fallback != nil {
		return f.fallback.Close()
	}
	return nil
}
