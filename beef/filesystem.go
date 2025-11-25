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
}

func NewFilesystemBeefStorage(basePath string) (*FilesystemBeefStorage, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &FilesystemBeefStorage{
		basePath: basePath,
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

func (t *FilesystemBeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	filePath := t.getFilePath(txid)

	beefBytes, err := os.ReadFile(filePath)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return beefBytes, nil
}

func (t *FilesystemBeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
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

// UpdateMerklePath is not supported by filesystem storage
func (f *FilesystemBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return nil, nil
}

// Close is a no-op for filesystem storage
func (f *FilesystemBeefStorage) Close() error {
	return nil
}
