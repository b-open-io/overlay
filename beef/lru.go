package beef

import (
	"container/list"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// lruEntry stores tx and optional proof together
type lruEntry struct {
	rawTx []byte
	proof []byte
}

type LRUBeefStorage struct {
	maxBytes    int64        // Maximum total size in bytes
	currentSize atomic.Int64 // Current total size in bytes (atomic for lock-free reads)
	cache       map[chainhash.Hash]*lruEntry
	lruIndex    map[chainhash.Hash]*list.Element // Maps txid to list element for O(1) access
	lru         *list.List                       // List of chainhash.Hash for LRU ordering
	mu          sync.RWMutex
}

// NewLRUBeefStorage creates a new LRU cache with the specified maximum size in bytes
func NewLRUBeefStorage(maxBytes int64) *LRUBeefStorage {
	return &LRUBeefStorage{
		maxBytes: maxBytes,
		cache:    make(map[chainhash.Hash]*lruEntry),
		lruIndex: make(map[chainhash.Hash]*list.Element),
		lru:      list.New(),
	}
}

func (t *LRUBeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, found := t.cache[*txid]
	if !found || entry.rawTx == nil {
		return nil, ErrNotFound
	}

	// Move to front (most recently used)
	if elem, exists := t.lruIndex[*txid]; exists {
		t.lru.MoveToFront(elem)
	}

	return assembleBEEF(txid, entry.rawTx, entry.proof)
}

func (t *LRUBeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	rawTx, proof, err := splitBEEF(beefBytes)
	if err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.putEntry(*txid, rawTx, proof)
	return nil
}

// putEntry stores tx and proof together (must be called with lock held)
func (t *LRUBeefStorage) putEntry(txid chainhash.Hash, rawTx, proof []byte) {
	newSize := int64(len(rawTx) + len(proof))

	if existing, found := t.cache[txid]; found {
		// Update existing entry
		oldSize := int64(len(existing.rawTx) + len(existing.proof))
		t.currentSize.Add(newSize - oldSize)

		existing.rawTx = copyBytes(rawTx)
		if len(proof) > 0 {
			existing.proof = copyBytes(proof)
		}

		if elem, exists := t.lruIndex[txid]; exists {
			t.lru.MoveToFront(elem)
		}
	} else {
		// New entry
		entry := &lruEntry{
			rawTx: copyBytes(rawTx),
		}
		if len(proof) > 0 {
			entry.proof = copyBytes(proof)
		}

		elem := t.lru.PushFront(txid)
		t.cache[txid] = entry
		t.lruIndex[txid] = elem
		t.currentSize.Add(newSize)
	}

	t.evictIfNeeded()
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func (t *LRUBeefStorage) evictIfNeeded() {
	for t.currentSize.Load() > t.maxBytes && t.lru.Len() > 0 {
		oldest := t.lru.Back()
		if oldest != nil {
			txid := oldest.Value.(chainhash.Hash)

			if entry, exists := t.cache[txid]; exists {
				t.currentSize.Add(-int64(len(entry.rawTx) + len(entry.proof)))
				delete(t.cache, txid)
			}

			t.lru.Remove(oldest)
			delete(t.lruIndex, txid)
		}
	}
}

// Stats returns cache statistics
func (t *LRUBeefStorage) Stats() (currentBytes int64, maxBytes int64, entryCount int) {
	currentBytes = t.currentSize.Load()
	maxBytes = t.maxBytes

	t.mu.RLock()
	entryCount = t.lru.Len()
	t.mu.RUnlock()

	return currentBytes, maxBytes, entryCount
}

// ParseSize parses size strings like "100mb", "1gb", "512KB" into bytes
func ParseSize(sizeStr string) (int64, error) {
	sizeStr = strings.ToLower(strings.TrimSpace(sizeStr))

	var number float64
	var unit string

	for i, r := range sizeStr {
		if (r < '0' || r > '9') && r != '.' {
			numStr := sizeStr[:i]
			unit = sizeStr[i:]

			var err error
			number, err = strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid number: %s", numStr)
			}
			break
		}
	}

	if unit == "" {
		n, err := strconv.ParseFloat(sizeStr, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size: %s", sizeStr)
		}
		return int64(n), nil
	}

	switch strings.TrimSpace(unit) {
	case "b", "byte", "bytes":
		return int64(number), nil
	case "kb", "kilobyte", "kilobytes":
		return int64(number * 1024), nil
	case "mb", "megabyte", "megabytes":
		return int64(number * 1024 * 1024), nil
	case "gb", "gigabyte", "gigabytes":
		return int64(number * 1024 * 1024 * 1024), nil
	case "tb", "terabyte", "terabytes":
		return int64(number * 1024 * 1024 * 1024 * 1024), nil
	default:
		return 0, fmt.Errorf("unknown size unit: %s", unit)
	}
}

// UpdateMerklePath is not supported by LRU storage
func (lru *LRUBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return nil, nil
}

// GetRawTx loads just the raw transaction bytes from cache
func (lru *LRUBeefStorage) GetRawTx(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	entry, found := lru.cache[*txid]
	if !found || entry.rawTx == nil {
		return nil, ErrNotFound
	}

	if elem, exists := lru.lruIndex[*txid]; exists {
		lru.lru.MoveToFront(elem)
	}

	return copyBytes(entry.rawTx), nil
}

// GetProof loads just the merkle proof bytes from cache
func (lru *LRUBeefStorage) GetProof(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	entry, found := lru.cache[*txid]
	if !found || entry.proof == nil {
		return nil, ErrNotFound
	}

	if elem, exists := lru.lruIndex[*txid]; exists {
		lru.lru.MoveToFront(elem)
	}

	return copyBytes(entry.proof), nil
}

// Close clears the LRU cache
func (lru *LRUBeefStorage) Close() error {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	lru.cache = make(map[chainhash.Hash]*lruEntry)
	lru.lruIndex = make(map[chainhash.Hash]*list.Element)
	lru.lru.Init()
	lru.currentSize.Store(0)

	return nil
}
