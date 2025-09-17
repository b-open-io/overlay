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

type LRUBeefStorage struct {
	maxBytes    int64        // Maximum total size in bytes
	currentSize atomic.Int64 // Current total size in bytes (atomic for lock-free reads)
	cache       map[chainhash.Hash][]byte
	lruIndex    map[chainhash.Hash]*list.Element // Maps txid to list element for O(1) access
	lru         *list.List                       // List of chainhash.Hash for LRU ordering
	mu          sync.RWMutex
	fallback    BeefStorage
}

// NewLRUBeefStorage creates a new LRU cache with the specified maximum size in bytes
func NewLRUBeefStorage(maxBytes int64, fallback BeefStorage) *LRUBeefStorage {
	return &LRUBeefStorage{
		maxBytes: maxBytes,
		cache:    make(map[chainhash.Hash][]byte),
		lruIndex: make(map[chainhash.Hash]*list.Element),
		lru:      list.New(),
		fallback: fallback,
	}
}

func (t *LRUBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// Try to get from cache
	t.mu.Lock()
	if beefBytes, found := t.cache[*txid]; found {
		// Move to front (most recently used) using the index
		if elem, exists := t.lruIndex[*txid]; exists {
			t.lru.MoveToFront(elem)
		}
		t.mu.Unlock()
		// Return a copy to avoid external modifications
		result := make([]byte, len(beefBytes))
		copy(result, beefBytes)
		return result, nil
	}
	t.mu.Unlock()

	// Not in cache, try fallback
	if t.fallback != nil {
		beefBytes, err := t.fallback.LoadBeef(ctx, txid)
		if err == nil {
			// Add to cache
			t.addToCache(*txid, beefBytes)
		}
		return beefBytes, err
	}

	return nil, ErrNotFound
}

func (t *LRUBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	// Add to cache
	t.addToCache(*txid, beefBytes)

	// Also save to fallback if available
	if t.fallback != nil {
		return t.fallback.SaveBeef(ctx, txid, beefBytes)
	}

	return nil
}

func (t *LRUBeefStorage) addToCache(txid chainhash.Hash, beefBytes []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	beefSize := int64(len(beefBytes))

	// Check if already exists
	if oldBeef, found := t.cache[txid]; found {
		// Update and move to front
		if elem, exists := t.lruIndex[txid]; exists {
			t.lru.MoveToFront(elem)
		}
		oldSize := int64(len(oldBeef))

		// Update size tracking atomically
		sizeDelta := beefSize - oldSize
		t.currentSize.Add(sizeDelta)

		// Make a copy and update the cache
		beefCopy := make([]byte, len(beefBytes))
		copy(beefCopy, beefBytes)
		t.cache[txid] = beefCopy

		// Still need to check capacity in case the new value is larger
		t.evictIfNeeded()
		return
	}

	// Make a copy of the beef bytes to avoid external modifications
	beefCopy := make([]byte, len(beefBytes))
	copy(beefCopy, beefBytes)

	// Add new entry
	elem := t.lru.PushFront(txid)
	t.cache[txid] = beefCopy
	t.lruIndex[txid] = elem
	t.currentSize.Add(beefSize)

	// Evict oldest entries if over capacity
	t.evictIfNeeded()
}

func (t *LRUBeefStorage) evictIfNeeded() {
	for t.currentSize.Load() > t.maxBytes && t.lru.Len() > 0 {
		oldest := t.lru.Back()
		if oldest != nil {
			txid := oldest.Value.(chainhash.Hash)

			// Get the size of the beef we're about to evict
			if beefBytes, exists := t.cache[txid]; exists {
				t.currentSize.Add(-int64(len(beefBytes)))
				delete(t.cache, txid)
			}

			// Remove from LRU list and index
			t.lru.Remove(oldest)
			delete(t.lruIndex, txid)
		}
	}
}

// Stats returns cache statistics
func (t *LRUBeefStorage) Stats() (currentBytes int64, maxBytes int64, entryCount int) {
	// currentSize can be read atomically without a lock
	currentBytes = t.currentSize.Load()
	maxBytes = t.maxBytes
	
	// Only need lock for reading the LRU list length
	t.mu.RLock()
	entryCount = t.lru.Len()
	t.mu.RUnlock()
	
	return currentBytes, maxBytes, entryCount
}

// ParseSize parses size strings like "100mb", "1gb", "512KB" into bytes
func ParseSize(sizeStr string) (int64, error) {
	sizeStr = strings.ToLower(strings.TrimSpace(sizeStr))

	// Extract number and unit
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

	// If no unit found, assume bytes
	if unit == "" {
		return int64(number), nil
	}

	// Convert to bytes based on unit
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

// Close closes the fallback storage and clears the LRU cache
func (lru *LRUBeefStorage) Close() error {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	
	// Clear cache
	lru.cache = make(map[chainhash.Hash][]byte)
	lru.lruIndex = make(map[chainhash.Hash]*list.Element)
	lru.lru.Init()
	lru.currentSize.Store(0)
	
	// Close fallback
	if lru.fallback != nil {
		return lru.fallback.Close()
	}
	return nil
}

// UpdateMerklePath updates the merkle path for a transaction by delegating to the fallback
func (lru *LRUBeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	if lru.fallback != nil {
		beefBytes, err := lru.fallback.UpdateMerklePath(ctx, txid)
		if err == nil && len(beefBytes) > 0 {
			// Update our own storage with the new beef
			lru.SaveBeef(ctx, txid, beefBytes)
		}
		return beefBytes, err
	}
	return nil, ErrNotFound
}
