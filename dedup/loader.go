package dedup

import (
	"sync"
)

// Operation represents an in-flight operation with result sharing
type Operation[T any] struct {
	wg     *sync.WaitGroup
	result T
	err    error
}

// Loader provides deduplication for concurrent load operations by key
type Loader[K comparable, T any] struct {
	loader     func(K) (T, error)
	operations sync.Map // map[K]*Operation[T]
}

// NewLoader creates a new deduplicated loader with the specified worker function
func NewLoader[K comparable, T any](loader func(K) (T, error)) *Loader[K, T] {
	return &Loader[K, T]{
		loader: loader,
	}
}

// Load executes the loader function for the given key, deduplicating concurrent calls.
// If another goroutine is already loading the same key, this call will wait
// for that result instead of executing the loader again.
func (d *Loader[K, T]) Load(key K) (T, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	// Check if there's already an in-flight operation for this key
	if inflight, loaded := d.operations.LoadOrStore(key, &Operation[T]{wg: &wg}); loaded {
		op := inflight.(*Operation[T])
		op.wg.Wait()
		return op.result, op.err
	} else {
		op := inflight.(*Operation[T])
		
		// Execute the loader function
		op.result, op.err = d.loader(key)

		// Clean up operations map
		d.operations.Delete(key)
		return op.result, op.err
	}
}

// Clear removes all inflight operations (useful for cleanup)
func (d *Loader[K, T]) Clear() {
	d.operations.Range(func(key, value interface{}) bool {
		d.operations.Delete(key)
		return true
	})
}