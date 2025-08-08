// Package processor provides transaction processing infrastructure for overlay services.
//
// The processor package implements a queue-based transaction processing system that
// enables scalable, concurrent processing of Bitcoin transactions. It's designed to
// work with Redis queues and support various processing patterns.
//
// Key Components:
//
// TransactionProcessor Interface:
//   - Defines how individual transactions are processed
//   - Returns topics/tokens that a transaction belongs to
//   - Implementations handle protocol-specific parsing
//
// QueueProcessor:
//   - Manages concurrent workers for processing transactions
//   - Handles batch processing for efficiency
//   - Provides retry mechanisms for failed transactions
//   - Supports graceful shutdown
//
// Configuration Options:
//   - Concurrency: Number of parallel workers
//   - BatchSize: Transactions per batch
//   - ProcessingTimeout: Maximum time per transaction
//   - PollInterval: Queue polling frequency
//   - RetryLimit: Maximum retry attempts
//   - DumpFailuresDir: Where to save failed transactions
//
// Example Usage:
//
//	// Define your transaction processor
//	type MyProcessor struct {
//	    storage Storage
//	    lookup  EventLookup
//	}
//	
//	func (p *MyProcessor) ProcessTransaction(ctx context.Context, txid *chainhash.Hash) ([]string, error) {
//	    // Parse transaction and extract protocol data
//	    // Return relevant topics
//	    return []string{"token1", "token2"}, nil
//	}
//	
//	// Create queue processor
//	config := &ProcessorConfig{
//	    QueueName:    "tx-queue",
//	    Concurrency:  10,
//	    BatchSize:    100,
//	    RetryLimit:   3,
//	}
//	
//	qp := NewQueueProcessor(redisClient, myProcessor, config)
//	
//	// Start processing
//	ctx := context.Background()
//	if err := qp.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
// Queue Management:
//
// The processor uses Redis lists for queue management:
//   - Main queue: Incoming transactions
//   - Processing queue: Currently being processed
//   - Failed queue: Transactions that failed after retries
//
// Monitoring:
//
// The processor provides metrics through:
//   - GetStats(): Current processing statistics
//   - Log output: Processing progress and errors
//
// Error Handling:
//
// Failed transactions are:
//   1. Retried up to RetryLimit times
//   2. Moved to failed queue if retries exhausted
//   3. Optionally dumped to disk for manual recovery
package processor