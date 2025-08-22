package processor

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/redis/go-redis/v9"
)

// TransactionProcessor defines the interface for processing individual transactions
type TransactionProcessor interface {
	// ProcessTransaction processes a single transaction by its ID
	// Returns a list of topics/tokens that this transaction belongs to
	ProcessTransaction(ctx context.Context, txid *chainhash.Hash) ([]string, error)
}

// ProcessorConfig holds configuration for the queue processor
type ProcessorConfig struct {
	// Queue name in Redis to process
	QueueName string
	
	// Maximum number of concurrent workers
	Concurrency int
	
	// Maximum number of transactions to process in each batch
	BatchSize int64
	
	// Sleep duration when queue is empty
	EmptyQueueSleep time.Duration
}

// QueueProcessor processes transactions from a Redis queue
type QueueProcessor struct {
	config    *ProcessorConfig
	processor TransactionProcessor
	rdb       *redis.Client
}

// NewQueueProcessor creates a new queue processor
func NewQueueProcessor(cfg *ProcessorConfig, redisClient *redis.Client, processor TransactionProcessor) *QueueProcessor {
	return &QueueProcessor{
		config:    cfg,
		processor: processor,
		rdb:       redisClient,
	}
}

// Start begins processing the queue and blocks until context is cancelled
func (qp *QueueProcessor) Start(ctx context.Context) error {
	log.Printf("Starting queue processor for %s with concurrency %d", qp.config.QueueName, qp.config.Concurrency)
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := qp.processBatch(ctx); err != nil {
				log.Printf("Error processing batch: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// processBatch processes a single batch of transactions from the queue
func (qp *QueueProcessor) processBatch(ctx context.Context) error {
	// Get transactions from queue (sorted by score - block height + index)
	query := redis.ZRangeArgs{
		Key:     qp.config.QueueName,
		Start:   "-inf",
		Stop:    "+inf",
		ByScore: true,
		Count:   qp.config.BatchSize,
	}
	
	txids, err := qp.rdb.ZRangeArgs(ctx, query).Result()
	if err != nil {
		return err
	}
	
	if len(txids) == 0 {
		time.Sleep(qp.config.EmptyQueueSleep)
		return nil
	}
	
	log.Printf("Processing batch of %d transactions", len(txids))
	
	// Process transactions concurrently
	var wg sync.WaitGroup
	limiter := make(chan struct{}, qp.config.Concurrency)
	
	for _, txidStr := range txids {
		wg.Add(1)
		limiter <- struct{}{} // Acquire semaphore
		
		go func(txidStr string) {
			defer func() {
				wg.Done()
				<-limiter // Release semaphore
			}()
			
			if err := qp.processTransaction(ctx, txidStr); err != nil {
				log.Printf("Failed to process transaction %s: %v", txidStr, err)
			} else {
				// Remove from queue on success
				if err := qp.rdb.ZRem(ctx, qp.config.QueueName, txidStr).Err(); err != nil {
					log.Panicf("Critical queue cleanup failure: failed to remove transaction %s from queue: %v", txidStr, err)
				}
			}
		}(txidStr)
	}
	
	wg.Wait()
	return nil
}

// processTransaction processes a single transaction
func (qp *QueueProcessor) processTransaction(ctx context.Context, txidStr string) error {
	txid, err := chainhash.NewHashFromHex(txidStr)
	if err != nil {
		return err
	}
	
	// Process the transaction using the provided processor
	topics, err := qp.processor.ProcessTransaction(ctx, txid)
	if err != nil {
		return err
	}
	
	// Log which topics this transaction was processed for
	if len(topics) > 0 {
		log.Printf("Processed transaction %s for topics: %v", txidStr, topics)
	}
	
	return nil
}

// GetQueueLength returns the current length of the processing queue
func (qp *QueueProcessor) GetQueueLength(ctx context.Context) (int64, error) {
	return qp.rdb.ZCard(ctx, qp.config.QueueName).Result()
}

// DefaultProcessorConfig returns a default processor configuration
func DefaultProcessorConfig() *ProcessorConfig {
	return &ProcessorConfig{
		Concurrency:     16,
		BatchSize:       1000,
		EmptyQueueSleep: 1 * time.Second,
	}
}