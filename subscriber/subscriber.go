package subscriber

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/GorillaPool/go-junglebus"
	"github.com/GorillaPool/go-junglebus/models"
	"github.com/redis/go-redis/v9"
)

// SubscriberConfig holds configuration for a JungleBus subscriber
type SubscriberConfig struct {
	// Topic ID to subscribe to
	TopicID string
	
	// Queue name in Redis where transactions will be stored
	QueueName string
	
	// Starting block and page for subscription
	FromBlock uint64
	FromPage  uint64
	
	// JungleBus subscription options
	QueueSize uint64
	LiteMode  bool
}

// Subscriber manages JungleBus subscriptions with Redis queue integration
type Subscriber struct {
	config *SubscriberConfig
	jb     *junglebus.Client
	rdb    *redis.Client
	sub    *junglebus.Subscription
}

// NewSubscriber creates a new subscriber with the given configuration and connections
func NewSubscriber(cfg *SubscriberConfig, redisClient *redis.Client, jbClient *junglebus.Client) *Subscriber {
	return &Subscriber{
		config: cfg,
		jb:     jbClient,
		rdb:    redisClient,
	}
}

// Start begins the subscription and blocks until context is cancelled or signal received
func (s *Subscriber) Start(ctx context.Context) error {
	// Check for existing progress
	fromBlock := s.config.FromBlock
	fromPage := s.config.FromPage
	
	if progress, err := s.rdb.HGet(ctx, "progress", s.config.TopicID).Int(); err == nil {
		fromBlock = uint64(progress)
		log.Printf("Resuming %s from block %d", s.config.TopicID, fromBlock)
	}
	
	txcount := 0
	log.Printf("Subscribing to JungleBus topic %s from block %d, page %d", s.config.TopicID, fromBlock, fromPage)
	
	// Create subscription
	var sub *junglebus.Subscription
	var err error
	sub, err = s.jb.SubscribeWithQueue(ctx,
		s.config.TopicID,
		fromBlock,
		fromPage,
		junglebus.EventHandler{
			OnTransaction: func(txn *models.TransactionResponse) {
				txcount++
				log.Printf("[TX]: %d - %d: %d %s", txn.BlockHeight, txn.BlockIndex, len(txn.Transaction), txn.Id)
				
				// Add to Redis queue with score based on block height and index
				if err := s.rdb.ZAdd(ctx, s.config.QueueName, redis.Z{
					Member: txn.Id,
					Score:  float64(txn.BlockHeight)*1e9 + float64(txn.BlockIndex),
				}).Err(); err != nil {
					log.Printf("Failed to add transaction to queue: %v", err)
				}
			},
			OnStatus: func(status *models.ControlResponse) {
				log.Printf("[STATUS]: %d %v %d processed", status.StatusCode, status.Message, txcount)
				switch status.StatusCode {
				case 200:
					// Update progress
					if err := s.rdb.HSet(ctx, "progress", s.config.TopicID, status.Block+1).Err(); err != nil {
						log.Printf("Failed to update progress: %v", err)
					}
					txcount = 0
				case 999:
					log.Println(status.Message)
					log.Println("Subscription completed, unsubscribing...")
					if sub != nil {
						sub.Unsubscribe()
					}
					return
				}
			},
			OnError: func(err error) {
				log.Printf("[ERROR]: %v", err)
			},
		},
		&junglebus.SubscribeOptions{
			QueueSize: uint32(s.config.QueueSize),
			LiteMode:  s.config.LiteMode,
		},
	)
	
	if err != nil {
		return err
	}
	
	s.sub = sub
	
	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	select {
	case <-ctx.Done():
		log.Println("Context cancelled, unsubscribing...")
	case sig := <-sigChan:
		log.Printf("Received signal %v, unsubscribing...", sig)
	}
	
	if s.sub != nil {
		s.sub.Unsubscribe()
	}
	return ctx.Err()
}

// Stop gracefully stops the subscription
func (s *Subscriber) Stop() {
	if s.sub != nil {
		s.sub.Unsubscribe()
	}
}

// GetProgress returns the current progress for the topic
func (s *Subscriber) GetProgress(ctx context.Context) (uint64, error) {
	progress, err := s.rdb.HGet(ctx, "progress", s.config.TopicID).Uint64()
	if err == redis.Nil {
		return s.config.FromBlock, nil
	}
	return progress, err
}

// DefaultSubscriberConfig returns a default configuration
func DefaultSubscriberConfig() *SubscriberConfig {
	return &SubscriberConfig{
		QueueSize: 10000000,
		LiteMode:  true,
		FromBlock: 0,
		FromPage:  0,
	}
}