// Package subscriber provides JungleBus subscription management for overlay services.
//
// The subscriber package handles the connection to JungleBus, receives blockchain
// transactions in real-time, and queues them for processing. It's the entry point
// for transaction data in overlay services.
//
// JungleBus Integration:
//
// JungleBus is a Bitcoin transaction subscription service that provides:
//   - Real-time transaction streaming
//   - Topic-based filtering
//   - Resumable subscriptions with block/page tracking
//   - Both full and lite modes for different use cases
//
// Key Components:
//
// Subscriber:
//   - Manages JungleBus client connection
//   - Handles transaction callbacks
//   - Queues transactions in Redis for processing
//   - Tracks subscription progress
//   - Provides graceful shutdown
//
// Configuration:
//   - TopicID: JungleBus topic to subscribe to
//   - QueueName: Redis queue for storing transactions
//   - FromBlock/FromPage: Resume point for subscriptions
//   - QueueSize: Buffer size for incoming transactions
//   - LiteMode: Whether to receive lite transactions
//
// Example Usage:
//
//	// Create subscriber configuration
//	config := &SubscriberConfig{
//	    TopicID:   "your-topic-id",
//	    QueueName: "tx-queue",
//	    FromBlock: 850000,
//	    FromPage:  0,
//	    QueueSize: 1000,
//	    LiteMode:  false,
//	}
//	
//	// Create Redis client
//	redisClient := redis.NewClient(&redis.Options{
//	    Addr: "localhost:6379",
//	})
//	
//	// Create and start subscriber
//	subscriber := NewSubscriber(redisClient, config)
//	
//	ctx := context.Background()
//	if err := subscriber.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	
//	// Subscriber will run until context is cancelled
//	// or shutdown signal is received
//
// Transaction Flow:
//
// 1. JungleBus sends transactions matching the topic
// 2. Subscriber receives transaction callback
// 3. Transaction ID is extracted and queued in Redis
// 4. Progress (block/page) is saved for resumption
// 5. Processor picks up transactions from queue
//
// Progress Tracking:
//
// The subscriber saves progress to Redis, enabling:
//   - Resumption after restarts
//   - No duplicate processing
//   - Efficient catch-up after downtime
//
// Error Handling:
//
// The subscriber handles:
//   - JungleBus connection failures with reconnection
//   - Redis queue failures with retries
//   - Graceful shutdown on signals (SIGINT, SIGTERM)
//
// Monitoring:
//
// Progress and errors are logged for monitoring:
//   - Current block/page being processed
//   - Queue status
//   - Connection health
package subscriber