// Package publish provides event publishing capabilities for overlay services.
//
// The publish package enables real-time notifications when new data is available,
// allowing subscribers to react immediately to changes in the overlay network.
//
// Key Features:
//   - Topic-based publishing for event segregation
//   - Real-time event delivery
//   - Support for different backend implementations
//
// Currently Supported Backends:
//   - Redis: Uses Redis pub/sub for reliable message delivery
//
// Publisher Interface:
//
// The Publisher interface defines a simple contract:
//   - Publish(ctx, topic, data) - Send data to all subscribers of a topic
//
// Example Usage:
//
//	// Create a Redis publisher
//	publisher, err := publish.NewRedisPublisher("redis://localhost:6379")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	
//	// Publish BEEF data when a new transaction is processed
//	beefData := base64.StdEncoding.EncodeToString(beef)
//	err = publisher.Publish(ctx, "my-topic", beefData)
//	
//	// Publish custom event data
//	eventData := fmt.Sprintf("event:transfer,txid:%s", txid)
//	err = publisher.Publish(ctx, "events", eventData)
//
// Integration with Storage:
//
// Publishers are typically integrated with storage implementations to automatically
// notify subscribers when new outputs are stored. This enables real-time streaming
// of blockchain data to connected clients.
//
// Subscriber Patterns:
//
// Subscribers can use various patterns:
//   - Topic-specific subscriptions for filtered data
//   - Wildcard subscriptions for broader monitoring
//   - Multiple concurrent subscriptions for different data types
package publish