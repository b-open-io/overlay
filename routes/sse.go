package routes

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/storage"
	"github.com/gofiber/fiber/v2"
)

// SSERoutesConfig holds the configuration for SSE streaming routes
type SSERoutesConfig struct {
	Storage *storage.EventDataStorage
	Context context.Context
}

// Global shared SSE manager
var sharedSSEManager *pubsub.SSEManager

// RegisterSSERoutes registers Server-Sent Events streaming routes
func RegisterSSERoutes(group fiber.Router, config *SSERoutesConfig) {
	if config == nil || config.Storage == nil || config.Context == nil {
		log.Fatal("RegisterSSERoutes: config, storage, and context are required")
	}

	store := config.Storage
	ctx := config.Context

	// Initialize shared SSE manager once with server context
	if sharedSSEManager == nil {
		if ps := store.GetPubSub(); ps != nil {
			sharedSSEManager = pubsub.NewSSEManager(ctx, ps)
		}
	}

	// SSE subscription route
	group.Get("/subscribe/:events", func(c *fiber.Ctx) error {
		events := strings.Split(c.Params("events"), ",")
		log.Printf("Subscription request for events: %v", events)

		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("Transfer-Encoding", "chunked")
		c.Set("X-Accel-Buffering", "no")
		c.Set("Access-Control-Allow-Origin", "*")

		// Check for Last-Event-ID header for resumption
		lastEventID := c.Get("Last-Event-ID")
		var fromScore float64
		if lastEventID != "" {
			if score, err := strconv.ParseFloat(lastEventID, 64); err == nil {
				fromScore = score
				log.Printf("Resuming from score: %f", fromScore)
			}
		}

		// Create a channel for this client to signal when to close
		clientDone := make(chan struct{})

		c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
			// If resuming, first send any missed events using storage
			if fromScore > 0 {
				// For each event, get recent events since the last score
				for _, event := range events {
					// For BSV21, if event starts with "tm_", it's a topic, otherwise extract tokenId
					var topic string
					if strings.HasPrefix(event, "tm_") {
						topic = event
					} else {
						// Extract tokenId from event patterns like "p2pkh:address:tokenId"
						parts := strings.Split(event, ":")
						if len(parts) >= 3 {
							tokenId := parts[len(parts)-1] // Last part is tokenId
							topic = "tm_" + tokenId
						} else {
							continue // Skip malformed events
						}
					}

					if recentEvents, err := store.LookupEventScores(ctx, topic, event, fromScore); err == nil {
						// Send each missed event with its actual score
						for _, eventScore := range recentEvents {
							fmt.Fprintf(w, "data: %s\n", eventScore.Member)
							fmt.Fprintf(w, "id: %.0f\n\n", eventScore.Score)
							if err := w.Flush(); err != nil {
								return // Connection closed
							}
						}
					}
				}
			}

			// Send initial connection message
			fmt.Fprintf(w, "data: Connected to events: %s\n\n", strings.Join(events, ", "))
			if err := w.Flush(); err != nil {
				return // Connection closed
			}

			// Register this client with the shared SSE manager
			if sharedSSEManager != nil {
				// Register client for each event
				for _, event := range events {
					sharedSSEManager.AddSSEClient(event, w)
				}

				// Cleanup function - remove client when connection closes
				defer func() {
					for _, event := range events {
						sharedSSEManager.RemoveSSEClient(event, w)
					}
					close(clientDone)
				}()
			}

			// Keep connection alive - wait for context cancellation
			<-ctx.Done()
		})

		return nil
	})
}