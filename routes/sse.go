package routes

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
	"github.com/gofiber/fiber/v2"
)

// CatchupFunc retrieves historical events for SSE catchup when resuming from a score.
// It should return events from all provided keys merged in score order.
type CatchupFunc func(ctx context.Context, keys []string, fromScore float64) ([]queue.ScoredMember, error)

// SSERoutesConfig holds the configuration for SSE streaming routes
type SSERoutesConfig struct {
	SSEManager *pubsub.SSEManager
	Catchup    CatchupFunc
	Context    context.Context
}

// RegisterSSERoutes registers Server-Sent Events streaming routes
func RegisterSSERoutes(group fiber.Router, config *SSERoutesConfig) {
	if config == nil || config.SSEManager == nil || config.Context == nil {
		log.Fatal("RegisterSSERoutes: config, SSEManager, and context are required")
	}

	sseManager := config.SSEManager
	catchup := config.Catchup
	ctx := config.Context

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
			// If resuming and catchup function provided, send missed events
			if fromScore > 0 && catchup != nil {
				members, err := catchup(ctx, events, fromScore)
				if err != nil {
					log.Printf("Catchup error: %v", err)
				} else {
					for _, member := range members {
						fmt.Fprintf(w, "data: %s\n", member.Member)
						fmt.Fprintf(w, "id: %.0f\n\n", member.Score)
						if err := w.Flush(); err != nil {
							return // Connection closed
						}
					}
				}
			}

			// Send initial connection message
			fmt.Fprintf(w, "data: Connected to events: %s\n\n", strings.Join(events, ", "))
			if err := w.Flush(); err != nil {
				return // Connection closed
			}

			// Register client for all requested topics
			clientID := sseManager.RegisterClient(events, w)

			// Cleanup function - remove client when connection closes
			defer func() {
				sseManager.DeregisterClient(clientID)
				close(clientDone)
			}()

			// Keep connection alive with periodic pings
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					// Send ping to keep connection alive
					fmt.Fprintf(w, ": ping\n\n")
					if err := w.Flush(); err != nil {
						return // Connection closed
					}
				case <-ctx.Done():
					return
				}
			}
		})

		return nil
	})
}
