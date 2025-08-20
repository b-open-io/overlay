package routes

import (
	"context"
	"log"
	"strings"

	"github.com/b-open-io/overlay/pubsub"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/gofiber/fiber/v2"
)

// SubmitRoutesConfig holds the configuration for enhanced submit routes
type SubmitRoutesConfig struct {
	Engine          *engine.Engine
	LibP2PSync      *pubsub.LibP2PSync     // Optional LibP2P sync for transaction broadcasting - can be nil
	PeerBroadcaster *pubsub.PeerBroadcaster // Optional HTTP peer broadcaster configured with peer-topic mapping - can be nil
}

// RegisterSubmitRoutes registers enhanced submit routes with peer broadcasting and LibP2P support.
// The caller is responsible for setting up the LibP2PSync and PeerBroadcaster instances
// with the appropriate peer configurations before calling this function.
func RegisterSubmitRoutes(app *fiber.App, config *SubmitRoutesConfig) {
	if config == nil || config.Engine == nil {
		log.Fatal("RegisterSubmitRoutes: config and engine are required")
	}

	e := config.Engine
	libp2pSync := config.LibP2PSync
	peerBroadcaster := config.PeerBroadcaster

	// Enhanced submit route with peer broadcasting
	app.Post("/api/v1/submit", func(c *fiber.Ctx) error {
		// Parse x-topics header
		topicsHeader := c.Get("x-topics")
		if topicsHeader == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "x-topics header is required",
			})
		}

		topics := strings.Split(topicsHeader, ",")
		for i, topic := range topics {
			topics[i] = strings.TrimSpace(topic)
		}

		// Read BEEF body
		beef := c.Body()
		if len(beef) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "BEEF transaction data is required",
			})
		}

		// Create TaggedBEEF
		taggedBEEF := overlay.TaggedBEEF{
			Beef:   beef,
			Topics: topics,
		}

		// Submit to local engine
		steak, err := e.Submit(c.Context(), taggedBEEF, engine.SubmitModeCurrent, nil)
		if err != nil {
			log.Printf("Failed to submit transaction locally: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": "Failed to process transaction",
			})
		}

		// Get successfully processed topics for broadcasting
		if len(steak) > 0 {
			successfulTopics := make([]string, 0, len(steak))
			for topic := range steak {
				successfulTopics = append(successfulTopics, topic)
			}

			// Publish to LibP2P network if enabled
			if libp2pSync != nil {
				go func() {
					// Parse BEEF to get transaction ID for publishing
					_, tx, _, err := transaction.ParseBeef(beef)
					if err != nil {
						log.Printf("Failed to parse BEEF for LibP2P publishing: %v", err)
						return
					}

					// Publish txid to each successful topic
					for topic := range steak {
						if err := libp2pSync.PublishTxid(context.Background(), topic, *tx.TxID()); err != nil {
							log.Printf("Failed to publish txid to LibP2P topic %s: %v", topic, err)
						} else {
							log.Printf("Published txid %s to LibP2P topic %s", tx.TxID().String(), topic)
						}
					}
				}()
			}

			// Broadcast to HTTP peers if configured
			if peerBroadcaster != nil {
				// Only broadcast topics that were successfully processed
				broadcastBEEF := overlay.TaggedBEEF{
					Beef:   beef,
					Topics: successfulTopics,
				}

				go func() {
					if err := peerBroadcaster.BroadcastTransaction(context.Background(), broadcastBEEF); err != nil {
						log.Printf("Failed to broadcast transaction to peers: %v", err)
					}
				}()
			}
		}

		// Return success response (matching overlay service format)
		return c.JSON(fiber.Map{
			"description": "Overlay engine successfully processed the submitted transaction",
			"steak":       steak,
		})
	})
}