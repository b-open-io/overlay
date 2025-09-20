package routes

import (
	"context"
	"fmt"
	"time"

	"github.com/b-open-io/overlay/headers"
	"github.com/b-open-io/overlay/headers/processor"
	"github.com/b-open-io/overlay/storage"
	"github.com/gofiber/fiber/v2"
)

// WebhookConfig holds configuration for webhook routes
type WebhookConfig struct {
	HeadersClient *headers.Client
	EventStorage  *storage.EventDataStorage
	WebhookToken  string // The token we expect in webhook callbacks
}

// RegisterWebhookRoutes registers the headers webhook callback endpoint
// It should be called with a router like app.Group("/api/1sat")
func RegisterWebhookRoutes(router fiber.Router, config WebhookConfig) {
	// Webhook callback from block headers service
	router.Post("/headers/webhook", authenticateWebhook(config.WebhookToken), handleHeadersWebhook(config.HeadersClient, config.EventStorage))
}

// authenticateWebhook middleware validates the webhook auth token
func authenticateWebhook(expectedToken string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// If no token is configured, skip authentication
		if expectedToken == "" {
			return c.Next()
		}

		// Check Authorization header
		authHeader := c.Get("Authorization")
		expectedAuth := "Bearer " + expectedToken

		if authHeader != expectedAuth {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "unauthorized",
			})
		}

		return c.Next()
	}
}

// HeadersWebhookEvent represents the event payload from block headers service
type HeadersWebhookEvent struct {
	Operation string       `json:"operation"` // "ADD" is the only operation
	Header    HeaderDetail `json:"header"`
}

// HeaderDetail contains the header information in the webhook
type HeaderDetail struct {
	Height        int32  `json:"height"`
	Hash          string `json:"hash"`
	Version       int32  `json:"version"`
	MerkleRoot    string `json:"merkleRoot"`
	Timestamp     string `json:"creationTimestamp"`
	Nonce         uint32 `json:"nonce"`
	State         string `json:"state"` // "LONGEST_CHAIN", "STALE", "ORPHAN", "REJECTED"
	PreviousBlock string `json:"prevBlockHash"`
}

// handleHeadersWebhook processes webhook callbacks from the block headers service
func handleHeadersWebhook(headersClient *headers.Client, eventStorage *storage.EventDataStorage) fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Parse the webhook event
		var event HeadersWebhookEvent
		if err := c.BodyParser(&event); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "invalid event payload",
			})
		}

		// Log the event
		fmt.Printf("Received headers webhook: operation=%s, height=%d, hash=%s, state=%s\n",
			event.Operation, event.Header.Height, event.Header.Hash, event.Header.State)

		// Only process if it's a LONGEST_CHAIN or STALE event (indicates main chain activity)
		if event.Header.State == "LONGEST_CHAIN" || event.Header.State == "STALE" {
			// Trigger immediate processing instead of waiting for the next poll
			// Run this async so we can return 200 immediately to the webhook service
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				if err := processor.ProcessChaintip(ctx, headersClient, eventStorage); err != nil {
					fmt.Printf("Error processing webhook event: %v\n", err)
				}
			}()
		}

		// Return success immediately
		return c.SendStatus(fiber.StatusOK)
	}
}