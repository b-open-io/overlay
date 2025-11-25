package routes

import (
	"fmt"
	"log"
	"strconv"

	"github.com/b-open-io/overlay/storage"
	"github.com/bsv-blockchain/go-chaintracks/pkg/chaintracks"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/gofiber/fiber/v2"
)

// RoutesConfig holds the configuration for common 1sat routes
type RoutesConfig struct {
	Storage      *storage.EventDataStorage
	ChainTracker chaintracks.Chaintracks
	Engine       *engine.Engine
}

// ParseEventQuery parses common query parameters for event endpoints
func ParseEventQuery(c *fiber.Ctx) *storage.EventQuestion {
	// Parse query parameters
	fromScore := 0.0
	limit := 100 // default limit

	// Parse 'from' parameter as float64
	if fromParam := c.Query("from"); fromParam != "" {
		if score, err := strconv.ParseFloat(fromParam, 64); err == nil {
			fromScore = score
		}
	}

	// Parse 'limit' parameter
	if limitParam := c.Query("limit"); limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil && l > 0 {
			limit = l
			if limit > 1000 {
				limit = 1000 // cap at 1000
			}
		}
	}

	return &storage.EventQuestion{
		Event: c.Params("event"),
		From:  fromScore,
		Limit: limit,
	}
}

// RegisterRoutes registers common 1sat API routes that are generic across overlay services
func RegisterRoutes(group fiber.Router, config *RoutesConfig) {
	if config == nil || config.Storage == nil || config.ChainTracker == nil || config.Engine == nil {
		log.Fatal("RegisterCommonRoutes: config, storage, chaintracker, and engine are required")
	}

	store := config.Storage
	chaintracker := config.ChainTracker
	eng := config.Engine

	// Route for event history
	group.Get("/events/:topic/:event/history", func(c *fiber.Ctx) error {
		topic := c.Params("topic")
		event := c.Params("event")
		log.Printf("Received request for event history: %s (topic: %s)", event, topic)

		// Validate topic is enabled
		if _, exists := eng.Managers[topic]; !exists {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"message": "Topic not available",
			})
		}

		// Build question and call FindOutputData for history
		question := ParseEventQuery(c)
		question.Event = event
		question.Topic = topic
		question.UnspentOnly = false // History includes all outputs
		outputs, err := store.FindOutputData(c.Context(), question)
		if err != nil {
			log.Printf("History lookup error: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": err.Error(),
			})
		}

		return c.JSON(outputs)
	})

	// POST route for multiple events history
	group.Post("/events/:topic/history", func(c *fiber.Ctx) error {
		topic := c.Params("topic")

		// Validate topic is enabled
		if _, exists := eng.Managers[topic]; !exists {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"message": "Topic not available",
			})
		}

		// Parse the request body - accept array of events directly
		var events []string
		if err := c.BodyParser(&events); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "Invalid request body",
			})
		}

		if len(events) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "No events provided",
			})
		}

		// Limit the number of events to prevent abuse
		if len(events) > 100 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "Too many events (max 100)",
			})
		}

		log.Printf("Received multi-event history request for %d events (topic: %s)", len(events), topic)

		// Parse query parameters for paging
		question := ParseEventQuery(c)
		question.Events = events
		question.Topic = topic
		question.UnspentOnly = false // History includes all outputs

		outputs, err := store.FindOutputData(c.Context(), question)
		if err != nil {
			log.Printf("History lookup error: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": "Failed to retrieve event history",
			})
		}

		return c.JSON(outputs)
	})

	// Route for unspent events only
	group.Get("/events/:topic/:event/unspent", func(c *fiber.Ctx) error {
		topic := c.Params("topic")
		event := c.Params("event")
		log.Printf("Received request for unspent events: %s (topic: %s)", event, topic)

		// Validate topic is enabled
		if _, exists := eng.Managers[topic]; !exists {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"message": "Topic not available",
			})
		}

		// Build question and call FindOutputData for unspent
		question := ParseEventQuery(c)
		question.Event = event
		question.Topic = topic
		question.UnspentOnly = true
		outputs, err := store.FindOutputData(c.Context(), question)
		if err != nil {
			log.Printf("Unspent lookup error: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": err.Error(),
			})
		}

		return c.JSON(outputs)
	})

	// POST route for multiple events unspent
	group.Post("/events/:topic/unspent", func(c *fiber.Ctx) error {
		topic := c.Params("topic")

		// Validate topic is enabled
		if _, exists := eng.Managers[topic]; !exists {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"message": "Topic not available",
			})
		}

		// Parse the request body - accept array of events directly
		var events []string
		if err := c.BodyParser(&events); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "Invalid request body",
			})
		}

		if len(events) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "No events provided",
			})
		}

		// Limit the number of events to prevent abuse
		if len(events) > 100 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "Too many events (max 100)",
			})
		}

		log.Printf("Received multi-event unspent request for %d events (topic: %s)", len(events), topic)

		// Parse query parameters for paging
		question := ParseEventQuery(c)
		question.Events = events
		question.Topic = topic
		question.UnspentOnly = true

		outputs, err := store.FindOutputData(c.Context(), question)
		if err != nil {
			log.Printf("Unspent lookup error: %v", err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": "Failed to retrieve unspent events",
			})
		}

		return c.JSON(outputs)
	})

	group.Get("/block/tip", func(c *fiber.Ctx) error {
		// Get current block tip from chaintracker
		tip := chaintracker.GetTip()
		if tip == nil {
			log.Printf("Failed to get block tip: tip is nil")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": "Failed to get current block tip",
			})
		}

		// Return header with height and properly formatted hash strings
		return c.JSON(tip)
	})

	group.Get("/block/:height", func(c *fiber.Ctx) error {
		heightStr := c.Params("height")

		// Parse height as uint32
		height64, err := strconv.ParseUint(heightStr, 10, 32)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "Invalid height parameter",
			})
		}
		height := uint32(height64)

		// Get block header by height
		blockHeader, err := chaintracker.GetHeaderByHeight(height)
		if err != nil {
			log.Printf("Failed to get block header for height %d: %v", height, err)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"message": "Failed to get block header",
			})
		}

		// Return header with height and properly formatted hash strings
		return c.JSON(blockHeader)
	})

	// BEEF endpoint for SSE and peer synchronization
	group.Get("/beef/:topic/:txid", func(c *fiber.Ctx) error {
		topic := c.Params("topic")
		txidStr := c.Params("txid")

		// Validate topic is enabled
		if _, exists := eng.Managers[topic]; !exists {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"message": "Topic not available",
			})
		}

		// Parse txid
		txid, err := chainhash.NewHashFromHex(txidStr)
		if err != nil {
			log.Printf("Invalid txid format: %v", err)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"message": "Invalid txid format",
			})
		}

		// Use the new LoadBeefByTxidAndTopic method
		completeBeef, err := store.LoadBeefByTxidAndTopic(c.Context(), txid, topic)
		if err != nil {
			log.Printf("BEEF not found for topic %s, txid %s: %v", topic, txidStr, err)
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"message": "BEEF not found",
			})
		}

		// Set content type and return raw BEEF data
		c.Set("Content-Type", "application/octet-stream")
		c.Set("Content-Length", fmt.Sprintf("%d", len(completeBeef)))

		return c.Send(completeBeef)
	})
}
