package pubsub

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// topicQueue represents a serial processing queue for a specific topic
type topicQueue struct {
	queue  chan SSEEvent
	cancel context.CancelFunc
	done   chan struct{}
}

// SSESync manages centralized SSE connections to peers and processes transactions
type SSESync struct {
	peerConnections map[string]*SSEConnection // peer URL -> connection
	topicQueues     sync.Map                  // topic -> *topicQueue (serial queues per topic)
	inFlightTxs     sync.Map                  // chainhash.Hash -> struct{} (global in-flight tracking)
	engine          engine.OverlayEngineProvider
	storage         engine.Storage
	httpClient      *http.Client

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// SSEConnection represents a single SSE connection to a peer
type SSEConnection struct {
	peerURL     string
	topics      []string
	eventChan   chan SSEEvent
	cancel      context.CancelFunc
	done        chan struct{}
	lastEventID atomic.Pointer[string] // Track last received event ID for reconnection
}

// SSEEvent represents an event received from SSE
type SSEEvent struct {
	Topic   string
	Txid    chainhash.Hash
	PeerURL string
}

// NewSSESync creates a new centralized SSE sync manager
func NewSSESync(engine engine.OverlayEngineProvider, storage engine.Storage) *SSESync {
	return &SSESync{
		peerConnections: make(map[string]*SSEConnection),
		// topicQueues and inFlightTxs are sync.Maps, no need to initialize
		engine:  engine,
		storage: storage,
		httpClient: &http.Client{
			Timeout: 0, // No timeout for streaming connections
		},
	}
}

// Start begins SSE sync with configured peers
func (s *SSESync) Start(ctx context.Context, peerTopics map[string][]string) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Start connections to each peer
	for peerURL, topics := range peerTopics {
		conn := &SSEConnection{
			peerURL:   peerURL,
			topics:    topics,
			eventChan: make(chan SSEEvent, 100),
			done:      make(chan struct{}),
		}

		s.peerConnections[peerURL] = conn

		// Start connection goroutine
		s.wg.Add(1)
		go func(connection *SSEConnection) {
			defer s.wg.Done()
			s.handlePeerConnection(connection)
		}(conn)

		// Start event processing goroutine
		s.wg.Add(1)
		go func(connection *SSEConnection) {
			defer s.wg.Done()
			s.processEvents(connection.eventChan)
		}(conn)
	}

	slog.Info("SSE sync started", "peers", len(peerTopics))
	return nil
}

// Stop stops all SSE connections and topic queues
func (s *SSESync) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}

	// Close all peer connections
	for _, conn := range s.peerConnections {
		if conn.cancel != nil {
			conn.cancel()
		}
		<-conn.done
	}

	// Stop all topic queues
	s.topicQueues.Range(func(key, value any) bool {
		topic := key.(string)
		queue := value.(*topicQueue)

		if queue.cancel != nil {
			queue.cancel()
		}
		<-queue.done

		slog.Info("Stopped topic queue", "topic", topic)
		return true // Continue iteration
	})

	// Wait for all goroutines to finish
	s.wg.Wait()

	slog.Info("SSE sync stopped")
	return nil
}

// handlePeerConnection manages a single SSE connection to a peer
func (s *SSESync) handlePeerConnection(conn *SSEConnection) {
	defer close(conn.done)

	backoff := time.Second
	maxBackoff := time.Minute * 5
	reconnectCount := 0

	for {
		select {
		case <-s.ctx.Done():
			slog.Debug("SSE connection handler stopped", "peer", conn.peerURL, "reconnect_count", reconnectCount)
			return
		default:
		}

		startTime := time.Now()
		if err := s.connectAndListen(conn); err != nil {
			duration := time.Since(startTime)
			reconnectCount++

			slog.Error("SSE connection failed",
				"peer", conn.peerURL,
				"error", err,
				"duration", duration,
				"reconnect_count", reconnectCount,
				"next_retry_in", backoff)

			// Exponential backoff for reconnection
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < maxBackoff {
					backoff *= 2
				}
				slog.Debug("Attempting SSE reconnection",
					"peer", conn.peerURL,
					"attempt", reconnectCount+1,
					"backoff", backoff)
			}
		} else {
			// Reset backoff on successful connection
			backoff = time.Second
			slog.Debug("SSE connection closed normally", "peer", conn.peerURL, "duration", time.Since(startTime))
		}
	}
}

// connectAndListen establishes SSE connection and processes events
func (s *SSESync) connectAndListen(conn *SSEConnection) error {
	// Build subscription URL with topics
	topicsStr := strings.Join(conn.topics, ",")
	subscribeURL := fmt.Sprintf("%s/api/1sat/subscribe/%s", conn.peerURL, topicsStr)

	slog.Info("Connecting to SSE endpoint", "url", subscribeURL, "topics", conn.topics)

	req, err := http.NewRequestWithContext(s.ctx, "GET", subscribeURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create SSE request: %w", err)
	}

	// Set SSE headers
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	// Set Last-Event-ID for reconnection if we have one
	if lastID := conn.lastEventID.Load(); lastID != nil {
		req.Header.Set("Last-Event-ID", *lastID)
		slog.Info("Reconnecting with Last-Event-ID", "peer", conn.peerURL, "lastID", *lastID)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to SSE endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SSE endpoint returned status %d", resp.StatusCode)
	}

	slog.Info("SSE connection established", "peer", conn.peerURL, "topics", conn.topics)

	// Process events
	return s.processSSEStream(conn, resp.Body)
}

// processSSEStream reads and handles SSE events from the response body
func (s *SSESync) processSSEStream(conn *SSEConnection, body io.Reader) error {
	scanner := bufio.NewScanner(body)
	var currentEvent struct {
		eventType string
		data      string
		id        string
	}

	eventCount := 0
	lastLogTime := time.Now()

	for scanner.Scan() {
		select {
		case <-s.ctx.Done():
			slog.Debug("SSE stream processing cancelled", "peer", conn.peerURL, "events_processed", eventCount)
			return s.ctx.Err()
		default:
		}

		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			// Empty line indicates end of event - process it
			if currentEvent.data != "" && currentEvent.eventType != "" {
				s.handleSSEEvent(conn, currentEvent.eventType, currentEvent.data)
				eventCount++

				// Log periodic status every minute
				if time.Since(lastLogTime) > time.Minute {
					slog.Debug("SSE stream status", "peer", conn.peerURL, "events_processed", eventCount, "uptime", time.Since(lastLogTime))
					lastLogTime = time.Now()
				}
			}
			currentEvent = struct {
				eventType string
				data      string
				id        string
			}{} // Reset for next event
			continue
		}

		// Parse SSE field
		if colonIndex := strings.Index(line, ":"); colonIndex > 0 {
			field := line[:colonIndex]
			value := strings.TrimSpace(line[colonIndex+1:])

			switch field {
			case "event":
				currentEvent.eventType = value
			case "data":
				currentEvent.data = value
			case "id":
				currentEvent.id = value
				// Store the event ID for reconnection
				conn.lastEventID.Store(&value)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Error("SSE stream scanner error", "peer", conn.peerURL, "error", err, "events_processed", eventCount)
		return err
	}

	slog.Info("SSE stream ended", "peer", conn.peerURL, "events_processed", eventCount)
	return scanner.Err()
}

// handleSSEEvent processes a received SSE event
func (s *SSESync) handleSSEEvent(conn *SSEConnection, eventType, data string) {
	// Skip connection messages
	if strings.HasPrefix(data, "Connected to events:") {
		slog.Debug("SSE connection message", "peer", conn.peerURL, "message", data)
		return
	}

	// Parse txid from event data
	txid, err := chainhash.NewHashFromHex(data)
	if err != nil {
		slog.Error("Invalid txid format in SSE event", "data", data, "peer", conn.peerURL, "error", err)
		return
	}

	slog.Debug("SSE event received", "peer", conn.peerURL, "topic", eventType, "txid", txid.String())

	// Send to processing channel
	select {
	case conn.eventChan <- SSEEvent{
		Topic:   eventType,
		Txid:    *txid,
		PeerURL: conn.peerURL,
	}:
		slog.Debug("SSE event queued for processing", "peer", conn.peerURL, "topic", eventType, "txid", txid.String())
	case <-s.ctx.Done():
		slog.Debug("SSE event dropped - context cancelled", "peer", conn.peerURL, "topic", eventType, "txid", txid.String())
	}
}

// getOrCreateTopicQueue gets or creates a serial queue for the given topic
func (s *SSESync) getOrCreateTopicQueue(topic string) *topicQueue {
	if existing, ok := s.topicQueues.Load(topic); ok {
		return existing.(*topicQueue)
	}

	// Create new topic queue with context
	ctx, cancel := context.WithCancel(s.ctx)
	queue := &topicQueue{
		queue:  make(chan SSEEvent, 100), // Buffered to prevent blocking
		cancel: cancel,
		done:   make(chan struct{}),
	}

	// Try to store - if another goroutine created it first, use that one
	if actual, loaded := s.topicQueues.LoadOrStore(topic, queue); loaded {
		// Another goroutine created it first, clean up ours and use theirs
		cancel()
		close(queue.queue)
		return actual.(*topicQueue)
	}

	// Start worker for this topic queue
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.processTopicQueue(ctx, topic, queue)
	}()

	slog.Info("Created serial queue for topic", "topic", topic)
	return queue
}

// processTopicQueue processes events serially for a specific topic
func (s *SSESync) processTopicQueue(ctx context.Context, topic string, queue *topicQueue) {
	defer close(queue.done)
	defer close(queue.queue)

	slog.Info("Starting serial queue processor", "topic", topic)

	for {
		select {
		case <-ctx.Done():
			slog.Info("Serial queue processor stopped", "topic", topic)
			return
		case event, ok := <-queue.queue:
			if !ok {
				return // Channel closed
			}
			s.processTransactionSync(event)
		}
	}
}

// processEvents handles events from the event channel
func (s *SSESync) processEvents(eventChan <-chan SSEEvent) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case event := <-eventChan:
			s.enqueueTransaction(event)
		}
	}
}

// enqueueTransaction adds a transaction event to the appropriate topic queue
func (s *SSESync) enqueueTransaction(event SSEEvent) {
	txid := &event.Txid

	// Check if transaction is already in-flight (fast memory check)
	if _, exists := s.inFlightTxs.Load(*txid); exists {
		slog.Debug("Transaction already in-flight", "txid", txid.String(), "peer", event.PeerURL)
		return
	}

	// Check if transaction already exists in storage for this topic
	exists, err := s.storage.DoesAppliedTransactionExist(s.ctx, &overlay.AppliedTransaction{
		Txid:  txid,
		Topic: event.Topic,
	})
	if err != nil {
		slog.Error("Failed to check transaction existence", "txid", txid.String(), "error", err)
		return
	}
	if exists {
		slog.Debug("Transaction already processed", "txid", txid.String(), "peer", event.PeerURL)
		return
	}

	// Get or create topic queue and enqueue the event
	queue := s.getOrCreateTopicQueue(event.Topic)
	select {
	case queue.queue <- event:
		slog.Debug("Transaction enqueued for serial processing", "txid", txid.String(), "topic", event.Topic, "peer", event.PeerURL)
	case <-s.ctx.Done():
		return
	default:
		slog.Warn("Topic queue full, dropping transaction", "txid", txid.String(), "topic", event.Topic, "peer", event.PeerURL)
	}
}

// processTransactionSync processes a single transaction event synchronously (called from topic queue)
func (s *SSESync) processTransactionSync(event SSEEvent) {
	txid := &event.Txid

	// Mark transaction as in-flight
	s.inFlightTxs.Store(*txid, struct{}{})
	defer func() {
		// Remove from in-flight when done
		s.inFlightTxs.Delete(*txid)
	}()

	// Process the transaction
	if err := s.fetchAndSubmitTransaction(event); err != nil {
		slog.Error("Failed to process transaction", "txid", txid.String(), "topic", event.Topic, "peer", event.PeerURL, "error", err)
	} else {
		slog.Info("Successfully processed transaction", "txid", txid.String(), "topic", event.Topic, "peer", event.PeerURL)
	}
}

// fetchAndSubmitTransaction fetches BEEF and submits to engine
func (s *SSESync) fetchAndSubmitTransaction(event SSEEvent) error {
	// Fetch BEEF from peer using topic-specific endpoint
	beefURL := fmt.Sprintf("%s/api/1sat/beef/%s/%s", event.PeerURL, event.Topic, event.Txid.String())

	resp, err := s.httpClient.Get(beefURL)
	if err != nil {
		return fmt.Errorf("failed to fetch BEEF: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("BEEF endpoint returned status %d", resp.StatusCode)
	}

	beefBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read BEEF response: %w", err)
	}

	_, tx, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return fmt.Errorf("failed to parse BEEF: %w", err)
	}

	submitMode := engine.SubmitModeCurrent
	if tx.MerklePath != nil {
		submitMode = engine.SubmitModeHistorical
	}
	// Create TaggedBEEF and submit to engine
	taggedBEEF := overlay.TaggedBEEF{
		Beef:   beefBytes,
		Topics: []string{event.Topic},
	}

	_, err = s.engine.Submit(s.ctx, taggedBEEF, submitMode, nil)
	if err != nil {
		return fmt.Errorf("failed to submit transaction to engine: %w", err)
	}

	return nil
}
