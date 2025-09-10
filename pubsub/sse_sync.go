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
	"time"

	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
)

// SSESync manages centralized SSE connections to peers and processes transactions
type SSESync struct {
	peerConnections map[string]*SSEConnection // peer URL -> connection
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
	peerURL   string
	topics    []string
	eventChan chan SSEEvent
	cancel    context.CancelFunc
	done      chan struct{}
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
		// inFlightTxs is a sync.Map, no need to initialize
		engine:          engine,
		storage:         storage,
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

// Stop stops all SSE connections
func (s *SSESync) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	
	// Close all connections
	for _, conn := range s.peerConnections {
		if conn.cancel != nil {
			conn.cancel()
		}
		<-conn.done
	}
	
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
	
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		
		if err := s.connectAndListen(conn); err != nil {
			slog.Error("SSE connection failed", "peer", conn.peerURL, "error", err)
			
			// Exponential backoff for reconnection
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < maxBackoff {
					backoff *= 2
				}
			}
		} else {
			// Reset backoff on successful connection
			backoff = time.Second
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
	
	for scanner.Scan() {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}
		
		line := strings.TrimSpace(scanner.Text())
		
		if line == "" {
			// Empty line indicates end of event - process it
			if currentEvent.data != "" && currentEvent.eventType != "" {
				s.handleSSEEvent(conn, currentEvent.eventType, currentEvent.data)
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
			}
		}
	}
	
	return scanner.Err()
}

// handleSSEEvent processes a received SSE event
func (s *SSESync) handleSSEEvent(conn *SSEConnection, eventType, data string) {
	// Skip connection messages
	if strings.HasPrefix(data, "Connected to events:") {
		return
	}
	
	// Parse txid from event data
	txid, err := chainhash.NewHashFromHex(data)
	if err != nil {
		slog.Error("Invalid txid format in SSE event", "data", data, "peer", conn.peerURL, "error", err)
		return
	}
	
	// Send to processing channel
	select {
	case conn.eventChan <- SSEEvent{
		Topic:   eventType,
		Txid:    *txid,
		PeerURL: conn.peerURL,
	}:
	case <-s.ctx.Done():
	}
}

// processEvents handles events from the event channel
func (s *SSESync) processEvents(eventChan <-chan SSEEvent) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case event := <-eventChan:
			s.processTransaction(event)
		}
	}
}

// processTransaction processes a single transaction event
func (s *SSESync) processTransaction(event SSEEvent) {
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
	
	// Mark as in-flight
	// Mark transaction as in-flight
	s.inFlightTxs.Store(*txid, struct{}{})
	
	// Process the transaction
	go func() {
		defer func() {
			// Remove from in-flight when done
			s.inFlightTxs.Delete(*txid)
		}()
		
		if err := s.fetchAndSubmitTransaction(event); err != nil {
			slog.Error("Failed to process transaction", "txid", txid.String(), "peer", event.PeerURL, "error", err)
		} else {
			slog.Info("Successfully processed transaction", "txid", txid.String(), "peer", event.PeerURL)
		}
	}()
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
	
	beef, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read BEEF response: %w", err)
	}
	
	// Create TaggedBEEF and submit to engine
	taggedBEEF := overlay.TaggedBEEF{
		Beef:   beef,
		Topics: []string{event.Topic},
	}
	
	_, err = s.engine.Submit(s.ctx, taggedBEEF, engine.SubmitModeCurrent, nil)
	if err != nil {
		return fmt.Errorf("failed to submit transaction to engine: %w", err)
	}
	
	return nil
}