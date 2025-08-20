package pubsub

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/b-open-io/overlay/beef"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

const (
	// BeefProtocolID is the protocol ID for BEEF requests
	BeefProtocolID = "/bsv-overlay/beef/1.0.0"
	
	// TopicPrefix for BSV overlay topics
	TopicPrefix = "tm_"
)

// StorageWithBeefByTopic defines the storage methods needed by LibP2PSync
type StorageWithBeefByTopic interface {
	engine.Storage
	LoadBeefByTxidAndTopic(ctx context.Context, txid *chainhash.Hash, topic string) ([]byte, error)
}


// LibP2PSync manages libp2p-based transaction synchronization
type LibP2PSync struct {
	host         host.Host
	pubsub       *pubsub.PubSub
	topicSubs    map[string]*pubsub.Subscription // topic -> subscription
	inFlightTxs  map[chainhash.Hash]bool         // global in-flight tracking by txid
	inFlightMutex *sync.RWMutex
	engine       engine.OverlayEngineProvider
	storage      engine.Storage
	beefStorage  beef.BeefStorage
	
	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// LibP2PEvent represents an event received from libp2p
type LibP2PEvent struct {
	Topic  string
	Txid   chainhash.Hash
	PeerID peer.ID
}

// NewLibP2PSync creates a new libp2p-based sync manager
func NewLibP2PSync(engine engine.OverlayEngineProvider, storage engine.Storage, beefStorage beef.BeefStorage) (*LibP2PSync, error) {
	// Load or create Bitcoin secp256k1 private key
	privKey, err := loadOrCreatePrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to load private key: %w", err)
	}
	
	// Create libp2p host with Bitcoin key
	host, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}
	
	// Create gossipsub
	ps, err := pubsub.NewGossipSub(context.Background(), host)
	if err != nil {
		return nil, fmt.Errorf("failed to create gossipsub: %w", err)
	}
	
	sync := &LibP2PSync{
		host:          host,
		pubsub:        ps,
		topicSubs:     make(map[string]*pubsub.Subscription),
		inFlightTxs:   make(map[chainhash.Hash]bool),
		inFlightMutex: &sync.RWMutex{},
		engine:        engine,
		storage:       storage,
		beefStorage:   beefStorage,
	}
	
	// Set up BEEF protocol handler
	host.SetStreamHandler(protocol.ID(BeefProtocolID), sync.handleBeefRequest)
	
	// Set up peer discovery
	if err := sync.setupMDNSDiscovery(); err != nil {
		slog.Warn("Failed to set up mDNS discovery", "error", err)
	}
	
	slog.Info("LibP2P sync initialized", "peerID", host.ID(), "addresses", host.Addrs())
	return sync, nil
}

// Start begins libp2p sync with configured topics
func (s *LibP2PSync) Start(ctx context.Context, topics []string) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	
	// Subscribe to each topic
	for _, topic := range topics {
		if err := s.subscribeToTopic(topic); err != nil {
			slog.Error("Failed to subscribe to topic", "topic", topic, "error", err)
			continue
		}
	}
	
	slog.Info("LibP2P sync started", "topics", topics, "peerID", s.host.ID())
	return nil
}

// Stop stops the libp2p sync
func (s *LibP2PSync) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	
	// Close all topic subscriptions
	for topic, sub := range s.topicSubs {
		sub.Cancel()
		delete(s.topicSubs, topic)
	}
	
	// Wait for all goroutines to finish
	s.wg.Wait()
	
	// Close libp2p host
	if err := s.host.Close(); err != nil {
		slog.Error("Failed to close libp2p host", "error", err)
	}
	
	slog.Info("LibP2P sync stopped")
	return nil
}

// PublishTxid publishes a txid to a topic
func (s *LibP2PSync) PublishTxid(ctx context.Context, topic string, txid chainhash.Hash) error {
	topicHandle, err := s.pubsub.Join(topic)
	if err != nil {
		return fmt.Errorf("failed to join topic %s: %w", topic, err)
	}
	defer topicHandle.Close()
	
	// Publish txid as string
	data := []byte(txid.String())
	if err := topicHandle.Publish(ctx, data); err != nil {
		return fmt.Errorf("failed to publish to topic %s: %w", topic, err)
	}
	
	slog.Debug("Published txid", "topic", topic, "txid", txid.String())
	return nil
}

// subscribeToTopic subscribes to a specific topic
func (s *LibP2PSync) subscribeToTopic(topicName string) error {
	topic, err := s.pubsub.Join(topicName)
	if err != nil {
		return fmt.Errorf("failed to join topic %s: %w", topicName, err)
	}
	
	sub, err := topic.Subscribe()
	if err != nil {
		topic.Close()
		return fmt.Errorf("failed to subscribe to topic %s: %w", topicName, err)
	}
	
	s.topicSubs[topicName] = sub
	
	// Start event processing goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.processTopicEvents(topicName, sub)
	}()
	
	slog.Info("Subscribed to topic", "topic", topicName)
	return nil
}

// processTopicEvents handles events from a topic subscription
func (s *LibP2PSync) processTopicEvents(topicName string, sub *pubsub.Subscription) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		
		msg, err := sub.Next(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return // Context cancelled
			}
			slog.Error("Failed to get next message", "topic", topicName, "error", err)
			continue
		}
		
		// Skip our own messages
		if msg.ReceivedFrom == s.host.ID() {
			continue
		}
		
		// Parse txid from message data
		txid, err := chainhash.NewHashFromHex(string(msg.Data))
		if err != nil {
			slog.Error("Invalid txid format", "topic", topicName, "data", string(msg.Data), "peer", msg.ReceivedFrom, "error", err)
			continue
		}
		
		// Process the transaction
		event := LibP2PEvent{
			Topic:  topicName,
			Txid:   *txid,
			PeerID: msg.ReceivedFrom,
		}
		
		s.processTransaction(event)
	}
}

// processTransaction processes a single transaction event
func (s *LibP2PSync) processTransaction(event LibP2PEvent) {
	txid := &event.Txid
	
	// Check if transaction is already in-flight (fast memory check)
	s.inFlightMutex.RLock()
	if s.inFlightTxs[*txid] {
		s.inFlightMutex.RUnlock()
		slog.Debug("Transaction already in-flight", "txid", txid.String(), "peer", event.PeerID)
		return
	}
	s.inFlightMutex.RUnlock()
	
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
		slog.Debug("Transaction already processed", "txid", txid.String(), "peer", event.PeerID)
		return
	}
	
	// Mark as in-flight
	s.inFlightMutex.Lock()
	s.inFlightTxs[*txid] = true
	s.inFlightMutex.Unlock()
	
	// Process the transaction
	go func() {
		defer func() {
			// Remove from in-flight when done
			s.inFlightMutex.Lock()
			delete(s.inFlightTxs, *txid)
			s.inFlightMutex.Unlock()
		}()
		
		if err := s.fetchAndSubmitTransaction(event); err != nil {
			slog.Error("Failed to process transaction", "txid", txid.String(), "peer", event.PeerID, "error", err)
		} else {
			slog.Info("Successfully processed transaction", "txid", txid.String(), "peer", event.PeerID)
		}
	}()
}

// fetchAndSubmitTransaction fetches BEEF and submits to engine
func (s *LibP2PSync) fetchAndSubmitTransaction(event LibP2PEvent) error {
	// Request BEEF from peer using custom protocol
	beef, err := s.requestBeefFromPeer(event.PeerID, event.Topic, event.Txid)
	if err != nil {
		return fmt.Errorf("failed to fetch BEEF from peer %s: %w", event.PeerID, err)
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

// requestBeefFromPeer requests BEEF data from a specific peer
func (s *LibP2PSync) requestBeefFromPeer(peerID peer.ID, topic string, txid chainhash.Hash) ([]byte, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	
	// Open stream to peer
	stream, err := s.host.NewStream(ctx, peerID, protocol.ID(BeefProtocolID))
	if err != nil {
		return nil, fmt.Errorf("failed to open stream to peer: %w", err)
	}
	defer stream.Close()
	
	// Send BEEF request with topic and txid
	request := fmt.Sprintf("GET %s %s\n", topic, txid.String())
	if _, err := stream.Write([]byte(request)); err != nil {
		return nil, fmt.Errorf("failed to send BEEF request: %w", err)
	}
	
	// Read response
	reader := bufio.NewReader(stream)
	
	// Read status line
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read status line: %w", err)
	}
	
	statusLine = strings.TrimSpace(statusLine)
	if !strings.HasPrefix(statusLine, "200") {
		return nil, fmt.Errorf("peer returned error: %s", statusLine)
	}
	
	// Read Content-Length header (simplified)
	_, err = reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read content length: %w", err)
	}
	
	// Skip empty line
	_, err = reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read header separator: %w", err)
	}
	
	// Read BEEF data (for simplicity, read all remaining data)
	beef, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read BEEF data: %w", err)
	}
	
	if len(beef) == 0 {
		return nil, fmt.Errorf("empty BEEF response")
	}
	
	slog.Debug("Successfully fetched BEEF", "txid", txid.String(), "peer", peerID, "size", len(beef))
	return beef, nil
}

// handleBeefRequest handles incoming BEEF requests from peers
func (s *LibP2PSync) handleBeefRequest(stream network.Stream) {
	defer stream.Close()
	
	peerID := stream.Conn().RemotePeer()
	reader := bufio.NewReader(stream)
	
	// Read request line
	requestLine, err := reader.ReadString('\n')
	if err != nil {
		slog.Error("Failed to read request line", "peer", peerID, "error", err)
		return
	}
	
	requestLine = strings.TrimSpace(requestLine)
	parts := strings.Fields(requestLine)
	if len(parts) != 3 || parts[0] != "GET" {
		slog.Error("Invalid request format, expected: GET <topic> <txid>", "peer", peerID, "request", requestLine)
		s.sendErrorResponse(stream, "400 Bad Request")
		return
	}
	
	// Parse topic and txid
	topic := parts[1]
	txidStr := parts[2]
	txid, err := chainhash.NewHashFromHex(txidStr)
	if err != nil {
		slog.Error("Invalid txid format", "peer", peerID, "txid", txidStr, "error", err)
		s.sendErrorResponse(stream, "400 Bad Request")
		return
	}
	
	// Use the new LoadBeefByTxidAndTopic method
	completeBeef, err := s.storage.(StorageWithBeefByTopic).LoadBeefByTxidAndTopic(context.Background(), txid, topic)
	if err != nil {
		slog.Warn("BEEF not found", "peer", peerID, "topic", topic, "txid", txidStr, "error", err)
		s.sendErrorResponse(stream, "404 Not Found")
		return
	}
	
	// Send successful response
	response := fmt.Sprintf("200 OK\nContent-Length: %d\n\n", len(completeBeef))
	if _, err := stream.Write([]byte(response)); err != nil {
		slog.Error("Failed to send response headers", "peer", peerID, "error", err)
		return
	}
	
	// Send BEEF data
	if _, err := stream.Write(completeBeef); err != nil {
		slog.Error("Failed to send BEEF data", "peer", peerID, "error", err)
		return
	}
	
	slog.Debug("Successfully served BEEF", "peer", peerID, "topic", topic, "txid", txidStr, "size", len(completeBeef))
}

// sendErrorResponse sends an error response to the peer
func (s *LibP2PSync) sendErrorResponse(stream network.Stream, status string) {
	response := fmt.Sprintf("%s\nContent-Length: 0\n\n", status)
	stream.Write([]byte(response))
}

// setupMDNSDiscovery sets up mDNS for local peer discovery
func (s *LibP2PSync) setupMDNSDiscovery() error {
	service := mdns.NewMdnsService(s.host, "bsv-overlay", &mdnsNotifee{host: s.host})
	return service.Start()
}

// mdnsNotifee handles mDNS peer discovery notifications
type mdnsNotifee struct {
	host host.Host
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.host.ID() {
		return // Skip self
	}
	
	slog.Info("Discovered peer via mDNS", "peer", pi.ID, "addrs", pi.Addrs)
	
	// Connect to discovered peer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := n.host.Connect(ctx, pi); err != nil {
		slog.Warn("Failed to connect to discovered peer", "peer", pi.ID, "error", err)
	} else {
		slog.Info("Connected to peer", "peer", pi.ID)
	}
}

// loadOrCreatePrivateKey loads or creates a Bitcoin secp256k1 private key
func loadOrCreatePrivateKey() (crypto.PrivKey, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %w", err)
	}
	
	keyDir := filepath.Join(homeDir, ".1sat")
	keyPath := filepath.Join(keyDir, "libp2p.key")
	
	// Ensure directory exists
	if err := os.MkdirAll(keyDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create key directory: %w", err)
	}
	
	// Try to load existing key
	if keyData, err := os.ReadFile(keyPath); err == nil {
		// Decode hex private key
		privKeyBytes, err := hex.DecodeString(string(keyData))
		if err != nil {
			return nil, fmt.Errorf("failed to decode private key: %w", err)
		}
		
		// Create secp256k1 private key
		privKey := secp256k1.PrivKeyFromBytes(privKeyBytes)
		
		// Convert to libp2p format
		libp2pKey, _, err := crypto.ECDSAKeyPairFromKey(privKey.ToECDSA())
		if err != nil {
			return nil, fmt.Errorf("failed to convert private key: %w", err)
		}
		
		slog.Info("Loaded existing private key", "path", keyPath)
		return libp2pKey, nil
	}
	
	// Generate new key
	privKeyBytes := make([]byte, 32)
	if _, err := rand.Read(privKeyBytes); err != nil {
		return nil, fmt.Errorf("failed to generate random key: %w", err)
	}
	
	// Create secp256k1 private key
	privKey := secp256k1.PrivKeyFromBytes(privKeyBytes)
	
	// Convert to libp2p format
	libp2pKey, _, err := crypto.ECDSAKeyPairFromKey(privKey.ToECDSA())
	if err != nil {
		return nil, fmt.Errorf("failed to convert private key: %w", err)
	}
	
	// Save key as hex
	keyHex := hex.EncodeToString(privKeyBytes)
	if err := os.WriteFile(keyPath, []byte(keyHex), 0600); err != nil {
		return nil, fmt.Errorf("failed to save private key: %w", err)
	}
	
	slog.Info("Generated new private key", "path", keyPath)
	return libp2pKey, nil
}