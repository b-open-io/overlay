package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/bsv-blockchain/go-sdk/overlay"
)

// PeerBroadcaster handles broadcasting submitted transactions to peer overlays
type PeerBroadcaster struct {
	peerTopics map[string][]string // peer URL -> topics to broadcast
	httpClient *http.Client
}

// NewPeerBroadcaster creates a new peer broadcaster with the given peer-to-topics mapping
func NewPeerBroadcaster(peerTopics map[string][]string) *PeerBroadcaster {
	slog.Info("Peer broadcaster created", "peers", len(peerTopics))
	return &PeerBroadcaster{
		peerTopics: peerTopics,
		httpClient: &http.Client{},
	}
}

// BroadcastTransaction broadcasts a TaggedBEEF to all configured peers for the topics
func (p *PeerBroadcaster) BroadcastTransaction(ctx context.Context, taggedBEEF overlay.TaggedBEEF) error {
	if len(p.peerTopics) == 0 {
		return nil // No peers configured
	}
	
	// Group peers by topics they support
	peersToNotify := make(map[string][]string) // peer URL -> topics to include
	
	for _, topic := range taggedBEEF.Topics {
		for peerURL, supportedTopics := range p.peerTopics {
			// Check if this peer supports this topic
			for _, supportedTopic := range supportedTopics {
				if supportedTopic == topic {
					peersToNotify[peerURL] = append(peersToNotify[peerURL], topic)
					break
				}
			}
		}
	}
	
	if len(peersToNotify) == 0 {
		slog.Debug("No peers configured for transaction topics", "topics", taggedBEEF.Topics)
		return nil
	}
	
	// Broadcast to each peer concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, len(peersToNotify))
	
	for peerURL, topics := range peersToNotify {
		wg.Add(1)
		go func(peer string, topicsToSend []string) {
			defer wg.Done()
			
			if err := p.broadcastToPeer(ctx, peer, topicsToSend, taggedBEEF.Beef); err != nil {
				slog.Error("Failed to broadcast to peer", "peer", peer, "topics", topicsToSend, "error", err)
				errChan <- fmt.Errorf("peer %s: %w", peer, err)
			} else {
				slog.Info("Successfully broadcasted to peer", "peer", peer, "topics", topicsToSend)
			}
		}(peerURL, topics)
	}
	
	wg.Wait()
	close(errChan)
	
	// Collect any errors (non-blocking)
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("broadcast failed to %d peers: %v", len(errors), errors)
	}
	
	return nil
}

// broadcastToPeer sends a transaction to a specific peer
func (p *PeerBroadcaster) broadcastToPeer(ctx context.Context, peerURL string, topics []string, beef []byte) error {
	// Build submit URL
	submitURL := fmt.Sprintf("%s/api/v1/submit", peerURL)
	
	// Create request with BEEF body
	req, err := http.NewRequestWithContext(ctx, "POST", submitURL, bytes.NewReader(beef))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	// Set required headers
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("x-topics", strings.Join(topics, ","))
	
	// Send request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	
	// Check response status
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("peer returned status %d", resp.StatusCode)
	}
	
	slog.Debug("Peer broadcast successful", "peer", peerURL, "topics", topics, "status", resp.StatusCode)
	return nil
}

