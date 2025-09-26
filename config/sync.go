package config

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/b-open-io/overlay/queue"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
)

const DefaultGASPConcurrency = 256

// PeerSettings defines the configuration for a peer's capabilities
type PeerSettings struct {
	SSE       bool `json:"sse"`       // Server-Sent Events support
	GASP      bool `json:"gasp"`      // GASP protocol support
	Broadcast bool `json:"broadcast"` // Transaction broadcasting support
}

// PeerConfigKey returns the storage key for peer configuration for a given topic
func PeerConfigKey(topic string) string {
	return "peers:" + topic
}

// ConfigureSync configures engine sync settings by looking up peer configurations
// from storage for the given topic IDs. Atomically replaces the engine's sync configuration.
func ConfigureSync(ctx context.Context, eng *engine.Engine, queueStore queue.QueueStorage, topicIds []string) error {
	if eng == nil || queueStore == nil {
		return fmt.Errorf("engine and queue storage are required")
	}

	// Build new sync configuration map
	newSyncConfig := make(map[string]engine.SyncConfiguration)

	// Track configured topics and peers for logging
	configuredTopics := 0
	totalPeers := 0

	for _, topicId := range topicIds {
		// Look up peer configuration using PeerConfigKey
		peerConfigKey := PeerConfigKey(topicId)
		peerData, err := queueStore.HGetAll(ctx, peerConfigKey)
		if err != nil {
			log.Printf("Failed to get peer config for topic %s: %v", topicId, err)
			continue
		}

		if len(peerData) == 0 {
			// No peer configuration found for this topic - skip it
			log.Printf("No peer configuration found for topic: %s", topicId)
			continue
		}

		// Parse peer settings and build configurations
		gaspPeers := make([]string, 0)

		for peerURL, settingsJSON := range peerData {
			var settings PeerSettings
			if err := json.Unmarshal([]byte(settingsJSON), &settings); err != nil {
				log.Printf("Warning: failed to parse settings for peer %s (topic %s): %v", peerURL, topicId, err)
				continue
			}

			// Build GASP peer list
			if settings.GASP {
				// Add /api/v1 path for GASP endpoints
				gaspPeers = append(gaspPeers, peerURL+"/api/v1")
			}

			totalPeers++
		}

		// Add sync configuration if GASP peers are available
		if len(gaspPeers) > 0 {
			newSyncConfig[topicId] = engine.SyncConfiguration{
				Type:        engine.SyncConfigurationPeers,
				Peers:       gaspPeers,
				Concurrency: DefaultGASPConcurrency,
			}
			configuredTopics++
			log.Printf("Configured GASP sync for topic %s with %d peers", topicId, len(gaspPeers))
		}
	}

	// Atomically replace the engine's sync configuration
	eng.SyncConfiguration = newSyncConfig

	log.Printf("ConfigureSync completed: %d/%d topics configured with %d total peers",
		configuredTopics, len(topicIds), totalPeers)

	return nil
}

// GetBroadcastPeerTopics builds a peer-to-topics mapping for transaction broadcasting
// based on stored peer configurations
func GetBroadcastPeerTopics(ctx context.Context, queueStore queue.QueueStorage, topicIds []string) (map[string][]string, error) {
	if queueStore == nil {
		return nil, fmt.Errorf("queue storage is required")
	}
	peerTopics := make(map[string][]string)

	for _, topicId := range topicIds {
		// Look up peer configuration using PeerConfigKey
		peerConfigKey := PeerConfigKey(topicId)
		peerData, err := queueStore.HGetAll(ctx, peerConfigKey)
		if err != nil {
			log.Printf("Failed to get peer config for topic %s: %v", topicId, err)
			continue
		}

		if len(peerData) == 0 {
			continue
		}

		// Parse peer settings and build broadcast mapping
		for peerURL, settingsJSON := range peerData {
			var settings PeerSettings
			if err := json.Unmarshal([]byte(settingsJSON), &settings); err != nil {
				log.Printf("Warning: failed to parse settings for peer %s (topic %s): %v", peerURL, topicId, err)
				continue
			}

			// Add to broadcast mapping if enabled
			if settings.Broadcast {
				peerTopics[peerURL] = append(peerTopics[peerURL], topicId)
			}
		}
	}

	return peerTopics, nil
}

// GetSSEPeerTopics builds a peer-to-topics mapping for SSE synchronization
// based on stored peer configurations
func GetSSEPeerTopics(ctx context.Context, queueStore queue.QueueStorage, topicIds []string) (map[string][]string, error) {
	if queueStore == nil {
		return nil, fmt.Errorf("queue storage is required")
	}
	ssePeerTopics := make(map[string][]string)

	for _, topicId := range topicIds {
		// Look up peer configuration using PeerConfigKey
		peerConfigKey := PeerConfigKey(topicId)
		peerData, err := queueStore.HGetAll(ctx, peerConfigKey)
		if err != nil {
			log.Printf("Failed to get peer config for topic %s: %v", topicId, err)
			continue
		}

		if len(peerData) == 0 {
			continue
		}

		// Parse peer settings and build SSE mapping
		for peerURL, settingsJSON := range peerData {
			var settings PeerSettings
			if err := json.Unmarshal([]byte(settingsJSON), &settings); err != nil {
				log.Printf("Warning: failed to parse settings for peer %s (topic %s): %v", peerURL, topicId, err)
				continue
			}

			// Add to SSE mapping if enabled
			if settings.SSE {
				ssePeerTopics[peerURL] = append(ssePeerTopics[peerURL], topicId)
			}
		}
	}

	return ssePeerTopics, nil
}
