package pubsub

import (
	"fmt"
	"strings"
)

// CreatePubSub creates the appropriate PubSub implementation from a connection string.
// Auto-detects the pubsub type from the URL scheme.
//
// Supported formats:
//   - redis://localhost:6379 - Redis-based pub/sub
//   - channels:// - In-memory channel-based pub/sub (no dependencies)
//   - Empty string: defaults to channels:// (in-memory channels)
//
// Example:
//
//	CreatePubSub("redis://localhost:6379")  // Redis pub/sub
//	CreatePubSub("channels://")             // Channel pub/sub
//	CreatePubSub("")                        // Defaults to channels://
func CreatePubSub(connectionString string) (PubSub, error) {
	// Default to channel-based pub/sub if no connection string provided
	if connectionString == "" {
		connectionString = "channels://"
	}

	switch {
	case strings.HasPrefix(connectionString, "redis://"):
		// Create Redis-based pub/sub
		redisPubSub, err := NewRedisPubSub(connectionString)
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis pub/sub: %w", err)
		}
		return redisPubSub, nil

	case strings.HasPrefix(connectionString, "channels://"):
		// Create channel-based pub/sub (no dependencies)
		return NewChannelPubSub(), nil

	default:
		return nil, fmt.Errorf("unsupported pub/sub URL scheme: %s", connectionString)
	}
}