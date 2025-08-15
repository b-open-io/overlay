package publish_test

import (
	"context"
	"testing"
	"time"

	"github.com/b-open-io/overlay/publish"
)

func TestRedisPubSub(t *testing.T) {
	// Skip if no Redis available
	pubsub, err := publish.NewRedisPubSub("redis://localhost:6379")
	if err != nil {
		t.Skip("Redis not available:", err)
	}
	defer pubsub.Close()

	ctx := context.Background()
	
	// Subscribe to a channel
	msgChan, err := pubsub.Subscribe(ctx, "test-channel")
	if err != nil {
		t.Fatal("Failed to subscribe:", err)
	}
	
	// Give subscription time to establish
	time.Sleep(100 * time.Millisecond)
	
	// Publish a message
	err = pubsub.Publish(ctx, "test-channel", "hello world")
	if err != nil {
		t.Fatal("Failed to publish:", err)
	}
	
	// Receive the message
	select {
	case msg := <-msgChan:
		if msg.Channel != "test-channel" {
			t.Errorf("Expected channel 'test-channel', got '%s'", msg.Channel)
		}
		if msg.Payload != "hello world" {
			t.Errorf("Expected payload 'hello world', got '%s'", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
	
	// Test pattern subscription
	patternChan, err := pubsub.PSubscribe(ctx, "test-*")
	if err != nil {
		t.Fatal("Failed to pattern subscribe:", err)
	}
	
	time.Sleep(100 * time.Millisecond)
	
	// Publish to a matching pattern
	err = pubsub.Publish(ctx, "test-pattern", "pattern message")
	if err != nil {
		t.Fatal("Failed to publish pattern message:", err)
	}
	
	// Receive the pattern message
	select {
	case msg := <-patternChan:
		if msg.Channel != "test-pattern" {
			t.Errorf("Expected channel 'test-pattern', got '%s'", msg.Channel)
		}
		if msg.Pattern != "test-*" {
			t.Errorf("Expected pattern 'test-*', got '%s'", msg.Pattern)
		}
		if msg.Payload != "pattern message" {
			t.Errorf("Expected payload 'pattern message', got '%s'", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for pattern message")
	}
}