package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
)

var (
	PORT int
	BIND_ADDR string
)

func init() {
	godotenv.Load(".env")
	
	// Parse PORT from env before flags
	PORT, _ = strconv.Atoi(os.Getenv("PORT"))
	BIND_ADDR = os.Getenv("BIND_ADDR")
	
	// Define command-line flags with env var defaults
	flag.IntVar(&PORT, "p", PORT, "Port to listen on")
	flag.StringVar(&BIND_ADDR, "bind", BIND_ADDR, "Address to bind to")
	flag.Parse()
	
	// Apply defaults
	if PORT == 0 {
		PORT = 4001
	}
	if BIND_ADDR == "" {
		BIND_ADDR = "0.0.0.0"
	}
}

// bootstrapNotifee handles peer discovery notifications for bootstrap node
type bootstrapNotifee struct {
	host host.Host
}

func (n *bootstrapNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.host.ID() {
		return // Skip self
	}
	
	log.Printf("Bootstrap: Discovered peer %s with addresses: %v", pi.ID, pi.Addrs)
	
	// Connect to discovered peer to help with network formation
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := n.host.Connect(ctx, pi); err != nil {
		log.Printf("Bootstrap: Failed to connect to peer %s: %v", pi.ID, err)
	} else {
		log.Printf("Bootstrap: Connected to peer %s", pi.ID)
	}
}

func main() {
	log.Printf("Starting BSV Overlay Bootstrap Node on %s:%d", BIND_ADDR, PORT)
	
	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Handle OS signals for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		log.Println("Received shutdown signal, shutting down...")
		cancel()
	}()
	
	// Create libp2p host for bootstrap node
	listenAddr := fmt.Sprintf("/ip4/%s/tcp/%d", BIND_ADDR, PORT)
	host, err := libp2p.New(
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.EnableRelay(), // Allow relaying for other peers
	)
	if err != nil {
		log.Fatalf("Failed to create libp2p host: %v", err)
	}
	defer host.Close()
	
	// Create gossipsub (required for proper network participation)
	ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		log.Fatalf("Failed to create gossipsub: %v", err)
	}
	_ = ps // Keep reference to prevent GC
	
	// Set up mDNS discovery
	mdnsService := mdns.NewMdnsService(host, "bsv-overlay", &bootstrapNotifee{host: host})
	if err := mdnsService.Start(); err != nil {
		log.Printf("Warning: Failed to start mDNS service: %v", err)
	} else {
		log.Println("mDNS discovery service started")
	}
	
	// Print bootstrap node information
	log.Printf("Bootstrap Node ID: %s", host.ID())
	log.Println("Listen addresses:")
	for _, addr := range host.Addrs() {
		// Create full multiaddr with peer ID
		fullAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("%s/p2p/%s", addr, host.ID()))
		if err != nil {
			log.Printf("  %s/p2p/%s", addr, host.ID())
		} else {
			log.Printf("  %s", fullAddr)
		}
	}
	log.Println()
	log.Println("Bootstrap node ready - other overlay nodes can connect to these addresses")
	log.Println("Press Ctrl+C to shutdown")
	
	// Keep the bootstrap node running and periodically log status
	// TODO: Consider adding explicit liveness checking, connection limits,
	// peer reputation scoring, and graceful reconnection with exponential backoff
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down bootstrap node...")
			if mdnsService != nil {
				mdnsService.Close()
			}
			return
		case <-ticker.C:
			peers := host.Network().Peers()
			log.Printf("Bootstrap status: Connected to %d peers", len(peers))
			if len(peers) > 0 {
				log.Printf("Connected peers: %v", peers)
			}
		}
	}
}