package headers

import (
	"context"
	"fmt"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker/headers_client"
)

const defaultMaxConcurrentRequests = 8

// inFlightRequest holds the state of an ongoing request
type inFlightRequest struct {
	wg    sync.WaitGroup
	valid bool
	err   error
}

type ClientParams struct {
	Url                   string
	ApiKey                string
	MaxConcurrentRequests int
}

type Client struct {
	// underlying go-sdk headers client
	client *headers_client.Client
	// inFlightRequests tracks ongoing requests to avoid duplicates
	// Key format: "merkleRoot|height", Value: *inFlightRequest
	inFlightRequests sync.Map
	// limiter controls concurrent request limiting
	limiter chan struct{}
}

// NewClient creates a new headers client with ClientParams
func NewClient(params ClientParams) *Client {
	maxConcurrent := defaultMaxConcurrentRequests
	if params.MaxConcurrentRequests > 0 {
		maxConcurrent = params.MaxConcurrentRequests
	}

	// Create the underlying go-sdk client
	sdkClient := &headers_client.Client{
		Url:    params.Url,
		ApiKey: params.ApiKey,
	}

	return &Client{
		client:  sdkClient,
		limiter: make(chan struct{}, maxConcurrent),
	}
}

func (c *Client) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	requestKey := fmt.Sprintf("%s|%d", root.String(), height)

	// Try to load existing request or create new one
	req := &inFlightRequest{}
	req.wg.Add(1)

	if existing, loaded := c.inFlightRequests.LoadOrStore(requestKey, req); loaded {
		// Another goroutine is already handling this request, wait for it
		existingReq := existing.(*inFlightRequest)
		existingReq.wg.Wait()

		return existingReq.valid, existingReq.err
	}

	// We are the first request for this key, perform the actual request
	defer func() {
		req.wg.Done()
		c.inFlightRequests.Delete(requestKey)
	}()

	// Acquire limiter to limit concurrency
	c.limiter <- struct{}{}
	defer func() { <-c.limiter }()

	// Perform the actual request using the underlying client
	valid, err := c.client.IsValidRootForHeight(ctx, root, height)

	// Store result for waiting goroutines
	req.valid = valid
	req.err = err

	return valid, err
}

func (c *Client) BlockByHeight(ctx context.Context, height uint32) (*headers_client.Header, error) {
	return c.client.BlockByHeight(ctx, height)
}

func (c *Client) GetBlockState(ctx context.Context, hash string) (*headers_client.State, error) {
	return c.client.GetBlockState(ctx, hash)
}

func (c *Client) GetChaintip(ctx context.Context) (*headers_client.State, error) {
	return c.client.GetChaintip(ctx)
}

func (c *Client) CurrentHeight(ctx context.Context) (uint32, error) {
	return c.client.CurrentHeight(ctx)
}