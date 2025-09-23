package beef

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
)

type S3BeefStorage struct {
	client   *s3.Client
	bucket   string
	prefix   string
	fallback BeefStorage
}

// NewS3BeefStorage creates a new S3-based BEEF storage
// bucket: S3 bucket name
// prefix: optional prefix for all keys (e.g., "beef/")
// fallback: optional fallback storage
func NewS3BeefStorage(bucket string, prefix string, fallback BeefStorage) (*S3BeefStorage, error) {
	// Load the default AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3BeefStorage{
		client:   client,
		bucket:   bucket,
		prefix:   prefix,
		fallback: fallback,
	}, nil
}

// NewS3BeefStorageWithClient creates a new S3-based BEEF storage with a provided client
func NewS3BeefStorageWithClient(client *s3.Client, bucket string, prefix string, fallback BeefStorage) *S3BeefStorage {
	return &S3BeefStorage{
		client:   client,
		bucket:   bucket,
		prefix:   prefix,
		fallback: fallback,
	}
}

// getKey returns the S3 key for a given txid
// Uses subdirectories based on first 2 chars of txid to avoid too many files in one prefix
func (s *S3BeefStorage) getKey(txid *chainhash.Hash) string {
	txidStr := txid.String()
	// Create subdirectory structure: prefix/xx/xxxxxxxxxxxx.beef
	subDir := txidStr[:2]
	fileName := txidStr + ".beef"

	if s.prefix != "" {
		return s.prefix + subDir + "/" + fileName
	}
	return subDir + "/" + fileName
}

func (s *S3BeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	key := s.getKey(txid)

	// Try to load from S3 first
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	if err == nil {
		defer result.Body.Close()
		beefBytes, err := io.ReadAll(result.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read S3 object: %w", err)
		}
		return beefBytes, nil
	}

	// Check if it's a not found error
	var nfe *types.NoSuchKey
	if errors.As(err, &nfe) {
		// If not found and we have a fallback, try it
		if s.fallback != nil {
			beefBytes, err := s.fallback.LoadBeef(ctx, txid)
			if err == nil {
				// Save to S3 for future use
				s.SaveBeef(ctx, txid, beefBytes)
			}
			return beefBytes, err
		}
		return nil, ErrNotFound
	}

	// For other errors, return them
	return nil, fmt.Errorf("failed to get object from S3: %w", err)
}

func (s *S3BeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	key := s.getKey(txid)

	// Upload to S3
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(beefBytes),
		ContentType: aws.String("application/octet-stream"),
	})

	if err != nil {
		return fmt.Errorf("failed to put object to S3: %w", err)
	}

	return nil
}

// UpdateMerklePath updates the merkle path for a transaction by delegating to the fallback
func (s *S3BeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash, ct chaintracker.ChainTracker) ([]byte, error) {
	if s.fallback != nil {
		// Get updated BEEF from fallback
		beefBytes, err := s.fallback.UpdateMerklePath(ctx, txid, ct)
		if err != nil {
			return nil, err
		}

		// Save updated BEEF to S3
		if err := s.SaveBeef(ctx, txid, beefBytes); err != nil {
			// Log error but don't fail the operation
			// The updated BEEF is still returned even if S3 save fails
			fmt.Printf("Warning: failed to save updated BEEF to S3: %v\n", err)
		}

		return beefBytes, nil
	}
	return nil, ErrNotFound
}

// Close closes the fallback storage
func (s *S3BeefStorage) Close() error {
	if s.fallback != nil {
		return s.fallback.Close()
	}
	return nil
}

// CreateS3Config creates an AWS config with custom endpoint and/or region
func CreateS3Config(endpoint, region string) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{}

	// Set region if provided
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	// Load config
	cfg, err := config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Override endpoint if provided (for MinIO/S3-compatible services)
	if endpoint != "" {
		cfg.BaseEndpoint = aws.String(endpoint)
	}

	return cfg, nil
}

// NewS3ClientFromConfig creates an S3 client from an AWS config
func NewS3ClientFromConfig(cfg aws.Config) *s3.Client {
	return s3.NewFromConfig(cfg)
}