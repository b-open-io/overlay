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
)

type S3BeefStorage struct {
	client *s3.Client
	bucket string
}

// NewS3BeefStorage creates a new S3-based BEEF storage
func NewS3BeefStorage(bucket string) (*S3BeefStorage, error) {
	// Load the default AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3BeefStorage{
		client: client,
		bucket: bucket,
	}, nil
}

// NewS3BeefStorageWithClient creates a new S3-based BEEF storage with a provided client
func NewS3BeefStorageWithClient(client *s3.Client, bucket string) *S3BeefStorage {
	return &S3BeefStorage{
		client: client,
		bucket: bucket,
	}
}

// getKey returns the S3 key for a given txid
// Uses subdirectories based on first 2 chars of txid to avoid too many files in one prefix
func (s *S3BeefStorage) getKey(txid *chainhash.Hash) string {
	txidStr := txid.String()
	// Create subdirectory structure: xx/xxxxxxxxxxxx.beef
	subDir := txidStr[:2]
	fileName := txidStr + ".beef"
	return subDir + "/" + fileName
}

func (s *S3BeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	key := s.getKey(txid)

	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nfe *types.NoSuchKey
		if errors.As(err, &nfe) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("S3 Get failed (bucket: %s, key: %s): %w", s.bucket, key, err)
	}

	defer result.Body.Close()
	beefBytes, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read S3 object: %w", err)
	}

	return beefBytes, nil
}

func (s *S3BeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	key := s.getKey(txid)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(beefBytes),
		ContentType: aws.String("application/octet-stream"),
	})

	if err != nil {
		return fmt.Errorf("S3 Put failed (bucket: %s, key: %s): %w", s.bucket, key, err)
	}

	return nil
}

// UpdateMerklePath is not supported by S3 storage
func (s *S3BeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return nil, nil
}

// Close is a no-op for S3 storage
func (s *S3BeefStorage) Close() error {
	return nil
}

// CreateS3Config creates an AWS config with custom endpoint, region, and/or credentials
func CreateS3Config(endpoint, region, accessKey, secretKey string) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{}

	// Set region - default to "auto" for S3-compatible services if not provided
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	} else if endpoint != "" {
		// For custom endpoints (like R2), we need to set a region even if it's not used
		opts = append(opts, config.WithRegion("auto"))
	}

	// Set credentials if provided
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretKey,
				}, nil
			}),
		))
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