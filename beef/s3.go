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

func (s *S3BeefStorage) Get(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	// Get assembles BEEF from separate tx and proof storage
	rawTx, err := s.GetRawTx(ctx, txid)
	if err != nil {
		return nil, err
	}

	// Try to get proof (optional)
	proof, _ := s.GetProof(ctx, txid)

	return assembleBEEF(txid, rawTx, proof)
}

func (s *S3BeefStorage) Put(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	// Put splits BEEF into separate tx and proof storage
	rawTx, proof, err := splitBEEF(beefBytes)
	if err != nil {
		return err
	}

	if err := s.putRawTx(ctx, txid, rawTx); err != nil {
		return err
	}

	if len(proof) > 0 {
		if err := s.putProof(ctx, txid, proof); err != nil {
			return err
		}
	}

	return nil
}

// UpdateMerklePath is not supported by S3 storage
func (s *S3BeefStorage) UpdateMerklePath(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	return nil, nil
}

// getTxKey returns the S3 key for raw transaction storage
func (s *S3BeefStorage) getTxKey(txid *chainhash.Hash) string {
	txidStr := txid.String()
	subDir := txidStr[:2]
	return subDir + "/" + txidStr + ".tx"
}

// getProofKey returns the S3 key for proof storage
func (s *S3BeefStorage) getProofKey(txid *chainhash.Hash) string {
	txidStr := txid.String()
	subDir := txidStr[:2]
	return subDir + "/" + txidStr + ".prf"
}

// GetRawTx loads just the raw transaction bytes from S3
func (s *S3BeefStorage) GetRawTx(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	key := s.getTxKey(txid)

	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nfe *types.NoSuchKey
		if errors.As(err, &nfe) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("S3 Get tx failed: %w", err)
	}

	defer result.Body.Close()
	return io.ReadAll(result.Body)
}

// putRawTx stores raw transaction bytes to S3
func (s *S3BeefStorage) putRawTx(ctx context.Context, txid *chainhash.Hash, rawTx []byte) error {
	key := s.getTxKey(txid)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(rawTx),
		ContentType: aws.String("application/octet-stream"),
	})

	return err
}

// GetProof loads just the merkle proof bytes from S3
func (s *S3BeefStorage) GetProof(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	key := s.getProofKey(txid)

	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		var nfe *types.NoSuchKey
		if errors.As(err, &nfe) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("S3 Get proof failed: %w", err)
	}

	defer result.Body.Close()
	return io.ReadAll(result.Body)
}

// putProof stores merkle proof bytes to S3
func (s *S3BeefStorage) putProof(ctx context.Context, txid *chainhash.Hash, proof []byte) error {
	key := s.getProofKey(txid)

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(proof),
		ContentType: aws.String("application/octet-stream"),
	})

	return err
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