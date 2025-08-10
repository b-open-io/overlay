package beef

import (
	"context"
	"io"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker/headers_client"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
)

type MongoBeefStorage struct {
	BaseBeefStorage
	db     *mongo.Database
	bucket *mongo.GridFSBucket
}

func NewMongoBeefStorage(connString string) (*MongoBeefStorage, error) {
	client, err := mongo.Connect(nil, options.Client().ApplyURI(connString))
	if err != nil {
		return nil, err
	}
	
	// Extract database name from the connection string
	// MongoDB connection strings can include the database as: mongodb://host/database
	dbName := "beef" // default
	if cs, err := connstring.ParseAndValidate(connString); err == nil && cs.Database != "" {
		dbName = cs.Database
	}
	
	db := client.Database(dbName)
	return &MongoBeefStorage{
		db:     db,
		bucket: db.GridFSBucket(),
	}, nil
}

func (t *MongoBeefStorage) LoadBeef(ctx context.Context, txid *chainhash.Hash) ([]byte, error) {
	if t.db.Collection("fs.files").FindOne(ctx, bson.M{"_id": txid.String()}, options.FindOne().SetProjection(bson.M{"_id": 1})).Err() == nil {
		if downloadStream, err := t.bucket.OpenDownloadStream(ctx, txid.String()); err == nil {
			defer downloadStream.Close()
			return io.ReadAll(downloadStream)
		}
	}
	beefBytes, err := t.BaseBeefStorage.LoadBeef(ctx, txid)
	if err == nil {
		t.SaveBeef(ctx, txid, beefBytes)
	}
	return beefBytes, err
}

func (t *MongoBeefStorage) SaveBeef(ctx context.Context, txid *chainhash.Hash, beefBytes []byte) error {
	t.bucket.Delete(ctx, txid.String()) // Remove any existing file with the same ID
	// t.db.Collection("fs.chunks").DeleteMany(ctx, bson.M{"files_id": txid.String()})
	txidStr := txid.String()
	if uploadStream, err := t.bucket.OpenUploadStreamWithID(
		ctx,
		txidStr, // Use txid as the file ID
		txidStr,
	); err != nil {
		return err
	} else {
		defer uploadStream.Close()
		_, err = uploadStream.Write(beefBytes)
		return err
	}
}

// LoadTx loads a transaction from BEEF storage with optional merkle path validation
func (t *MongoBeefStorage) LoadTx(ctx context.Context, txid *chainhash.Hash, chaintracker *headers_client.Client) (*transaction.Transaction, error) {
	// Load BEEF from storage - this will use MongoBeefStorage.LoadBeef
	beefBytes, err := t.LoadBeef(ctx, txid)
	if err != nil {
		return nil, err
	}

	return LoadTxFromBeef(ctx, beefBytes, txid, chaintracker)
}
