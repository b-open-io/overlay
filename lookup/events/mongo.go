package events

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoEventLookup struct {
	db *mongo.Database
}

// type MongoEvent

func NewMongoEventLookup(connString string, dbName string) (*MongoEventLookup, error) {
	if client, err := mongo.Connect(nil, options.Client().ApplyURI(connString)); err != nil {
		return nil, err
	} else {
		db := client.Database(dbName)
		return &MongoEventLookup{
			db: db,
		}, nil
	}
}

func (l *MongoEventLookup) SaveEvent(ctx context.Context, outpoint *overlay.Outpoint, event string, height uint32, idx uint64) error {
	return nil
}

func (l *MongoEventLookup) SaveEvents(ctx context.Context, outpoint *overlay.Outpoint, events []string, height uint32, idx uint64) error {
	return nil
}

func (l *MongoEventLookup) OutputAdded(ctx context.Context, outpoint *overlay.Outpoint, topic string, beef []byte) error {
	// Implementation for adding an output event
	return nil
}

func (l *MongoEventLookup) OutputSpent(ctx context.Context, outpoint *overlay.Outpoint, topic string, beef []byte) error {
	// Implementation for marking an output as spent
	return nil
}
func (l *MongoEventLookup) OutputDeleted(ctx context.Context, outpoint *overlay.Outpoint, topic string) error {
	// Implementation for deleting an output event
	return nil
}
func (l *MongoEventLookup) OutputBlockHeightUpdated(ctx context.Context, outpoint *overlay.Outpoint, blockHeight uint32, blockIndex uint64) error {
	// Implementation for updating the block height of an output
	return nil
}
func (l *MongoEventLookup) Lookup(ctx context.Context, question *lookup.LookupQuestion) (*lookup.LookupAnswer, error) {
	// Implementation for looking up events based on the question
	return nil, nil
}
func (l *MongoEventLookup) GetDocumentation() string {
	// Implementation for returning documentation about the event lookup service
	return "MongoDB Event Lookup Service Documentation"
}
func (l *MongoEventLookup) GetMetaData() *overlay.MetaData {
	// Implementation for returning metadata about the event lookup service
	return &overlay.MetaData{
		Name: "MongoDB Event Lookup Service",
	}
}
