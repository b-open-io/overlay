package storage

import (
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// OutputMetadata stores auxiliary information about an output as JSON
// This is used by both SQLite (as JSON TEXT) and PostgreSQL (as JSONB)
type OutputMetadata struct {
	AncillaryTxids  []string `json:"txids,omitempty"`
	OutputsConsumed []string `json:"consumes,omitempty"`
	ConsumedBy      []string `json:"consumed_by,omitempty"`
}

type BSONBeef struct {
	Txid   string   `bson:"_id"`
	Beef   []byte   `bson:"beef"`
	Topics []string `bson:"topics"`
}

type BSONOutput struct {
	Outpoint        string             `bson:"outpoint"`
	Txid            string             `bson:"txid"`
	Vout            uint32             `bson:"vout"`
	Topic           string             `bson:"topic"`
	Spend           *string            `bson:"spend"` // Changed from Spent bool to Spend *string
	OutputsConsumed []string           `bson:"outputsConsumed"`
	ConsumedBy      []string           `bson:"consumedBy"`
	BlockHeight     uint32             `bson:"blockHeight"`
	BlockIdx        uint64             `bson:"blockIdx"`
	Score           float64            `bson:"score"`
	AncillaryTxids  []string           `bson:"ancillaryTxids"`
	Events          []string           `bson:"events"`         // Event names this output is associated with
	Data            interface{}        `bson:"data,omitempty"` // Arbitrary data associated with the output
	MerkleRoot      *string            `bson:"merkleRoot,omitempty"`
	MerkleState     engine.MerkleState `bson:"merkleValidationState"`
}

func NewBSONOutput(o *engine.Output) *BSONOutput {
	bo := &BSONOutput{
		Outpoint:        o.Outpoint.String(),
		Txid:            o.Outpoint.Txid.String(),
		Vout:            o.Outpoint.Index,
		Topic:           o.Topic,
		Spend:           nil, // Will be set when output is spent
		BlockHeight:     o.BlockHeight,
		BlockIdx:        o.BlockIdx,
		Score:           o.Score,
		AncillaryTxids:  make([]string, 0, len(o.AncillaryTxids)),
		OutputsConsumed: make([]string, 0, len(o.OutputsConsumed)),
		ConsumedBy:      make([]string, 0, len(o.ConsumedBy)),
		Events:          []string{o.Topic}, // Initialize with the topic as an event
		Data:            nil,               // Initialize empty data
		MerkleState:     o.MerkleState,
	}

	// Set merkle root if present
	if o.MerkleRoot != nil {
		merkleRootStr := o.MerkleRoot.String()
		bo.MerkleRoot = &merkleRootStr
	}
	for _, oc := range o.OutputsConsumed {
		bo.OutputsConsumed = append(bo.OutputsConsumed, oc.String())
	}
	for _, cb := range o.ConsumedBy {
		bo.ConsumedBy = append(bo.ConsumedBy, cb.String())
	}
	for _, at := range o.AncillaryTxids {
		bo.AncillaryTxids = append(bo.AncillaryTxids, at.String())
	}
	return bo
}

func (o *BSONOutput) ToEngineOutput() *engine.Output {
	outpoint, _ := transaction.OutpointFromString(o.Outpoint)
	output := &engine.Output{
		Outpoint:        *outpoint,
		Topic:           o.Topic,
		Spent:           o.Spend != nil, // Set to true if Spend field has a value
		BlockHeight:     o.BlockHeight,
		BlockIdx:        o.BlockIdx,
		Score:           o.Score,
		AncillaryTxids:  make([]*chainhash.Hash, 0, len(o.AncillaryTxids)),
		OutputsConsumed: make([]*transaction.Outpoint, 0, len(o.OutputsConsumed)),
		ConsumedBy:      make([]*transaction.Outpoint, 0, len(o.ConsumedBy)),
		MerkleState:     o.MerkleState,
	}

	// Convert merkle root if present
	if o.MerkleRoot != nil && *o.MerkleRoot != "" {
		merkleRoot, _ := chainhash.NewHashFromHex(*o.MerkleRoot)
		output.MerkleRoot = merkleRoot
	}
	for _, oc := range o.OutputsConsumed {
		op, _ := transaction.OutpointFromString(oc)
		output.OutputsConsumed = append(output.OutputsConsumed, op)
	}
	for _, cb := range o.ConsumedBy {
		op, _ := transaction.OutpointFromString(cb)
		output.ConsumedBy = append(output.ConsumedBy, op)
	}
	for _, at := range o.AncillaryTxids {
		txid, _ := chainhash.NewHashFromHex(at)
		output.AncillaryTxids = append(output.AncillaryTxids, txid)
	}

	return output
}
