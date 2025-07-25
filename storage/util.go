package storage

import (
	"strconv"

	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

func outputToMap(output *engine.Output) map[string]interface{} {
	m := make(map[string]interface{})
	m["h"] = output.BlockHeight
	m["i"] = output.BlockIdx
	m["st"] = output.Satoshis
	m["sc"] = output.Script.Bytes()
	// m["sp"] = output.Spent
	return m
}

func outputToTopicMap(output *engine.Output) map[string]interface{} {
	m := make(map[string]interface{})
	m["t"] = output.Topic
	m["sp"] = output.Spent
	if len(output.OutputsConsumed) > 0 {
		m["c"] = outpointsToBytes(output.OutputsConsumed)
	}
	if len(output.ConsumedBy) > 0 {
		m["cb"] = outpointsToBytes(output.ConsumedBy)
	}
	if len(output.AncillaryTxids) > 0 {
		m["at"] = chainhashesToBytes(output.AncillaryTxids)
	}
	m["ab"] = output.AncillaryBeef
	return m
}

func populateOutput(o *engine.Output, m map[string]string) error {
	if height, err := strconv.ParseUint(m["h"], 10, 32); err != nil {
		return err
	} else if o.BlockIdx, err = strconv.ParseUint(m["i"], 10, 64); err != nil {
		return err
	} else if o.Satoshis, err = strconv.ParseUint(m["st"], 10, 64); err != nil {
		return err
	} else {
		o.BlockHeight = uint32(height)

	}
	o.Script = script.NewFromBytes([]byte(m["sc"]))
	return nil
}

func populateOutputTopic(o *engine.Output, m map[string]string) (err error) {
	o.Topic = m["t"]
	o.Spent = m["sp"] == "1"
	o.OutputsConsumed = bytesToOutpoints([]byte(m["c"]))
	o.ConsumedBy = bytesToOutpoints([]byte(m["cb"]))
	o.AncillaryTxids = bytesToChainhashes([]byte(m["at"]))
	o.AncillaryBeef = []byte(m["ab"])
	return
}

func outpointsToBytes(outpoints []*transaction.Outpoint) []byte {
	b := make([]byte, 36*len(outpoints))
	for i, outpoint := range outpoints {
		copy(b[i*36:], outpoint.Bytes())
	}
	return b
}
func bytesToOutpoints(b []byte) []*transaction.Outpoint {
	outpoints := make([]*transaction.Outpoint, 0, len(b)/36)
	for i := 0; i < len(b); i += 36 {
		outpoints = append(outpoints, transaction.NewOutpointFromBytes([36]byte(b[i:i+36])))
	}
	return outpoints
}
func chainhashesToBytes(hashes []*chainhash.Hash) []byte {
	b := make([]byte, 32*len(hashes))
	for i, hash := range hashes {
		copy(b[i*32:], hash.CloneBytes())
	}
	return b
}
func bytesToChainhashes(b []byte) []*chainhash.Hash {
	hashes := make([]*chainhash.Hash, 0, len(b)/32)
	for i := 0; i < len(b); i += 32 {
		if txid, err := chainhash.NewHash(b[i : i+32]); err != nil {
			return nil
		} else {
			hashes = append(hashes, txid)
		}
	}
	return hashes
}

type BSONBeef struct {
	Txid   string   `bson:"_id"`
	Beef   []byte   `bson:"beef"`
	Topics []string `bson:"topics"`
}

type BSONOutput struct {
	Outpoint        string   `bson:"outpoint"`
	Txid            string   `bson:"txid"`
	Topic           string   `bson:"topic"`
	Script          []byte   `bson:"script"`
	Satoshis        uint64   `bson:"satoshis"`
	Spent           bool     `bson:"spent"`
	OutputsConsumed []string `bson:"outputsConsumed"`
	ConsumedBy      []string `bson:"consumedBy"`
	BlockHeight     uint32   `bson:"blockHeight"`
	BlockIdx        uint64   `bson:"blockIdx"`
	Score           float64  `bson:"score"`
	AncillaryTxids  []string `bson:"ancillaryTxids"`
	AncillaryBeef   []byte   `bson:"ancillaryBeef"`
}

func NewBSONOutput(o *engine.Output) *BSONOutput {
	bo := &BSONOutput{
		Outpoint:        o.Outpoint.String(),
		Txid:            o.Outpoint.Txid.String(),
		Topic:           o.Topic,
		Script:          o.Script.Bytes(),
		Satoshis:        o.Satoshis,
		Spent:           o.Spent,
		BlockHeight:     o.BlockHeight,
		BlockIdx:        o.BlockIdx,
		Score:           o.Score,
		AncillaryTxids:  make([]string, 0, len(o.AncillaryTxids)),
		AncillaryBeef:   o.AncillaryBeef,
		OutputsConsumed: make([]string, 0, len(o.OutputsConsumed)),
		ConsumedBy:      make([]string, 0, len(o.ConsumedBy)),
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
		Script:          script.NewFromBytes(o.Script),
		Satoshis:        o.Satoshis,
		Spent:           o.Spent,
		BlockHeight:     o.BlockHeight,
		BlockIdx:        o.BlockIdx,
		Score:           o.Score,
		AncillaryTxids:  make([]*chainhash.Hash, 0, len(o.AncillaryTxids)),
		AncillaryBeef:   o.AncillaryBeef,
		OutputsConsumed: make([]*transaction.Outpoint, 0, len(o.OutputsConsumed)),
		ConsumedBy:      make([]*transaction.Outpoint, 0, len(o.ConsumedBy)),
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
