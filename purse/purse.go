package purse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	feemodel "github.com/bsv-blockchain/go-sdk/transaction/fee_model"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/redis/go-redis/v9"
)

const SATS_PER_KB = uint64(10)

type PayPurse struct {
	pk            *ec.PrivateKey
	db            *redis.Client
	SatsPerKb     uint64
	Address       *script.Address
	LockingScript *script.Script
	unlock        *p2pkh.P2PKH
	ChangeSplits  uint8
	utxoKey       string
}

func NewPayPurse(connString, wif string) (p *PayPurse, err error) {
	p = &PayPurse{}
	if p.pk, err = ec.PrivateKeyFromWif(wif); err != nil {
		return nil, err
	} else if p.Address, err = script.NewAddressFromPublicKey(p.pk.PubKey(), true); err != nil {
		return nil, err
	} else if p.LockingScript, err = p2pkh.Lock(p.Address); err != nil {
		return nil, err
	} else if p.unlock, err = p2pkh.Unlock(p.pk, nil); err != nil {
		return nil, err
	} else if opts, err := redis.ParseURL(connString); err != nil {
		return nil, err
	} else {
		p.db = redis.NewClient(opts)
		p.utxoKey = "u:" + p.Address.AddressString
	}
	return
}

func (p *PayPurse) UpdateFromTx(ctx context.Context, tx *transaction.Transaction) error {
	for _, txin := range tx.Inputs {
		outpoint := &transaction.Outpoint{
			Txid:  *txin.SourceTXID,
			Index: txin.SourceTxOutIndex,
		}
		if err := p.db.ZRem(ctx, p.utxoKey, outpoint.String()).Err(); err != nil {
			return err
		}
	}
	txid := tx.TxID()
	for vout, txout := range tx.Outputs {
		outpoint := &transaction.Outpoint{
			Txid:  *txid,
			Index: uint32(vout),
		}
		if bytes.Equal(*txout.LockingScript, *p.LockingScript) {
			if err := p.db.ZAdd(ctx, p.utxoKey, redis.Z{
				Score:  float64(txout.Satoshis),
				Member: outpoint.String(),
			}).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *PayPurse) FundAndSign(ctx context.Context, tx *transaction.Transaction, fundOutputs bool) error {
	feeModel := &feemodel.SatoshisPerKilobyte{Satoshis: p.SatsPerKb}
	satsIn := uint64(0)
	for _, txin := range tx.Inputs {
		sourceOutput := txin.SourceTxOutput()
		if sourceOutput == nil {
			return transaction.ErrEmptyPreviousTx
		}
		satsIn += sourceOutput.Satoshis
	}
	satsOut := uint64(0)
	for _, txout := range tx.Outputs {
		satsOut += txout.Satoshis
	}
	if fundOutputs && satsIn < satsOut {
		return transaction.ErrInsufficientInputs
	}
	fee, err := feeModel.ComputeFee(tx)
	if err != nil {
		log.Panicln(err)
	}
	for satsIn < satsOut+fee {
		if utxos, err := p.LockUtxos(ctx, satsOut+fee+10); err != nil {
			log.Panicln(err)
		} else {
			for _, u := range utxos {
				tx.AddInputsFromUTXOs()
				satsIn += u.Satoshis
			}
		}
	}
	for range p.ChangeSplits {
		tx.AddOutput(&transaction.TransactionOutput{
			LockingScript: p.LockingScript,
			Change:        true,
		})
	}
	if err := tx.Fee(feeModel, transaction.ChangeDistributionEqual); err != nil {
		log.Println(err)
		return err
	} else if err := tx.Sign(); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (p *PayPurse) LockUtxos(ctx context.Context, satoshis uint64) ([]*transaction.UTXO, error) {
	results, err := p.db.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:     p.utxoKey,
		ByScore: true,
		Rev:     true,
		Count:   25,
	}).Result()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	utxos := make([]*transaction.UTXO, 0, len(results))
	collected := uint64(0)
	for _, result := range results {
		if collected >= satoshis {
			break
		}
		op := result.Member.(string)
		if outpoint, err := transaction.OutpointFromString(op); err != nil {
			log.Panicln(err)
		} else if locked, err := p.db.SetNX(ctx, "lock:"+op, time.Now().Unix(), time.Minute).Result(); err != nil {
			log.Panic(err)
		} else if locked {
			utxos = append(utxos, &transaction.UTXO{
				TxID:                    &outpoint.Txid,
				Vout:                    outpoint.Index,
				LockingScript:           p.LockingScript,
				Satoshis:                uint64(result.Score),
				UnlockingScriptTemplate: p.unlock,
			})
			collected += uint64(result.Score)
		}
	}
	if collected < satoshis {
		return nil, transaction.ErrInsufficientFunds
	}
	return utxos, nil
}

type WOCResponse struct {
	Error  string    `json:"error"`
	Result []WOCUtxo `json:"result"`
}

type WOCUtxo struct {
	TxPos   uint32 `json:"tx_pos"`
	TxHash  string `json:"tx_hash"`
	Value   uint64 `json:"value"`
	IsSpent bool   `json:"isSpentInMempoolTx"`
	Status  string `json:"status"`
}

func (p *PayPurse) Balance(ctx context.Context) (bal uint64, count int, err error) {
	if results, err := p.db.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:     p.utxoKey,
		ByScore: true,
		Rev:     true,
		Count:   25,
	}).Result(); err == nil {
		for _, result := range results {
			bal += uint64(result.Score)
			count++
		}
	}
	return
}
func (p *PayPurse) RefreshBalance(ctx context.Context) (bal uint64, count int, err error) {
	if resp, err := http.Get("https://api.whatsonchain.com/v1/bsv/main/address/" + p.Address.AddressString + "/unspent/all"); err != nil {
		log.Println(err)
		return 0, 0, err
	} else if resp.StatusCode != http.StatusOK {
		log.Println("Error: ", resp.Status)
		return 0, 0, transaction.ErrEmptyPreviousTx
	} else if resp.Body != nil {
		defer resp.Body.Close()
		var response WOCResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			log.Println(err)
			return 0, 0, err
		}
		if p.db.Del(ctx, p.utxoKey).Err() != nil {
			log.Println(err)
		}
		for _, u := range response.Result {
			if u.IsSpent {
				continue
			} else if err := p.db.ZAdd(ctx, p.utxoKey, redis.Z{
				Score:  float64(u.Value),
				Member: fmt.Sprintf("%s.%d", u.TxHash, u.TxPos),
			}).Err(); err != nil {
				log.Println(err)
			} else {
				bal += u.Value
				count++
			}
		}
	}
	return
}
