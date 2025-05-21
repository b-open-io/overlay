package storage

import (
	"context"
	"encoding/base64"
	"log"
	"strings"
	"time"

	"github.com/4chain-ag/go-overlay-services/pkg/core/engine"
	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/publish"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/redis/go-redis/v9"
)

type RedisStorage struct {
	DB        *redis.Client
	BeefStore beef.BeefStorage
	pub       publish.Publisher
}

func NewRedisStorage(connString string, beefStore beef.BeefStorage, pub publish.Publisher) (r *RedisStorage, err error) {
	r = &RedisStorage{BeefStore: beefStore}
	log.Println("Connecting to Redis Storage...", connString)
	if opts, err := redis.ParseURL(connString); err != nil {
		return nil, err
	} else {
		r.DB = redis.NewClient(opts)
		return r, nil
	}
}

func (s *RedisStorage) InsertOutput(ctx context.Context, utxo *engine.Output) (err error) {
	if err := s.BeefStore.SaveBeef(ctx, &utxo.Outpoint.Txid, utxo.Beef); err != nil {
		return err
	}
	_, err = s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		op := utxo.Outpoint.String()
		if err := p.HMSet(ctx, OutputTopicKey(&utxo.Outpoint, utxo.Topic), outputToTopicMap(utxo)).Err(); err != nil {
			return err
		} else if err := p.HMSet(ctx, outputKey(&utxo.Outpoint), outputToMap(utxo)).Err(); err != nil {
			return err
		} else {
			var score float64
			if utxo.BlockHeight > 0 {
				score = float64(utxo.BlockHeight)*1e9 + float64(utxo.BlockIdx)
			} else {
				score = float64(time.Now().UnixNano())
			}
			if err = p.ZAdd(ctx, OutMembershipKey(utxo.Topic), redis.Z{
				Score:  score,
				Member: op,
			}).Err(); err != nil {
				return err
			}
		}
		if s.pub != nil {
			s.pub.Publish(ctx, utxo.Topic, base64.StdEncoding.EncodeToString(utxo.Beef))
		}
		return nil
	})
	return err
}

func (s *RedisStorage) FindOutput(ctx context.Context, outpoint *overlay.Outpoint, topic *string, spent *bool, includeBEEF bool) (o *engine.Output, err error) {
	o = &engine.Output{
		Outpoint: *outpoint,
	}
	if o.Spent, err = s.DB.HExists(ctx, SpendsKey, outpoint.String()).Result(); err != nil {
		return nil, err
	} else if spent != nil && *spent != o.Spent {
		return nil, nil
	}
	if topic != nil {
		otKey := OutputTopicKey(outpoint, *topic)
		if tm, err := s.DB.HGetAll(ctx, otKey).Result(); err == redis.Nil {
			return nil, nil
		} else if err != nil {
			return nil, err
		} else if len(tm) == 0 {
			return nil, nil
		} else if err := populateOutputTopic(o, tm); err != nil {
			return nil, err
		}
	}

	if m, err := s.DB.HGetAll(ctx, outputKey(outpoint)).Result(); err != nil {
		return nil, err
	} else if len(m) == 0 {
		return nil, nil
	} else if err := populateOutput(o, m); err != nil {
		return nil, err
	}
	if includeBEEF {
		if o.Beef, err = s.BeefStore.LoadBeef(ctx, &outpoint.Txid); err != nil {
			return nil, err
		}
	}
	return
}

func (s *RedisStorage) FindOutputs(ctx context.Context, outpoints []*overlay.Outpoint, topic string, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
	outputs := make([]*engine.Output, 0, len(outpoints))
	for _, outpoint := range outpoints {
		if output, err := s.FindOutput(ctx, outpoint, &topic, spent, includeBEEF); err != nil {
			return nil, err
		} else {
			outputs = append(outputs, output)
		}
	}
	return outputs, nil
}

func (s *RedisStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
	iter := s.DB.Scan(ctx, 0, "ot:"+txid.String()+"*", 0).Iterator()
	var outputs []*engine.Output
	for iter.Next(ctx) {
		parts := strings.Split(iter.Val(), ":")
		if outpoint, err := overlay.NewOutpointFromString(parts[1]); err != nil {
			return nil, err
		} else {
			topic := parts[2]
			if output, err := s.FindOutput(ctx, outpoint, &topic, nil, includeBEEF); err != nil {
				return nil, err
			} else if output != nil {
				outputs = append(outputs, output)
			}
		}
	}
	return outputs, nil
}

func (s *RedisStorage) FindUTXOsForTopic(ctx context.Context, topic string, since uint32, includeBEEF bool) ([]*engine.Output, error) {
	if outpoints, err := s.DB.ZRangeByScore(ctx, OutMembershipKey(topic), &redis.ZRangeBy{
		Min: "0",
		Max: "inf",
	}).Result(); err != nil {
		return nil, err
	} else {
		outputs := make([]*engine.Output, 0, len(outpoints))
		for _, outpointStr := range outpoints {
			if outpoint, err := overlay.NewOutpointFromString(outpointStr); err != nil {
				return nil, err
			} else if output, err := s.FindOutput(ctx, outpoint, &topic, nil, includeBEEF); err != nil {
				return nil, err
			} else if output != nil {
				outputs = append(outputs, output)
			}
		}
		return outputs, nil
	}
}

func (s *RedisStorage) DeleteOutput(ctx context.Context, outpoint *overlay.Outpoint, topic string) error {
	_, err := s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		if err := p.Del(ctx, OutputTopicKey(outpoint, topic)).Err(); err != nil {
			return err
		} else if err = p.ZRem(ctx, OutMembershipKey(topic), outpoint.String()).Err(); err != nil {
			return err
		}
		iter := p.Scan(ctx, 0, "ot:"+outpoint.String()+":*", 0).Iterator()
		if !iter.Next(ctx) {
			if err := p.Del(ctx, outputKey(outpoint)).Err(); err != nil {
				return err
				// } else if p.HSetNX(ctx, "beef", beefKey(&utxo.Outpoint.Txid), utxo.Beef).Err(); err != nil {
				// 	return err
			}
		}
		return nil
	})
	return err
}

// func (s *RedisStorage) DeleteOutputs(ctx context.Context, outpoints []*overlay.Outpoint, topic string) error {
// 	for _, outpoint := range outpoints {
// 		if err := s.DeleteOutput(ctx, outpoint, topic); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func (s *RedisStorage) MarkUTXOAsSpent(ctx context.Context, outpoint *overlay.Outpoint, topic string, beef []byte) error {
	if _, _, spendTxid, err := transaction.ParseBeef(beef); err != nil {
		return err
	} else {
		return s.DB.HSet(ctx, SpendsKey, outpoint.String(), spendTxid.String()).Err()
	}
}

func (s *RedisStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*overlay.Outpoint, topic string, spendTxid *chainhash.Hash) error {
	values := make(map[string]interface{}, len(outpoints)*2)
	for _, outpoint := range outpoints {
		values[outpoint.String()] = spendTxid.String()
	}

	return s.DB.HSet(ctx, SpendsKey, values).Err()
}

func (s *RedisStorage) UpdateConsumedBy(ctx context.Context, outpoint *overlay.Outpoint, topic string, consumedBy []*overlay.Outpoint) error {
	return s.DB.HSet(ctx, OutputTopicKey(outpoint, topic), "cb", outpointsToBytes(consumedBy)).Err()
}

func (s *RedisStorage) UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error {
	return s.BeefStore.SaveBeef(ctx, txid, beef)
}

func (s *RedisStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *overlay.Outpoint, topic string, blockHeight uint32, blockIndex uint64, ancelliaryBeef []byte) error {
	_, err := s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		if err := p.ZAdd(ctx, OutMembershipKey(topic), redis.Z{
			Score:  float64(blockHeight)*1e9 + float64(blockIndex),
			Member: outpoint.String(),
		}).Err(); err != nil {
			return err
		} else if err := p.HSet(ctx, OutputTopicKey(outpoint, topic), "h", blockHeight, "i", blockIndex).Err(); err != nil {
			return err
		}
		return nil

	})
	return err
}

func (s *RedisStorage) InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error {
	return s.DB.ZAdd(ctx, TxMembershipKey(tx.Topic), redis.Z{
		Member: tx.Txid.String(),
		Score:  float64(time.Now().UnixNano()),
	}).Err()
}

func (s *RedisStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	if _, err := s.DB.ZScore(ctx, TxMembershipKey(tx.Topic), tx.Txid.String()).Result(); err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (s *RedisStorage) Close() error {
	return s.DB.Close()
}
