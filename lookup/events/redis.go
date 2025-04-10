package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/4chain-ag/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/redis/go-redis/v9"
)

type RedisEventLookup struct {
	Db      *redis.Client
	Storage engine.Storage
	Topic   string
}

func EventKey(event string) string {
	return "ev:" + event
}

func OutpointEventsKey(outpoint *overlay.Outpoint) string {
	return "oe:" + outpoint.String()
}

func NewRedisEventLookup(connString string, storage engine.Storage, topic string) (*RedisEventLookup, error) {
	r := &RedisEventLookup{
		Storage: storage,
		Topic:   topic,
	}
	if opts, err := redis.ParseURL(connString); err != nil {
		return nil, err
	} else {
		r.Db = redis.NewClient(opts)
		return r, nil
	}
}

func (l *RedisEventLookup) SaveEvent(ctx context.Context, outpoint *overlay.Outpoint, event string, height uint32, idx uint64) error {
	var score float64
	if height > 0 {
		score = float64(height)*1e9 + float64(idx)
	} else {
		score = float64(time.Now().UnixNano())
	}
	_, err := l.Db.Pipelined(ctx, func(p redis.Pipeliner) error {
		op := outpoint.String()
		if err := p.ZAdd(ctx, EventKey(event), redis.Z{
			Score:  score,
			Member: op,
		}).Err(); err != nil {
			return err
		} else if err := p.SAdd(ctx, OutpointEventsKey(outpoint), event).Err(); err != nil {
			return err
		}
		p.Publish(ctx, event, fmt.Sprintf("%f:%s", score, op))
		return nil
	})
	return err

}
func (l *RedisEventLookup) SaveEvents(ctx context.Context, outpoint *overlay.Outpoint, events []string, height uint32, idx uint64) error {
	var score float64
	if height > 0 {
		score = float64(height)*1e9 + float64(idx)
	} else {
		score = float64(time.Now().UnixNano())
	}
	op := outpoint.String()
	_, err := l.Db.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, event := range events {
			if err := p.ZAdd(ctx, EventKey(event), redis.Z{
				Score:  score,
				Member: op,
			}).Err(); err != nil {
				return err
			} else if err := p.SAdd(ctx, OutpointEventsKey(outpoint), event).Err(); err != nil {
				return err
			}
			p.Publish(ctx, event, op)
		}
		return nil
	})
	return err
}
func (l *RedisEventLookup) Close() {
	if l.Db != nil {
		l.Db.Close()
	}
}
func (l *RedisEventLookup) LookupOutpoints(ctx context.Context, question *Question) (outputs []*overlay.Outpoint, err error) {
	startScore := float64(question.From.Height)*1e9 + float64(question.From.Idx)
	var ops []string
	if len(question.Events) > 0 {
		join := JoinTypeIntersect
		if question.JoinType != nil {
			join = *question.JoinType
		}
		keys := make([]string, len(question.Events))
		for _, event := range question.Events {
			keys = append(keys, EventKey(event))
		}
		var results []redis.Z
		switch join {
		case JoinTypeIntersect:
			results, err = l.Db.ZInterWithScores(ctx, &redis.ZStore{
				Aggregate: "MIN",
				Keys:      keys,
			}).Result()
		case JoinTypeUnion:
			results, err = l.Db.ZUnionWithScores(ctx, redis.ZStore{
				Aggregate: "MIN",
				Keys:      keys,
			}).Result()
		case JoinTypeDifference:
			results, err = l.Db.ZDiffWithScores(ctx, keys...).Result()
		default:
			return nil, errors.New("invalid join type")
		}
		if err != nil {
			return nil, err
		}
		slices.SortFunc(results, func(a, b redis.Z) int {
			if question.Reverse {
				if a.Score > b.Score {
					return 1
				} else if a.Score < b.Score {
					return -1
				}
			} else {
				if a.Score < b.Score {
					return 1
				} else if a.Score > b.Score {
					return -1
				}
			}
			return 0
		})
		for _, item := range results {
			if question.Reverse && item.Score < startScore {
				ops = append(ops, item.Member.(string))
			} else if !question.Reverse && item.Score > startScore {
				ops = append(ops, item.Member.(string))
			}
		}
	} else if question.Event != "" {
		query := redis.ZRangeArgs{
			Key:     EventKey(question.Event),
			Start:   fmt.Sprintf("(%f", startScore),
			Stop:    "+inf",
			ByScore: true,
			Rev:     question.Reverse,
		}
		if ops, err = l.Db.ZRangeArgs(ctx, query).Result(); err != nil {
			return nil, err
		}
	}

	// outpoints := make([]*overlay.Outpoint, 0, len(ops))
	members := make([]any, len(ops))
	for _, op := range ops {
		members = append(members, op)
	}
	results := make([]*overlay.Outpoint, 0, len(ops))
	if question.Spent != nil {
		if spent, err := l.Db.SMIsMember(ctx, "spends", members...).Result(); err != nil {
			return nil, err
		} else {
			for i, op := range ops {
				if spent[i] == *question.Spent {
					if question.Limit > 0 && len(ops) >= question.Limit {
						break
					}
					if outpoint, err := overlay.NewOutpointFromString(op); err != nil {
						return nil, err
					} else {
						results = append(results, outpoint)
					}
				}
			}
		}
	}
	return results, nil
}

func (l *RedisEventLookup) LookupOutputs(ctx context.Context, question *Question) (outputs []*engine.Output, err error) {
	outpoints, err := l.LookupOutpoints(ctx, question)
	if err != nil {
		return nil, err
	}
	if len(outpoints) == 0 {
		return nil, nil
	}
	results, err := l.Storage.FindOutputs(ctx, outpoints, &l.Topic, nil, true)
	if err != nil {
		return nil, err
	}
	outputs = make([]*engine.Output, 0, len(results))
	for _, output := range results {
		if output != nil {
			outputs = append(outputs, output)
		}
	}

	return outputs, nil
}

func (l *RedisEventLookup) Lookup(ctx context.Context, q *lookup.LookupQuestion) (answer *lookup.LookupAnswer, err error) {
	question := &Question{}
	if err := json.Unmarshal(q.Query, question); err != nil {
		return nil, err
	}
	outputs, err := l.LookupOutputs(ctx, question)
	if err != nil {
		return nil, err
	}

	answer = &lookup.LookupAnswer{
		Type: lookup.AnswerTypeOutputList,
	}

	for _, output := range outputs {
		if beef, _, _, err := transaction.ParseBeef(output.Beef); err != nil {
			return nil, err
		} else {
			if len(output.AncillaryBeef) > 0 {
				if err = beef.MergeBeefBytes(output.AncillaryBeef); err != nil {
					return nil, err
				}
			}
			if beefBytes, err := beef.AtomicBytes(&output.Outpoint.Txid); err != nil {
				return nil, err
			} else {
				answer.Outputs = append(answer.Outputs, &lookup.OutputListItem{
					OutputIndex: output.Outpoint.OutputIndex,
					Beef:        beefBytes,
				})
			}
		}
	}
	return answer, nil
}

func (l *RedisEventLookup) FindEvents(ctx context.Context, outpoint *overlay.Outpoint) ([]string, error) {
	if events, err := l.Db.SMembers(ctx, OutpointEventsKey(outpoint)).Result(); err != nil {
		return nil, err
	} else {
		return events, nil
	}
}

func (l *RedisEventLookup) OutputSpent(ctx context.Context, outpoint *overlay.Outpoint, _ string) error {
	return l.Db.SAdd(ctx, "spends", outpoint.String()).Err()
}

func (l *RedisEventLookup) OutputsSpent(ctx context.Context, outpoints []*overlay.Outpoint, _ string) error {
	args := make([]interface{}, 0, len(outpoints))
	for _, outpoint := range outpoints {
		args = append(args, outpoint.Bytes())
	}
	return l.Db.SAdd(ctx, "spends", args...).Err()
}

func (l *RedisEventLookup) OutputDeleted(ctx context.Context, outpoint *overlay.Outpoint, topic string) error {
	op := outpoint.String()
	if events, err := l.Db.SMembers(ctx, OutpointEventsKey(outpoint)).Result(); err != nil {
		return err
	} else if len(events) == 0 {
		return nil
	} else {
		_, err := l.Db.Pipelined(ctx, func(p redis.Pipeliner) error {
			for _, event := range events {
				if err := p.ZRem(ctx, EventKey(event), op).Err(); err != nil {
					return err
				}
			}
			return p.Del(ctx, OutpointEventsKey(outpoint)).Err()
		})
		return err
	}
}

func (l *RedisEventLookup) OutputBlockHeightUpdated(ctx context.Context, outpoint *overlay.Outpoint, height uint32, idx uint64) error {
	var score float64
	if height > 0 {
		score = float64(height)*1e9 + float64(idx)
	} else {
		score = float64(time.Now().UnixNano())
	}
	op := outpoint.String()
	if events, err := l.Db.SMembers(ctx, OutpointEventsKey(outpoint)).Result(); err != nil {
		return err
	} else if len(events) == 0 {
		return nil
	} else {
		_, err := l.Db.Pipelined(ctx, func(p redis.Pipeliner) error {
			for _, event := range events {
				if err := p.ZAdd(ctx, EventKey(event), redis.Z{
					Score:  score,
					Member: op,
				}).Err(); err != nil {
					return err
				}
			}
			return nil
		})
		return err
	}
}

func (l *RedisEventLookup) GetDocumentation() string {
	return "Events lookup"
}

func (l *RedisEventLookup) GetMetaData() *overlay.MetaData {
	return &overlay.MetaData{
		Name: "Events",
	}
}
