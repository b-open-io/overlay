package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/redis/go-redis/v9"
)

type RedisEventDataStorage struct {
	BaseEventDataStorage
	DB *redis.Client
}

// GetBeefStorage is inherited from BaseEventDataStorage

// GetRedisClient returns the underlying Redis client for direct access
func (s *RedisEventDataStorage) GetRedisClient() *redis.Client {
	return s.DB
}


func NewRedisEventDataStorage(connString string, beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubsub pubsub.PubSub) (r *RedisEventDataStorage, err error) {
	r = &RedisEventDataStorage{
		BaseEventDataStorage: NewBaseEventDataStorage(beefStore, queueStorage, pubsub),
	}
	log.Println("Connecting to Redis Storage...", connString)
	if opts, err := redis.ParseURL(connString); err != nil {
		return nil, err
	} else {
		r.DB = redis.NewClient(opts)
		return r, nil
	}
}

func (s *RedisEventDataStorage) InsertOutput(ctx context.Context, utxo *engine.Output) (err error) {
	if err := s.beefStore.SaveBeef(ctx, &utxo.Outpoint.Txid, utxo.Beef); err != nil {
		return err
	}
	_, err = s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		op := utxo.Outpoint.String()

		// Calculate score
		var score float64
		if utxo.BlockHeight > 0 {
			score = float64(utxo.BlockHeight) + float64(utxo.BlockIdx)/1e9
		} else {
			score = float64(time.Now().Unix())
		}

		// Store output topic data
		if err := p.HMSet(ctx, OutputTopicKey(&utxo.Outpoint, utxo.Topic), outputToTopicMap(utxo)).Err(); err != nil {
			return err
		}

		// Store output data with score
		outputMap := outputToMap(utxo)
		outputMap["score"] = score
		if err := p.HMSet(ctx, outputKey(&utxo.Outpoint), outputMap).Err(); err != nil {
			return err
		}

		// Add to topic membership
		if err = p.ZAdd(ctx, OutMembershipKey(utxo.Topic), redis.Z{
			Score:  score,
			Member: op,
		}).Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Add topic as an event using SaveEvents (handles pubsub publishing) 
	return s.SaveEvents(ctx, &utxo.Outpoint, []string{utxo.Topic}, utxo.Topic, utxo.BlockHeight, utxo.BlockIdx, nil)
}

func (s *RedisEventDataStorage) FindOutput(ctx context.Context, outpoint *transaction.Outpoint, topic *string, spent *bool, includeBEEF bool) (o *engine.Output, err error) {
	o = &engine.Output{
		Outpoint: *outpoint,
	}
	if _, err := s.DB.HGet(ctx, SpendsKey, outpoint.String()).Result(); err == redis.Nil {
		// Output is not spent
		o.Spent = false
	} else if err != nil {
		return nil, err
	} else {
		// Output is spent (we have the spending txid but don't need it here)
		o.Spent = true
	}

	if spent != nil && *spent != o.Spent {
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
		if o.Beef, err = s.beefStore.LoadBeef(ctx, &outpoint.Txid); err != nil {
			return nil, err
		}
	}
	return
}

func (s *RedisEventDataStorage) FindOutputs(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
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

func (s *RedisEventDataStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
	iter := s.DB.Scan(ctx, 0, "ot:"+txid.String()+"*", 0).Iterator()
	var outputs []*engine.Output
	for iter.Next(ctx) {
		parts := strings.Split(iter.Val(), ":")
		if outpoint, err := transaction.OutpointFromString(parts[1]); err != nil {
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

func (s *RedisEventDataStorage) FindUTXOsForTopic(ctx context.Context, topic string, since float64, limit uint32, includeBEEF bool) ([]*engine.Output, error) {
	rangeBy := &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", since),
		Max: "+inf",
	}
	if limit > 0 {
		rangeBy.Count = int64(limit)
	}
	if outpoints, err := s.DB.ZRangeByScore(ctx, OutMembershipKey(topic), rangeBy).Result(); err != nil {
		return nil, err
	} else {
		outputs := make([]*engine.Output, 0, len(outpoints))
		for _, outpointStr := range outpoints {
			if outpoint, err := transaction.OutpointFromString(outpointStr); err != nil {
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

func (s *RedisEventDataStorage) DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint, topic string) error {
	outpointStr := outpoint.String()

	// First, gather data we need for cleanup
	// Check if this output was spent (to clean up reverse index)
	spendingTxid, _ := s.DB.HGet(ctx, SpendsKey, outpointStr).Result()

	// Get events from the output hash (new location)
	eventsJSON, _ := s.DB.HGet(ctx, outputKey(outpoint), "e").Result()
	var events []string
	if eventsJSON != "" {
		json.Unmarshal([]byte(eventsJSON), &events)
	}

	// Check if this output is used by other topics
	otherTopics := false
	iter := s.DB.Scan(ctx, 0, "ot:"+outpointStr+":*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		// Check if this is a different topic
		if key != OutputTopicKey(outpoint, topic) {
			otherTopics = true
			break
		}
	}

	_, err := s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		// 1. Delete output-topic data
		if err := p.Del(ctx, OutputTopicKey(outpoint, topic)).Err(); err != nil {
			return err
		}

		// 2. Remove from topic membership
		if err := p.ZRem(ctx, OutMembershipKey(topic), outpointStr).Err(); err != nil {
			return err
		}

		// If no other topics use this output, clean up everything
		if !otherTopics {
			// 3. Delete the main output hash (contains score and events now)
			p.Del(ctx, outputKey(outpoint))

			// 4. Delete optional data
			p.Del(ctx, "data:"+outpointStr)

			// 5. Clean up spend tracking if this output was spent
			if spendingTxid != "" {
				// Remove from forward index
				p.HDel(ctx, SpendsKey, outpointStr)
				// Remove from reverse index
				reverseKey := fmt.Sprintf("spends:%s", spendingTxid)
				p.SRem(ctx, reverseKey, outpointStr)
			}

			// 6. Clean up event-based indexes
			for _, event := range events {
				eventSetKey := fmt.Sprintf("evt:%s:%s", topic, event)
				p.ZRem(ctx, eventSetKey, outpointStr)
			}
		}

		return nil
	})
	return err
}

func (s *RedisEventDataStorage) MarkUTXOAsSpent(ctx context.Context, outpoint *transaction.Outpoint, topic string, beef []byte) error {
	if _, _, spendTxid, err := transaction.ParseBeef(beef); err != nil {
		return err
	} else {
		_, err = s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
			// Forward index: outpoint -> spending txid
			if err := p.HSet(ctx, SpendsKey, outpoint.String(), spendTxid.String()).Err(); err != nil {
				return err
			}
			// Reverse index: add outpoint to set of inputs for this txid
			reverseKey := fmt.Sprintf("spends:%s", spendTxid.String())
			return p.SAdd(ctx, reverseKey, outpoint.String()).Err()
		})
		return err
	}
}

func (s *RedisEventDataStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spendTxid *chainhash.Hash) error {
	_, err := s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		// Prepare values for forward index
		values := make(map[string]interface{}, len(outpoints))
		outpointStrs := make([]interface{}, 0, len(outpoints))

		for _, outpoint := range outpoints {
			opStr := outpoint.String()
			values[opStr] = spendTxid.String()
			outpointStrs = append(outpointStrs, opStr)
		}

		// Forward index: outpoints -> spending txid
		if err := p.HSet(ctx, SpendsKey, values).Err(); err != nil {
			return err
		}

		// Reverse index: add all outpoints to set of inputs for this txid
		reverseKey := fmt.Sprintf("spends:%s", spendTxid.String())
		return p.SAdd(ctx, reverseKey, outpointStrs...).Err()
	})
	return err
}

func (s *RedisEventDataStorage) UpdateConsumedBy(ctx context.Context, outpoint *transaction.Outpoint, topic string, consumedBy []*transaction.Outpoint) error {
	return s.DB.HSet(ctx, OutputTopicKey(outpoint, topic), "cb", outpointsToBytes(consumedBy)).Err()
}

func (s *RedisEventDataStorage) UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error {
	return s.beefStore.SaveBeef(ctx, txid, beef)
}

func (s *RedisEventDataStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *transaction.Outpoint, topic string, blockHeight uint32, blockIndex uint64, ancelliaryBeef []byte) error {
	score := float64(blockHeight) + float64(blockIndex)/1e9
	outpointStr := outpoint.String()

	// First, get the events associated with this output
	events, err := s.FindEvents(ctx, outpoint)
	if err != nil {
		return err
	}

	_, err = s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		// Update topic membership score
		if err := p.ZAdd(ctx, OutMembershipKey(topic), redis.Z{
			Score:  score,
			Member: outpointStr,
		}).Err(); err != nil {
			return err
		}

		// Update output block height, index, and score in main hash
		if err := p.HSet(ctx, outputKey(outpoint),
			"h", blockHeight,
			"i", blockIndex,
			"score", score,
		).Err(); err != nil {
			return err
		}

		// Update output topic data
		if err := p.HSet(ctx, OutputTopicKey(outpoint, topic), "h", blockHeight, "i", blockIndex).Err(); err != nil {
			return err
		}

		// Update scores in event sorted sets
		for _, event := range events {
			eventSetKey := fmt.Sprintf("evt:%s:%s", topic, event)
			if err := p.ZAdd(ctx, eventSetKey, redis.Z{
				Score:  score,
				Member: outpointStr,
			}).Err(); err != nil {
				return err
			}
		}

		return nil
	})
	return err
}

func (s *RedisEventDataStorage) InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error {
	score := float64(time.Now().UnixNano())
	
	// Add to transaction membership set
	if err := s.DB.ZAdd(ctx, TxMembershipKey(tx.Topic), redis.Z{
		Member: tx.Txid.String(),
		Score:  score,
	}).Err(); err != nil {
		return err
	}
	
	// Publish transaction to topic via PubSub (if available)
	if s.pubsub != nil {
		// For topic events (tm_*), publish the txid with the score
		if err := s.pubsub.Publish(ctx, tx.Topic, tx.Txid.String(), score); err != nil {
			// Log error but don't fail the transaction insertion
			log.Printf("Failed to publish transaction to topic %s: %v", tx.Topic, err)
		}
	}
	
	return nil
}

func (s *RedisEventDataStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	if _, err := s.DB.ZScore(ctx, TxMembershipKey(tx.Topic), tx.Txid.String()).Result(); err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (s *RedisEventDataStorage) UpdateLastInteraction(ctx context.Context, host string, topic string, since float64) error {
	key := fmt.Sprintf("interaction:%s", host)
	return s.DB.HSet(ctx, key, topic, since).Err()
}

func (s *RedisEventDataStorage) GetLastInteraction(ctx context.Context, host string, topic string) (float64, error) {
	key := fmt.Sprintf("interaction:%s", host)
	result, err := s.DB.HGet(ctx, key, topic).Result()
	if err == redis.Nil {
		return 0, nil // No record exists, return 0 as specified
	}
	if err != nil {
		return 0, err
	}

	score, err := strconv.ParseFloat(result, 64)
	if err != nil {
		return 0, err
	}
	return score, nil
}

func (s *RedisEventDataStorage) Close() error {
	return s.DB.Close()
}

// GetTransactionsByTopicAndHeight returns all transactions for a topic at a specific block height
func (s *RedisEventDataStorage) GetTransactionsByTopicAndHeight(ctx context.Context, topic string, height uint32) ([]*TransactionData, error) {
	// Use score range to efficiently get only outputs at the specified block height
	// Score format: blockHeight + blockIdx/1e9
	// So for height H, we want scores in range [H, H+1)
	minScore := float64(height)
	maxScore := float64(height + 1)

	// Get outpoints at the specific block height using score range
	outpointStrs, err := s.DB.ZRangeByScore(ctx, OutMembershipKey(topic), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", minScore),
		Max: fmt.Sprintf("(%f", maxScore), // Exclusive upper bound
	}).Result()
	if err != nil {
		return nil, err
	}

	// Group outputs by transaction ID
	txOutputMap := make(map[chainhash.Hash][]*OutputData)

	// Process each output
	for _, outpointStr := range outpointStrs {
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			continue
		}

		// Get the general output data
		outputData, err := s.DB.HGetAll(ctx, outputKey(outpoint)).Result()
		if err != nil {
			continue
		}

		// Parse satoshis
		var satoshis uint64
		if satoshisStr, ok := outputData["st"]; ok {
			satoshis, _ = strconv.ParseUint(satoshisStr, 10, 64)
		}

		// Parse script (stored as raw bytes, not base64)
		var script []byte
		if scriptStr, ok := outputData["sc"]; ok {
			script = []byte(scriptStr)
		}

		// Get data
		var data interface{}
		dataKey := "data:" + outpointStr
		if dataJSON, err := s.DB.Get(ctx, dataKey).Result(); err == nil && dataJSON != "" {
			json.Unmarshal([]byte(dataJSON), &data)
		}

		output := &OutputData{
			Vout:     outpoint.Index,
			Data:     data,
			Script:   script,
			Satoshis: satoshis,
		}

		txid := outpoint.Txid
		txOutputMap[txid] = append(txOutputMap[txid], output)
	}

	// Build TransactionData for each transaction
	var transactions []*TransactionData

	for txid, outputs := range txOutputMap {
		txData := &TransactionData{
			TxID:    txid,
			Outputs: outputs,
			Inputs:  make([]*OutputData, 0),
		}

		// Use reverse index to find inputs for this transaction
		reverseKey := fmt.Sprintf("spends:%s", txid.String())
		inputOutpointStrs, err := s.DB.SMembers(ctx, reverseKey).Result()
		if err == nil && len(inputOutpointStrs) > 0 {
			// Fetch data for each input
			for _, outpointStr := range inputOutpointStrs {
				inputOutpoint, err := transaction.OutpointFromString(outpointStr)
				if err != nil {
					continue
				}

				// Get the input's output data
				inputData, err := s.DB.HGetAll(ctx, outputKey(inputOutpoint)).Result()
				if err != nil || len(inputData) == 0 {
					// Skip inputs that don't exist in our system
					continue
				}

				// Parse satoshis
				var satoshis uint64
				if satoshisStr, ok := inputData["st"]; ok {
					satoshis, _ = strconv.ParseUint(satoshisStr, 10, 64)
				}

				// Parse script (stored as raw bytes, not base64)
				var script []byte
				if scriptStr, ok := inputData["sc"]; ok {
					script = []byte(scriptStr)
				}

				// Get data
				var data interface{}
				dataKey := "data:" + outpointStr
				if dataJSON, err := s.DB.Get(ctx, dataKey).Result(); err == nil && dataJSON != "" {
					json.Unmarshal([]byte(dataJSON), &data)
				}

				// Create OutputData for input with source txid
				sourceTxid := inputOutpoint.Txid
				input := &OutputData{
					TxID:     &sourceTxid,
					Vout:     inputOutpoint.Index,
					Data:     data,
					Script:   script,
					Satoshis: satoshis,
				}

				txData.Inputs = append(txData.Inputs, input)
			}
		}

		transactions = append(transactions, txData)
	}

	return transactions, nil
}

// SaveEvents associates multiple events with a single output, storing arbitrary data
func (s *RedisEventDataStorage) SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, topic string, height uint32, idx uint64, data interface{}) error {
	if len(events) == 0 {
		return nil
	}

	var score float64
	if height > 0 {
		score = float64(height) + float64(idx)/1e9
	} else {
		score = float64(time.Now().Unix())
	}

	outpointStr := outpoint.String()

	_, err := s.DB.Pipelined(ctx, func(p redis.Pipeliner) error {
		// Store events and score in the output hash
		eventsJSON, _ := json.Marshal(events)
		if err := p.HSet(ctx, outputKey(outpoint),
			"e", string(eventsJSON),
			"score", score,
		).Err(); err != nil {
			return err
		}

		// Store data as JSON if provided (keep separate due to size)
		if data != nil {
			dataKey := "data:" + outpointStr
			dataJSON, err := json.Marshal(data)
			if err != nil {
				return err
			}
			if err := p.Set(ctx, dataKey, dataJSON, 0).Err(); err != nil {
				return err
			}
		}

		// Add to event-based sorted sets for each event
		for _, event := range events {
			eventSetKey := fmt.Sprintf("evt:%s:%s", topic, event)
			if err := p.ZAdd(ctx, eventSetKey, redis.Z{
				Score:  score,
				Member: outpointStr,
			}).Err(); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Publish events if publisher is available
	if s.pubsub != nil {
		for _, event := range events {
			// Publish event with outpoint string as the message
			if err := s.pubsub.Publish(ctx, event, outpointStr); err != nil {
				// Log error but don't fail the operation
				// Publishing is best-effort
				continue
			}
		}
	}

	return nil
}

// FindEvents returns all events associated with a given outpoint
func (s *RedisEventDataStorage) FindEvents(ctx context.Context, outpoint *transaction.Outpoint) ([]string, error) {
	eventsJSON, err := s.DB.HGet(ctx, outputKey(outpoint), "e").Result()

	if err == redis.Nil {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}

	var events []string
	if err := json.Unmarshal([]byte(eventsJSON), &events); err != nil {
		return nil, err
	}

	return events, nil
}

// LookupOutpoints returns outpoints matching the given query criteria
func (s *RedisEventDataStorage) LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error) {
	withData := len(includeData) > 0 && includeData[0]

	// Determine score range
	var minScore, maxScore string
	if question.Reverse {
		maxScore = fmt.Sprintf("%f", question.From)
		if question.Until > 0 {
			minScore = fmt.Sprintf("%f", question.Until)
		} else {
			minScore = "-inf"
		}
	} else {
		minScore = fmt.Sprintf("%f", question.From)
		if question.Until > 0 {
			maxScore = fmt.Sprintf("%f", question.Until)
		} else {
			maxScore = "+inf"
		}
	}

	var outpointStrs []string
	var err error

	// Handle different event query types
	if question.Event != "" {
		// Single event query
		eventSetKey := fmt.Sprintf("evt:%s:%s", question.Topic, question.Event)
		if question.Reverse {
			outpointStrs, err = s.DB.ZRevRangeByScore(ctx, eventSetKey, &redis.ZRangeBy{
				Min: minScore,
				Max: maxScore,
			}).Result()
		} else {
			outpointStrs, err = s.DB.ZRangeByScore(ctx, eventSetKey, &redis.ZRangeBy{
				Min: minScore,
				Max: maxScore,
			}).Result()
		}
		if err != nil {
			return nil, err
		}
	} else if len(question.Events) > 0 {
		// Multiple events query - need to handle join types
		if question.JoinType == nil || *question.JoinType == JoinTypeUnion {
			// Union: combine results from multiple event sets
			outpointSet := make(map[string]bool)
			for _, event := range question.Events {
				eventSetKey := fmt.Sprintf("evt:%s:%s", question.Topic, event)
				var eventOutpoints []string
				if question.Reverse {
					eventOutpoints, err = s.DB.ZRevRangeByScore(ctx, eventSetKey, &redis.ZRangeBy{
						Min: minScore,
						Max: maxScore,
					}).Result()
				} else {
					eventOutpoints, err = s.DB.ZRangeByScore(ctx, eventSetKey, &redis.ZRangeBy{
						Min: minScore,
						Max: maxScore,
					}).Result()
				}
				if err != nil {
					return nil, err
				}
				for _, op := range eventOutpoints {
					outpointSet[op] = true
				}
			}
			// Convert set to slice
			for op := range outpointSet {
				outpointStrs = append(outpointStrs, op)
			}
		} else {
			// For intersection and difference, we'd need more complex Redis operations
			// For now, return empty results for these complex queries
			outpointStrs = []string{}
		}
	} else {
		// No event filter - this is complex in Redis without scanning all keys
		return []*OutpointResult{}, nil
	}

	// Apply limit
	if question.Limit > 0 && len(outpointStrs) > question.Limit {
		outpointStrs = outpointStrs[:question.Limit]
	}

	// Build results
	var results []*OutpointResult
	for _, outpointStr := range outpointStrs {
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			continue // Skip invalid outpoints
		}

		// Get score from output hash
		scoreStr, err := s.DB.HGet(ctx, outputKey(outpoint), "score").Result()
		if err != nil {
			continue // Skip if no score found
		}
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			continue // Skip if invalid score
		}

		// Check unspent filter if needed
		if question.UnspentOnly {
			if spent, err := s.DB.HExists(ctx, SpendsKey, outpointStr).Result(); err != nil || spent {
				continue // Skip spent outputs
			}
		}

		result := &OutpointResult{
			Outpoint: outpoint,
			Score:    score,
		}

		// Include data if requested
		if withData {
			dataKey := "data:" + outpointStr
			dataJSON, err := s.DB.Get(ctx, dataKey).Result()
			if err == nil && dataJSON != "" {
				var data interface{}
				if err := json.Unmarshal([]byte(dataJSON), &data); err == nil {
					result.Data = data
				}
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// GetOutputData retrieves the data associated with a specific output
func (s *RedisEventDataStorage) GetOutputData(ctx context.Context, outpoint *transaction.Outpoint) (interface{}, error) {
	dataKey := "data:" + outpoint.String()
	dataJSON, err := s.DB.Get(ctx, dataKey).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("outpoint not found")
	}
	if err != nil {
		return nil, err
	}

	if dataJSON == "" {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
		return nil, err
	}

	return data, nil
}

// LoadBeefByTxidAndTopic loads merged BEEF for a transaction within a topic context
func (s *RedisEventDataStorage) LoadBeefByTxidAndTopic(ctx context.Context, txid *chainhash.Hash, topic string) ([]byte, error) {
	// Scan for any output key matching ot:{topic}:{txid}_*
	pattern := "ot:" + topic + ":" + txid.String() + "_*"
	iter := s.DB.Scan(ctx, 0, pattern, 1).Iterator() // Only need one match
	
	var targetKey string
	if iter.Next(ctx) {
		targetKey = iter.Val()
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan for outputs: %w", err)
	}
	
	if targetKey == "" {
		return nil, fmt.Errorf("transaction %s not found in topic %s", txid.String(), topic)
	}
	
	// Get BEEF from beef storage
	beefBytes, err := s.beefStore.LoadBeef(ctx, txid)
	if err != nil {
		return nil, fmt.Errorf("failed to load BEEF: %w", err)
	}
	
	// Parse the main BEEF
	beef, _, _, err := transaction.ParseBeef(beefBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse main BEEF: %w", err)
	}
	
	// Get AncillaryBeef from the found output key (optional)
	ancillaryBeef, err := s.DB.HGet(ctx, targetKey, "ab").Bytes()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get AncillaryBeef: %w", err)
	}
	
	// Merge AncillaryBeef if present (field is optional)
	if err == nil && len(ancillaryBeef) > 0 {
		if err := beef.MergeBeefBytes(ancillaryBeef); err != nil {
			return nil, fmt.Errorf("failed to merge AncillaryBeef: %w", err)
		}
	}
	
	// Get atomic BEEF bytes for the specific transaction
	completeBeef, err := beef.AtomicBytes(txid)
	if err != nil {
		return nil, fmt.Errorf("failed to generate atomic BEEF: %w", err)
	}
	
	return completeBeef, nil
}

// FindOutputData returns outputs matching the given query criteria as OutputData objects
func (s *RedisEventDataStorage) FindOutputData(ctx context.Context, question *EventQuestion) ([]*OutputData, error) {
	// Determine score range
	var minScore, maxScore string
	if question.Reverse {
		maxScore = fmt.Sprintf("%f", question.From)
		if question.Until > 0 {
			minScore = fmt.Sprintf("%f", question.Until)
		} else {
			minScore = "-inf"
		}
	} else {
		minScore = fmt.Sprintf("%f", question.From)
		if question.Until > 0 {
			maxScore = fmt.Sprintf("%f", question.Until)
		} else {
			maxScore = "+inf"
		}
	}

	var outpointStrs []string
	var err error

	// Handle different event query types
	if question.Event != "" {
		// Single event query
		eventSetKey := fmt.Sprintf("evt:%s:%s", question.Topic, question.Event)
		if question.Reverse {
			outpointStrs, err = s.DB.ZRevRangeByScore(ctx, eventSetKey, &redis.ZRangeBy{
				Min: minScore,
				Max: maxScore,
			}).Result()
		} else {
			outpointStrs, err = s.DB.ZRangeByScore(ctx, eventSetKey, &redis.ZRangeBy{
				Min: minScore,
				Max: maxScore,
			}).Result()
		}
		if err != nil {
			return nil, err
		}
	} else if len(question.Events) > 0 {
		// Multiple events query - need to handle join types
		if question.JoinType == nil || *question.JoinType == JoinTypeUnion {
			// Union: combine results from multiple event sets
			outpointSet := make(map[string]bool)
			for _, event := range question.Events {
				eventSetKey := fmt.Sprintf("evt:%s:%s", question.Topic, event)
				var eventOutpoints []string
				if question.Reverse {
					eventOutpoints, err = s.DB.ZRevRangeByScore(ctx, eventSetKey, &redis.ZRangeBy{
						Min: minScore,
						Max: maxScore,
					}).Result()
				} else {
					eventOutpoints, err = s.DB.ZRangeByScore(ctx, eventSetKey, &redis.ZRangeBy{
						Min: minScore,
						Max: maxScore,
					}).Result()
				}
				if err != nil {
					return nil, err
				}
				for _, op := range eventOutpoints {
					outpointSet[op] = true
				}
			}
			// Convert set to slice
			for op := range outpointSet {
				outpointStrs = append(outpointStrs, op)
			}
		} else {
			// For intersection and difference, we'd need more complex Redis operations
			// For now, return empty results for these complex queries
			outpointStrs = []string{}
		}
	} else {
		// No event filter - this is complex in Redis without scanning all keys
		return []*OutputData{}, nil
	}

	// Apply limit
	if question.Limit > 0 && len(outpointStrs) > question.Limit {
		outpointStrs = outpointStrs[:question.Limit]
	}

	// Build OutputData results
	var results []*OutputData
	for _, outpointStr := range outpointStrs {
		outpoint, err := transaction.OutpointFromString(outpointStr)
		if err != nil {
			continue // Skip invalid outpoints
		}

		// Check unspent filter if needed
		if question.UnspentOnly {
			if spent, err := s.DB.HExists(ctx, SpendsKey, outpointStr).Result(); err != nil || spent {
				continue // Skip spent outputs
			}
		}

		// Get the general output data
		outputData, err := s.DB.HGetAll(ctx, outputKey(outpoint)).Result()
		if err != nil || len(outputData) == 0 {
			continue // Skip if no output data found
		}

		// Parse satoshis
		var satoshis uint64
		if satoshisStr, ok := outputData["st"]; ok {
			satoshis, _ = strconv.ParseUint(satoshisStr, 10, 64)
		}

		// Parse script (stored as raw bytes, not base64)
		var script []byte
		if scriptStr, ok := outputData["sc"]; ok {
			script = []byte(scriptStr)
		}

		// Get data
		var data interface{}
		dataKey := "data:" + outpointStr
		if dataJSON, err := s.DB.Get(ctx, dataKey).Result(); err == nil && dataJSON != "" {
			json.Unmarshal([]byte(dataJSON), &data)
		}

		// Get the transaction ID for this output
		txid := outpoint.Txid

		// Check if the output is spent to populate spend field
		var spendTxid *chainhash.Hash
		if spendTxidStr, err := s.DB.HGet(ctx, SpendsKey, outpointStr).Result(); err == nil && spendTxidStr != "" {
			if parsedSpendTxid, err := chainhash.NewHashFromHex(spendTxidStr); err == nil {
				spendTxid = parsedSpendTxid
			}
		}

		// Get score from output data
		var score float64
		if scoreStr, ok := outputData["score"]; ok {
			score, _ = strconv.ParseFloat(scoreStr, 64)
		}

		result := &OutputData{
			TxID:     &txid,
			Vout:     outpoint.Index,
			Data:     data,
			Script:   script,
			Satoshis: satoshis,
			Spend:    spendTxid,
			Score:    score,
		}

		results = append(results, result)
	}

	return results, nil
}

// LookupEventScores returns lightweight event scores for simple queries
func (s *RedisEventDataStorage) LookupEventScores(ctx context.Context, topic string, event string, fromScore float64) ([]queue.ScoredMember, error) {
	eventSetKey := fmt.Sprintf("evt:%s:%s", topic, event)
	
	// Query the sorted set directly without parsing outpoints
	results, err := s.DB.ZRangeByScoreWithScores(ctx, eventSetKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", fromScore),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}

	var members []queue.ScoredMember
	for _, result := range results {
		members = append(members, queue.ScoredMember{
			Member: result.Member.(string),
			Score:  result.Score,
		})
	}
	return members, nil
}

// CountOutputs returns the total count of outputs in a given topic
func (s *RedisEventDataStorage) CountOutputs(ctx context.Context, topic string) (int64, error) {
	// In Redis, outputs are stored in sorted sets with topic-based keys
	// We need to count all outputs across all events in this topic
	// Use pattern to find all event sets for this topic and sum their cardinalities
	
	pattern := fmt.Sprintf("events:%s:*", topic)
	keys, err := s.DB.Keys(ctx, pattern).Result()
	if err != nil {
		return 0, err
	}
	
	var total int64
	for _, key := range keys {
		count, err := s.DB.ZCard(ctx, key).Result()
		if err != nil {
			return 0, err
		}
		total += count
	}
	
	return total, nil
}

