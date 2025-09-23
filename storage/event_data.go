package storage

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/b-open-io/overlay/beef"
	"github.com/b-open-io/overlay/pubsub"
	"github.com/b-open-io/overlay/queue"
	"github.com/bsv-blockchain/go-overlay-services/pkg/core/engine"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// OutputData represents an input or output with its data
type OutputData struct {
	TxID     *chainhash.Hash `json:"txid,omitempty"` // Transaction ID (for inputs: source txid, for outputs: current txid)
	Vout     uint32          `json:"vout"`
	Data     interface{}     `json:"data,omitempty"`
	Script   []byte          `json:"script"`
	Satoshis uint64          `json:"satoshis"`
	Spend    *chainhash.Hash `json:"spend,omitempty"` // Spending transaction ID (only populated if spent)
	Score    float64         `json:"score,omitempty"` // Sort score for ordering/pagination
}

// TransactionData represents a transaction with its inputs and outputs
type TransactionData struct {
	TxID        chainhash.Hash `json:"txid"`
	Inputs      []*OutputData  `json:"inputs"`
	Outputs     []*OutputData  `json:"outputs"`
	Beef        []byte         `json:"beef,omitempty"` // Optional BEEF data
	BlockHeight uint32         `json:"block_height,omitempty"`
	BlockIndex  uint32         `json:"block_index,omitempty"`
}

// TopicDataStorage provides single-topic data operations
// This is implemented by MongoDB and SQLite providers with a specific topicId
type TopicDataStorage interface {
	// Core engine.Storage methods (topic-scoped)
	InsertOutput(ctx context.Context, utxo *engine.Output) error
	FindOutput(ctx context.Context, outpoint *transaction.Outpoint, spent *bool, includeBEEF bool) (*engine.Output, error)
	FindOutputs(ctx context.Context, outpoints []*transaction.Outpoint, spent *bool, includeBEEF bool) ([]*engine.Output, error)
	FindUTXOsForTopic(ctx context.Context, since float64, limit uint32, includeBEEF bool) ([]*engine.Output, error)
	DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint) error
	MarkUTXOAsSpent(ctx context.Context, outpoint *transaction.Outpoint, beef []byte) error
	MarkUTXOsAsSpent(ctx context.Context, outpoints []*transaction.Outpoint, spendTxid *chainhash.Hash) error
	UpdateConsumedBy(ctx context.Context, outpoint *transaction.Outpoint, consumedBy []*transaction.Outpoint) error
	UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error
	UpdateOutputBlockHeight(ctx context.Context, outpoint *transaction.Outpoint, blockHeight uint32, blockIndex uint64, ancelliaryBeef []byte) error

	// Applied transaction tracking (topic-scoped)
	InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error
	DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error)

	// Transaction queries within this topic
	FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error)
	GetTransactionsByHeight(ctx context.Context, height uint32) ([]*TransactionData, error)
	GetTransactionByTxid(ctx context.Context, txid *chainhash.Hash, includeBeef ...bool) (*TransactionData, error)
	LoadBeefByTxid(ctx context.Context, txid *chainhash.Hash) ([]byte, error)

	// Event management (topic-scoped)
	SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, score float64, data interface{}) error
	FindEvents(ctx context.Context, outpoint *transaction.Outpoint) ([]string, error)

	// Event queries (topic-scoped)
	LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error)
	LookupEventScores(ctx context.Context, event string, fromScore float64) ([]queue.ScoredMember, error)
	GetOutputData(ctx context.Context, outpoint *transaction.Outpoint) (interface{}, error)
	FindOutputData(ctx context.Context, question *EventQuestion) ([]*OutputData, error)
	CountOutputs(ctx context.Context) (int64, error)

	// Reconciliation and validation
	ReconcileValidatedMerkleRoots(ctx context.Context) error
	ReconcileMerkleRoot(ctx context.Context, blockHeight uint32, merkleRoot *chainhash.Hash) error
	FindOutpointsByMerkleState(ctx context.Context, state engine.MerkleState, limit uint32) ([]*transaction.Outpoint, error)

	// Topic information
	GetTopic() string

	// Resource management
	Close() error
}

// TopicDataStorageFactory creates TopicDataStorage instances for specific topics
type TopicDataStorageFactory func(topic string) (TopicDataStorage, error)

// EventDataStorage handles multi-tenant storage by coordinating TopicDataStorage instances
// It implements engine.Storage and provides topic management
type EventDataStorage struct {
	// Topic-specific storage providers (concurrent-safe)
	topicStorages sync.Map // map[string]TopicDataStorage

	// Factory function for creating new topic storages
	topicFactory TopicDataStorageFactory

	// Shared services
	beefStore    beef.BeefStorage
	pubsub       pubsub.PubSub
	queueStorage queue.QueueStorage
}

// EventQuestion defines query parameters for event-based lookups
type EventQuestion struct {
	Event       string    `json:"event"`
	Events      []string  `json:"events"`
	Topic       string    `json:"topic"` // Required topic scoping
	JoinType    *JoinType `json:"join"`
	From        float64   `json:"from"`
	Until       float64   `json:"until"`
	Limit       int       `json:"limit"`
	UnspentOnly bool      `json:"unspentOnly"`
	Reverse     bool      `json:"rev"`
}

// JoinType defines how multiple events are combined in queries
type JoinType int

const (
	// JoinTypeIntersect returns outputs that have ALL specified events
	JoinTypeIntersect JoinType = iota
	// JoinTypeUnion returns outputs that have ANY of the specified events
	JoinTypeUnion
	// JoinTypeDifference returns outputs from first event minus those in subsequent events
	JoinTypeDifference
)

// OutpointResult contains the result of an outpoint lookup
type OutpointResult struct {
	Outpoint *transaction.Outpoint `json:"outpoint"`
	Score    float64               `json:"score"`
	Data     interface{}           `json:"data,omitempty"`
}

// NewEventDataStorage creates a new EventDataStorage instance
func NewEventDataStorage(factory TopicDataStorageFactory, beefStore beef.BeefStorage, queueStorage queue.QueueStorage, pubsub pubsub.PubSub) *EventDataStorage {
	return &EventDataStorage{
		topicFactory: factory,
		beefStore:    beefStore,
		pubsub:       pubsub,
		queueStorage: queueStorage,
	}
}

// getTopicStorage returns the storage for a topic, creating it if necessary
func (s *EventDataStorage) getTopicStorage(topic string) (TopicDataStorage, error) {
	// Try to load existing storage
	if value, ok := s.topicStorages.Load(topic); ok {
		return value.(TopicDataStorage), nil
	}

	// Create new topic storage
	storage, err := s.topicFactory(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to create topic storage for %s: %w", topic, err)
	}

	// Store it atomically (LoadOrStore handles race conditions)
	actual, _ := s.topicStorages.LoadOrStore(topic, storage)
	return actual.(TopicDataStorage), nil
}

// ReconcileMerkleRoot reconciles outputs at a specific block height for a specific topic
func (s *EventDataStorage) ReconcileMerkleRoot(ctx context.Context, topic string, blockHeight uint32, merkleRoot *chainhash.Hash) error {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return err
	}
	return storage.ReconcileMerkleRoot(ctx, blockHeight, merkleRoot)
}

// ReconcileValidatedMerkleRoots reconciles all validated outputs across all topics
func (s *EventDataStorage) ReconcileValidatedMerkleRoots(ctx context.Context) error {
	topics := s.GetActiveTopics()
	for _, topic := range topics {
		storage, err := s.getTopicStorage(topic)
		if err != nil {
			return fmt.Errorf("failed to get storage for topic %s: %w", topic, err)
		}
		if err := storage.ReconcileValidatedMerkleRoots(ctx); err != nil {
			return fmt.Errorf("failed to reconcile validated outputs for topic %s: %w", topic, err)
		}
	}
	return nil
}

// FindOutpointsByMerkleState finds outpoints by their merkle validation state for a topic
func (s *EventDataStorage) FindOutpointsByMerkleState(ctx context.Context, topic string, state engine.MerkleState, limit uint32) ([]*transaction.Outpoint, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.FindOutpointsByMerkleState(ctx, state, limit)
}

// Shared service accessors
func (s *EventDataStorage) GetBeefStorage() beef.BeefStorage {
	return s.beefStore
}

func (s *EventDataStorage) GetPubSub() pubsub.PubSub {
	return s.pubsub
}

func (s *EventDataStorage) GetQueueStorage() queue.QueueStorage {
	return s.queueStorage
}

// Topic management
func (s *EventDataStorage) AddTopic(topic string) error {
	_, err := s.getTopicStorage(topic)
	return err
}

func (s *EventDataStorage) RemoveTopic(topic string) error {
	// Close the topic storage if it exists
	if value, ok := s.topicStorages.LoadAndDelete(topic); ok {
		storage := value.(TopicDataStorage)
		return storage.Close()
	}
	return nil
}

func (s *EventDataStorage) GetActiveTopics() []string {
	var topics []string
	s.topicStorages.Range(func(key, value interface{}) bool {
		topics = append(topics, key.(string))
		return true
	})
	return topics
}

// Core engine.Storage methods - route to appropriate topic storage
func (s *EventDataStorage) InsertOutput(ctx context.Context, utxo *engine.Output) error {
	storage, err := s.getTopicStorage(utxo.Topic)
	if err != nil {
		return err
	}
	return storage.InsertOutput(ctx, utxo)
}

func (s *EventDataStorage) FindOutput(ctx context.Context, outpoint *transaction.Outpoint, topic *string, spent *bool, includeBEEF bool) (*engine.Output, error) {
	if topic == nil {
		return nil, fmt.Errorf("topic is required")
	}
	storage, err := s.getTopicStorage(*topic)
	if err != nil {
		return nil, err
	}
	return storage.FindOutput(ctx, outpoint, spent, includeBEEF)
}

func (s *EventDataStorage) FindOutputs(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spent *bool, includeBEEF bool) ([]*engine.Output, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.FindOutputs(ctx, outpoints, spent, includeBEEF)
}

// Cross-topic method - query only relevant topics based on QueueStorage index
func (s *EventDataStorage) FindOutputsForTransaction(ctx context.Context, txid *chainhash.Hash, includeBEEF bool) ([]*engine.Output, error) {
	// Check which topics contain this transaction
	txKey := fmt.Sprintf("tx:%s", txid.String())
	topics, err := s.queueStorage.SMembers(ctx, txKey)
	if err != nil || len(topics) == 0 {
		return []*engine.Output{}, nil // No topics have this transaction
	}

	// Query only the relevant topics
	var allOutputs []*engine.Output
	for _, topic := range topics {
		storage, err := s.getTopicStorage(topic)
		if err != nil {
			continue // Skip unavailable topics
		}
		outputs, err := storage.FindOutputsForTransaction(ctx, txid, includeBEEF)
		if err != nil {
			return nil, fmt.Errorf("error querying topic %s: %w", topic, err)
		}
		allOutputs = append(allOutputs, outputs...)
	}

	return allOutputs, nil
}

func (s *EventDataStorage) FindUTXOsForTopic(ctx context.Context, topic string, since float64, limit uint32, includeBEEF bool) ([]*engine.Output, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.FindUTXOsForTopic(ctx, since, limit, includeBEEF)
}

func (s *EventDataStorage) DeleteOutput(ctx context.Context, outpoint *transaction.Outpoint, topic string) error {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return err
	}
	return storage.DeleteOutput(ctx, outpoint)
}

func (s *EventDataStorage) MarkUTXOAsSpent(ctx context.Context, outpoint *transaction.Outpoint, topic string, beef []byte) error {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return err
	}
	return storage.MarkUTXOAsSpent(ctx, outpoint, beef)
}

func (s *EventDataStorage) MarkUTXOsAsSpent(ctx context.Context, outpoints []*transaction.Outpoint, topic string, spendTxid *chainhash.Hash) error {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return err
	}
	return storage.MarkUTXOsAsSpent(ctx, outpoints, spendTxid)
}

func (s *EventDataStorage) UpdateConsumedBy(ctx context.Context, outpoint *transaction.Outpoint, topic string, consumedBy []*transaction.Outpoint) error {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return err
	}
	return storage.UpdateConsumedBy(ctx, outpoint, consumedBy)
}

func (s *EventDataStorage) UpdateTransactionBEEF(ctx context.Context, txid *chainhash.Hash, beef []byte) error {
	// Save BEEF to shared storage first
	if err := s.beefStore.SaveBeef(ctx, txid, beef); err != nil {
		return err
	}

	// Check which topics contain this transaction
	txKey := fmt.Sprintf("tx:%s", txid.String())
	topics, err := s.queueStorage.SMembers(ctx, txKey)
	if err != nil {
		fmt.Printf("ERROR: Failed to get topics for tx %s: %v\n", txid.String(), err)
		return nil
	}
	if len(topics) == 0 {
		fmt.Printf("INFO: No topics found for transaction %s in Redis key %s\n", txid.String(), txKey)
		return nil
	}

	fmt.Printf("INFO: Found %d topics for transaction %s: %v\n", len(topics), txid.String(), topics)

	// Update merkle validation state in each topic storage that has this transaction
	for _, topic := range topics {
		storage, err := s.getTopicStorage(topic)
		if err != nil {
			fmt.Printf("ERROR: Failed to get topic storage for %s: %v\n", topic, err)
			continue
		}
		// Update the merkle validation state for outputs in this topic
		fmt.Printf("INFO: Updating merkle validation state for topic %s, transaction %s\n", topic, txid.String())
		if err := storage.UpdateTransactionBEEF(ctx, txid, beef); err != nil {
			return fmt.Errorf("failed to update merkle state for topic %s: %w", topic, err)
		}
		fmt.Printf("INFO: Successfully updated merkle validation state for topic %s\n", topic)
	}

	return nil
}

func (s *EventDataStorage) UpdateOutputBlockHeight(ctx context.Context, outpoint *transaction.Outpoint, topic string, blockHeight uint32, blockIndex uint64, ancelliaryBeef []byte) error {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return err
	}
	return storage.UpdateOutputBlockHeight(ctx, outpoint, blockHeight, blockIndex, ancelliaryBeef)
}

// Applied transaction methods
func (s *EventDataStorage) InsertAppliedTransaction(ctx context.Context, tx *overlay.AppliedTransaction) error {
	// Insert into topic-specific storage first
	storage, err := s.getTopicStorage(tx.Topic)
	if err != nil {
		return err
	}
	if err := storage.InsertAppliedTransaction(ctx, tx); err != nil {
		return err
	}

	// Track transaction-topic association in QueueStorage for FindOutputsForTransaction
	txKey := fmt.Sprintf("tx:%s", tx.Txid.String())
	return s.queueStorage.SAdd(ctx, txKey, tx.Topic)
}

func (s *EventDataStorage) DoesAppliedTransactionExist(ctx context.Context, tx *overlay.AppliedTransaction) (bool, error) {
	storage, err := s.getTopicStorage(tx.Topic)
	if err != nil {
		return false, err
	}
	return storage.DoesAppliedTransactionExist(ctx, tx)
}

// Host interaction methods - managed in shared QueueStorage
func (s *EventDataStorage) UpdateLastInteraction(ctx context.Context, host string, topic string, since float64) error {
	key := fmt.Sprintf("interactions:%s", topic)
	return s.queueStorage.HSet(ctx, key, host, fmt.Sprintf("%.0f", since))
}

func (s *EventDataStorage) GetLastInteraction(ctx context.Context, host string, topic string) (float64, error) {
	key := fmt.Sprintf("interactions:%s", topic)
	scoreStr, err := s.queueStorage.HGet(ctx, key, host)
	if err != nil {
		return 0, nil // Return 0 if not found, matching existing behavior
	}
	if scoreStr == "" {
		return 0, nil
	}

	score, err := strconv.ParseFloat(scoreStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse interaction score: %w", err)
	}
	return score, nil
}

// Block data methods
func (s *EventDataStorage) GetTransactionsByTopicAndHeight(ctx context.Context, topic string, height uint32) ([]*TransactionData, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.GetTransactionsByHeight(ctx, height)
}

func (s *EventDataStorage) GetTransactionByTopic(ctx context.Context, topic string, txid *chainhash.Hash, includeBeef ...bool) (*TransactionData, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.GetTransactionByTxid(ctx, txid, includeBeef...)
}

func (s *EventDataStorage) LoadBeefByTxidAndTopic(ctx context.Context, txid *chainhash.Hash, topic string) ([]byte, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.LoadBeefByTxid(ctx, txid)
}

// Event methods
func (s *EventDataStorage) SaveEvents(ctx context.Context, outpoint *transaction.Outpoint, events []string, topic string, score float64, data interface{}) error {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return err
	}
	return storage.SaveEvents(ctx, outpoint, events, score, data)
}

func (s *EventDataStorage) FindEvents(ctx context.Context, outpoint *transaction.Outpoint, topic string) ([]string, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.FindEvents(ctx, outpoint)
}

// Event query methods
func (s *EventDataStorage) LookupOutpoints(ctx context.Context, question *EventQuestion, includeData ...bool) ([]*OutpointResult, error) {
	if question.Topic == "" {
		return nil, fmt.Errorf("topic is required in EventQuestion")
	}
	storage, err := s.getTopicStorage(question.Topic)
	if err != nil {
		return nil, err
	}
	return storage.LookupOutpoints(ctx, question, includeData...)
}

func (s *EventDataStorage) LookupEventScores(ctx context.Context, topic string, event string, fromScore float64) ([]queue.ScoredMember, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.LookupEventScores(ctx, event, fromScore)
}

func (s *EventDataStorage) GetOutputData(ctx context.Context, outpoint *transaction.Outpoint, topic string) (interface{}, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return nil, err
	}
	return storage.GetOutputData(ctx, outpoint)
}

func (s *EventDataStorage) FindOutputData(ctx context.Context, question *EventQuestion) ([]*OutputData, error) {
	if question.Topic == "" {
		return nil, fmt.Errorf("topic is required in EventQuestion")
	}
	storage, err := s.getTopicStorage(question.Topic)
	if err != nil {
		return nil, err
	}
	return storage.FindOutputData(ctx, question)
}

func (s *EventDataStorage) CountOutputs(ctx context.Context, topic string) (int64, error) {
	storage, err := s.getTopicStorage(topic)
	if err != nil {
		return 0, err
	}
	return storage.CountOutputs(ctx)
}

// Close closes all topic storages and shared services, cleaning up all resources
func (s *EventDataStorage) Close() error {
	var errs []error

	// Close all topic storages
	s.topicStorages.Range(func(key, value interface{}) bool {
		storage := value.(TopicDataStorage)
		if err := storage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close topic %s: %w", key.(string), err))
		}
		return true
	})

	// Close shared services
	if s.beefStore != nil {
		if err := s.beefStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close beef storage: %w", err))
		}
	}

	if s.pubsub != nil {
		if err := s.pubsub.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close pubsub: %w", err))
		}
	}

	if s.queueStorage != nil {
		if err := s.queueStorage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close queue storage: %w", err))
		}
	}

	// Clear all storages
	s.topicStorages = sync.Map{}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing resources: %v", errs)
	}
	return nil
}
