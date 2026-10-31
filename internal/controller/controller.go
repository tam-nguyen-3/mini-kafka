// Package controller models a single in-memory Kafka metadata authority.
// It does not implement KRaft consensus or controller failover.
package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
)

var (
	// ErrInvalidBrokerClient indicates a missing assignment client.
	ErrInvalidBrokerClient = errors.New("nil broker client")
	// ErrInvalidBrokerAddress indicates an empty or whitespace-only address.
	ErrInvalidBrokerAddress = errors.New("blank broker address")
	// ErrBrokerConflict indicates an ID already bound to another address.
	ErrBrokerConflict = errors.New("broker ID registered at another address")
	// ErrInvalidTopicName indicates an empty or whitespace-only topic name.
	ErrInvalidTopicName = errors.New("blank topic name")
	// ErrInvalidPartitionCount indicates a non-positive partition count.
	ErrInvalidPartitionCount = errors.New("partition count must be positive")
	// ErrNoBrokers indicates that placement cannot start without a broker.
	ErrNoBrokers = errors.New("no brokers registered")
	// ErrTopicAlreadyExists indicates an already published topic.
	ErrTopicAlreadyExists = errors.New("topic already exists")
	// ErrPendingTopicConflict indicates a retry with different parameters.
	ErrPendingTopicConflict = errors.New("pending topic has a different partition count")
)

// BrokerClient assigns storage without coupling the controller to HTTP.
// Implementations must honor ctx and make repeated assignments idempotent.
// Network implementations must also bound their waits with finite timeouts.
type BrokerClient interface {
	AssignPartition(ctx context.Context, target broker.BrokerInfo, tp broker.TopicPartition) error
}

type assignment struct {
	target broker.BrokerInfo
	tp     broker.TopicPartition
}

// Controller owns metadata, never broker-local logs. Construct it with New.
// Methods are safe for concurrent use; a Controller must not be copied.
// All state is lost on restart. There is no automatic reassignment when a
// broker joins, failure detection, or replication in this iteration.
type Controller struct {
	client     BrokerClient
	createGate chan struct{}
	mu         sync.RWMutex
	brokers    map[broker.BrokerID]broker.BrokerInfo
	pending    map[string][]assignment
	published  map[string][]broker.PartitionMetadata
}

// New creates a controller, rejecting a nil BrokerClient.
func New(client BrokerClient) (*Controller, error) {
	if client == nil {
		return nil, ErrInvalidBrokerClient
	}
	return &Controller{
		client:     client,
		createGate: make(chan struct{}, 1),
		brokers:    make(map[broker.BrokerID]broker.BrokerInfo),
		pending:    make(map[string][]assignment),
		published:  make(map[string][]broker.PartitionMetadata),
	}, nil
}

// RegisterBroker records an address. Repeating the same ID/address is safe;
// changing an existing ID's address returns ErrBrokerConflict. All integer IDs
// are accepted. Address syntax is left to transport; blank addresses fail.
func (c *Controller) RegisterBroker(id broker.BrokerID, addr string) error {
	if strings.TrimSpace(addr) == "" {
		return ErrInvalidBrokerAddress
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.brokers[id]; ok && existing.Addr != addr {
		return fmt.Errorf("%w: %d", ErrBrokerConflict, id)
	}
	c.brokers[id] = broker.BrokerInfo{ID: id, Addr: addr}
	return nil
}

// CreateTopic assigns partitions round-robin over a sorted broker snapshot.
// Calls serialize, but registration and metadata reads remain available during
// assignment. The caller supplies cancellation/deadlines.
//
// Metadata is published only after all assignments acknowledge. Failure leaves
// a pending plan: an explicit retry with the same count replays that plan,
// including acknowledged assignments; a different count conflicts. This is
// not a distributed transaction: unadvertised storage can remain on brokers.
func (c *Controller) CreateTopic(ctx context.Context, name string, numPartitions int) error {
	if strings.TrimSpace(name) == "" {
		return ErrInvalidTopicName
	}
	if numPartitions < 1 {
		return ErrInvalidPartitionCount
	}
	select {
	case c.createGate <- struct{}{}:
		defer func() { <-c.createGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	plan, err := c.reservePlan(name, numPartitions)
	if err != nil {
		return err
	}
	for _, a := range plan {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("assign %s to broker %d: %w", a.tp, a.target.ID, err)
		}
		if err := c.client.AssignPartition(ctx, a.target, a.tp); err != nil {
			return fmt.Errorf("assign %s to broker %d: %w", a.tp, a.target.ID, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("publish topic %q: %w", name, err)
	}
	partitions := make([]broker.PartitionMetadata, len(plan))
	for i, a := range plan {
		partitions[i] = broker.PartitionMetadata{TopicPartition: a.tp, Leader: a.target.ID}
	}
	c.mu.Lock()
	c.published[name] = partitions
	delete(c.pending, name)
	c.mu.Unlock()
	return nil
}

// reservePlan returns an immutable plan; the creation gate protects its use.
func (c *Controller) reservePlan(name string, count int) ([]assignment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.published[name]; ok {
		return nil, fmt.Errorf("%w: %q", ErrTopicAlreadyExists, name)
	}
	if plan, ok := c.pending[name]; ok {
		if len(plan) != count {
			return nil, fmt.Errorf("%w: %q", ErrPendingTopicConflict, name)
		}
		return plan, nil
	}
	if len(c.brokers) == 0 {
		return nil, ErrNoBrokers
	}
	brokers := c.sortedBrokers()
	plan := make([]assignment, count)
	for i := range plan {
		plan[i] = assignment{target: brokers[i%len(brokers)], tp: broker.TopicPartition{Topic: name, Partition: i}}
	}
	c.pending[name] = plan
	return plan, nil
}

// sortedBrokers requires the state lock and returns fresh storage.
func (c *Controller) sortedBrokers() []broker.BrokerInfo {
	brokers := make([]broker.BrokerInfo, 0, len(c.brokers))
	for _, b := range c.brokers {
		brokers = append(brokers, b)
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })
	return brokers
}

// Metadata returns copied, non-nil lists sorted by broker ID and by topic /
// partition number. Pending partitions are hidden, even if stored on brokers.
func (c *Controller) Metadata() broker.ClusterMetadata {
	c.mu.RLock()
	metadata := broker.ClusterMetadata{Brokers: c.sortedBrokers(), Partitions: make([]broker.PartitionMetadata, 0)}
	for _, partitions := range c.published {
		metadata.Partitions = append(metadata.Partitions, partitions...)
	}
	c.mu.RUnlock()
	sort.Slice(metadata.Partitions, func(i, j int) bool {
		a, b := metadata.Partitions[i], metadata.Partitions[j]
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})
	return metadata
}
