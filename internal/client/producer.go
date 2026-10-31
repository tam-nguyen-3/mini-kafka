// Package client models producer routing and standalone consumption without HTTP. Its clients
// can be network adapters or in-process fakes. Kafka partition offsets, not wall
// clocks, establish order; this iteration has no replication or deduplication.
package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
)

var (
	// ErrInvalidClient indicates a missing producer or consumer dependency.
	ErrInvalidClient = errors.New("nil client")
	// ErrInvalidMetadata indicates inconsistent routing or a changed partition count.
	ErrInvalidMetadata = errors.New("invalid metadata")
	// ErrInvalidTopicName indicates a blank client topic.
	ErrInvalidTopicName = errors.New("blank topic name")
)

// MetadataClient supplies published ownership. Implementations must honor ctx
// and bound network waits. The producer copies values before caching them.
type MetadataClient interface {
	Metadata(ctx context.Context) (broker.ClusterMetadata, error)
}

// BrokerClient appends at a selected owner. Implementations must honor ctx,
// bound network waits, and never retry an ambiguous append failure.
type BrokerClient interface {
	Produce(ctx context.Context, target broker.BrokerInfo, tp broker.TopicPartition, key, value string) (int64, error)
}

// Producer routes records using cached metadata and serializes sends within
// each topic-partition. Construct it with NewProducer; do not copy it.
// It is safe for concurrent use. Injected clients must also be concurrency-safe.
// Concurrent callers have no predetermined application order. Sequential
// successful sends to a partition get increasing offsets, but other producers
// may interleave records. There is no order across partitions or exactly-once
// delivery: a response failure may hide an append, and explicit retries may
// duplicate records. No background goroutines or Close operation are needed.
type Producer struct {
	metadata MetadataClient
	brokers  BrokerClient

	refreshGate chan struct{}
	mu          sync.Mutex
	routes      map[string][]broker.BrokerInfo
	// Topic state survives refresh/removal so fixed counts, send gates, and
	// round-robin sequences cannot reset while outstanding sends still use them.
	topics map[string]*producerTopic
}

type producerTopic struct {
	partitioner DefaultPartitioner
	gates       []chan struct{}
}

// NewProducer validates dependencies without making network calls.
func NewProducer(metadata MetadataClient, brokers BrokerClient) (*Producer, error) {
	if nilClient(metadata) || nilClient(brokers) {
		return nil, ErrInvalidClient
	}
	return &Producer{
		metadata: metadata, brokers: brokers,
		refreshGate: make(chan struct{}, 1),
		routes:      make(map[string][]broker.BrokerInfo),
		topics:      make(map[string]*producerTopic),
	}, nil
}

// Send selects one partition and appends at its owner. Metadata is loaded lazily
// and refreshed on a topic miss or a broker's ownership rejection; there is no
// TTL. Only an explicit ownership rejection permits one refresh and one retry,
// always for the original partition. All other errors return without retrying.
//
// Cancellation applies to metadata lookup, gate waits, and broker calls. Before
// partition selection, errors return (-1, -1, err); after selection they return
// (partition, -1, err). Failed sends may consume an empty-key round-robin turn.
func (p *Producer) Send(ctx context.Context, topic, key, value string) (int, int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, -1, fmt.Errorf("send topic %q: %w", topic, err)
	}
	if strings.TrimSpace(topic) == "" {
		return -1, -1, ErrInvalidTopicName
	}
	if err := p.ensureTopic(ctx, topic); err != nil {
		return -1, -1, fmt.Errorf("route topic %q: %w", topic, err)
	}

	p.mu.Lock()
	state := p.topics[topic]
	p.mu.Unlock()
	partition := state.partitioner.Partition(key, len(state.gates))
	tp := broker.TopicPartition{Topic: topic, Partition: partition}
	fail := func(err error) (int, int64, error) {
		return partition, -1, fmt.Errorf("send %s: %w", tp, err)
	}
	if err := acquire(ctx, state.gates[partition]); err != nil {
		return fail(err)
	}
	defer release(state.gates[partition])

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		// Resolve after waiting: another send may have refreshed ownership.
		target, err := p.owner(tp)
		if err != nil {
			return fail(err)
		}
		offset, err := p.brokers.Produce(ctx, target, tp, key, value)
		if err == nil {
			return partition, offset, nil
		}
		// An ownership rejection is an explicit refusal to append. Network
		// failures and other errors do not establish that refusal.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fail(ctxErr)
		}
		if attempt != 0 || !errors.Is(err, broker.ErrUnknownTopicOrPartition) {
			return fail(err)
		}
		if err := p.refresh(ctx, topic, true); err != nil {
			return fail(fmt.Errorf("refresh ownership: %w", err))
		}
	}
}

func (p *Producer) ensureTopic(ctx context.Context, topic string) error {
	p.mu.Lock()
	_, ok := p.routes[topic]
	p.mu.Unlock()
	if ok {
		return nil
	}
	return p.refresh(ctx, topic, false)
}

func (p *Producer) owner(tp broker.TopicPartition) (broker.BrokerInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	owners := p.routes[tp.Topic]
	if tp.Partition < 0 || tp.Partition >= len(owners) {
		return broker.BrokerInfo{}, fmt.Errorf("%w: %s", broker.ErrUnknownTopicOrPartition, tp)
	}
	return owners[tp.Partition], nil
}

func (p *Producer) refresh(ctx context.Context, topic string, force bool) error {
	if err := acquire(ctx, p.refreshGate); err != nil {
		return err
	}
	defer release(p.refreshGate)
	if !force {
		p.mu.Lock()
		_, ok := p.routes[topic]
		p.mu.Unlock()
		if ok {
			return nil
		}
	}

	snapshot, err := p.metadata.Metadata(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	routes, err := metadataRoutes(snapshot)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, owners := range routes {
		if state, ok := p.topics[name]; ok && len(owners) != len(state.gates) {
			return fmt.Errorf("%w: topic %q partition count changed from %d to %d", ErrInvalidMetadata, name, len(state.gates), len(owners))
		}
	}
	for name, owners := range routes {
		if _, ok := p.topics[name]; !ok {
			state := &producerTopic{gates: make([]chan struct{}, len(owners))}
			for i := range state.gates {
				state.gates[i] = make(chan struct{}, 1)
			}
			p.topics[name] = state
		}
	}
	p.routes = routes
	if _, ok := routes[topic]; !ok {
		return fmt.Errorf("%w: topic %q", broker.ErrUnknownTopicOrPartition, topic)
	}
	return nil
}
