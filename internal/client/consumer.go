package client

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
)

var (
	// ErrPartitionNotAssigned indicates a seek outside the current assignment.
	ErrPartitionNotAssigned = errors.New("partition not assigned")
	// ErrInvalidAssignment indicates a negative assignment partition ID.
	ErrInvalidAssignment = errors.New("invalid assignment")
	// ErrInvalidFetchResponse indicates records or LEO inconsistent with a fetch.
	ErrInvalidFetchResponse = errors.New("invalid fetch response")
)

// FetchClient reads one partition at a specified owner. Implementations must
// honor ctx and bound network waits; Consumer itself does not impose a timeout.
type FetchClient interface {
	Fetch(ctx context.Context, target broker.BrokerInfo, tp broker.TopicPartition, offset int64, maxRecords int) (broker.FetchResult, error)
}

// ConsumerRecord identifies a record's partition. Interleaving records from
// several partitions establishes no global order or wall-clock causality.
type ConsumerRecord struct {
	broker.TopicPartition
	broker.Record
}

// PartitionError identifies a failed visit and preserves its underlying cause.
type PartitionError struct {
	broker.TopicPartition
	Err error
}

func (e PartitionError) Error() string { return fmt.Sprintf("poll %s: %v", e.TopicPartition, e.Err) }
func (e PartitionError) Unwrap() error { return e.Err }

// PollResult retains successful records alongside partition-specific errors.
// Inspect PartitionErrors even when Poll's overall error is nil. Lists are
// non-nil and owned by the caller, including when an overall error is returned.
type PollResult struct {
	Records         []ConsumerRecord
	PartitionErrors []PartitionError
}

// Consumer tracks local fetch positions, without groups or committed offsets.
// Construct with NewConsumer; do not copy. All operations are safe concurrently
// and serialize through a cancellation-aware operation gate. No background
// goroutines or Close are needed. Position advances mean returned to the caller,
// not processed or durably acknowledged; restarting this object loses positions.
type Consumer struct {
	metadata MetadataClient
	fetcher  FetchClient
	gate     chan struct{}
	// Only the operation holding gate accesses state. This deliberately serializes
	// Poll/Assign/Seek, without holding a state mutex across network calls.
	assignment []broker.TopicPartition
	positions  map[broker.TopicPartition]int64
	cursor     int
	routes     map[string][]broker.BrokerInfo
	counts     map[string]int
}

// NewConsumer validates clients, including typed nil, without making I/O calls.
func NewConsumer(metadata MetadataClient, fetcher FetchClient) (*Consumer, error) {
	if nilClient(metadata) || nilClient(fetcher) {
		return nil, ErrInvalidClient
	}
	return &Consumer{metadata: metadata, fetcher: fetcher, gate: make(chan struct{}, 1), positions: make(map[broker.TopicPartition]int64), routes: make(map[string][]broker.BrokerInfo), counts: make(map[string]int)}, nil
}

// Assign replaces the assignment atomically, retaining positions only for
// overlapping partitions. New partitions start at zero; no arguments clears
// everything. Duplicates are removed and the visit cursor resets to sorted order.
// Validation is local: unavailable or unpublished partitions can be assigned.
func (c *Consumer) Assign(ctx context.Context, tps ...broker.TopicPartition) error {
	if err := acquire(ctx, c.gate); err != nil {
		return err
	}
	defer release(c.gate)
	positions := make(map[broker.TopicPartition]int64, len(tps))
	for _, tp := range tps {
		if strings.TrimSpace(tp.Topic) == "" {
			return fmt.Errorf("assign %s: %w", tp, ErrInvalidTopicName)
		}
		if tp.Partition < 0 {
			return fmt.Errorf("%w: %s", ErrInvalidAssignment, tp)
		}
		positions[tp] = c.positions[tp]
	}
	assignment := make([]broker.TopicPartition, 0, len(positions))
	for tp := range positions {
		assignment = append(assignment, tp)
	}
	sort.Slice(assignment, func(i, j int) bool {
		if assignment[i].Topic != assignment[j].Topic {
			return assignment[i].Topic < assignment[j].Topic
		}
		return assignment[i].Partition < assignment[j].Partition
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	c.assignment, c.positions, c.cursor = assignment, positions, 0
	return nil
}

// Seek changes an assigned partition's next fetch offset without contacting a
// broker. Beyond-LEO positions fail on Poll and never reset automatically, even
// after in-memory broker data loss. Explicit Seek is required to recover.
func (c *Consumer) Seek(ctx context.Context, tp broker.TopicPartition, offset int64) error {
	if err := acquire(ctx, c.gate); err != nil {
		return err
	}
	defer release(c.gate)
	if _, ok := c.positions[tp]; !ok {
		return fmt.Errorf("%w: %s", ErrPartitionNotAssigned, tp)
	}
	if offset < 0 {
		return fmt.Errorf("%w: %d", broker.ErrOffsetOutOfRange, offset)
	}
	c.positions[tp] = offset
	return nil
}

// Poll returns at most max records, fetching one per partition per round in
// rotating order. Empty/failed partitions are skipped for the rest of this poll.
// Fetches are sequential: a slow request delays later visits until its bounded
// client timeout. Set a caller deadline to bound the entire poll.
//
// Partition failures are in PollResult.PartitionErrors, not the overall error.
// Only visited partitions can trigger metadata lookup or partition errors.
// Input/cancellation errors use the overall error. Cancellation retains accepted
// records and their advanced positions; unfinished visits never advance them.
// A subsequent poll revisits failures. Offsets order records within a partition;
// timestamps and the returned cross-partition interleaving define no global order.
func (c *Consumer) Poll(ctx context.Context, max int) (PollResult, error) {
	out := PollResult{Records: make([]ConsumerRecord, 0), PartitionErrors: make([]PartitionError, 0)}
	if max < 1 {
		return out, broker.ErrInvalidMaxRecords
	}
	if err := acquire(ctx, c.gate); err != nil {
		return out, err
	}
	defer release(c.gate)
	if len(c.assignment) == 0 {
		return out, nil
	}

	var refresh pollRefreshState
	done := make([]bool, len(c.assignment))
	active := len(done)
	for active > 0 && len(out.Records) < max {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		i := c.cursor
		// Advance before I/O so a canceled visit cannot starve later partitions.
		c.cursor = (i + 1) % len(c.assignment)
		if done[i] {
			continue
		}
		tp := c.assignment[i]
		offset := c.positions[tp]
		result, err := c.fetchOne(ctx, tp, offset, &refresh)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return out, ctxErr
		}
		if err == nil {
			err = validateFetch(result, offset)
		}
		if err != nil {
			out.PartitionErrors = append(out.PartitionErrors, PartitionError{TopicPartition: tp, Err: err})
			done[i] = true
			active--
			continue
		}
		if len(result.Records) == 1 {
			record := result.Records[0]
			out.Records = append(out.Records, ConsumerRecord{TopicPartition: tp, Record: record})
			c.positions[tp] = record.Offset + 1 // validation proves this is below or at LEO
		}
		if c.positions[tp] == result.LogEndOffset {
			done[i] = true
			active--
		}
	}
	return out, ctx.Err()
}

// One missing-route refresh and one ownership refresh are allowed per Poll.
// All visits share their outcomes, including failures.
type pollRefreshState struct {
	missingAttempted   bool
	missingErr         error
	ownershipAttempted bool
	ownershipErr       error
}

// fetchOne resolves a route on demand and retries only an ownership rejection.
// It leaves response validation and position changes to Poll.
func (c *Consumer) fetchOne(ctx context.Context, tp broker.TopicPartition, offset int64, refresh *pollRefreshState) (broker.FetchResult, error) {
	target, err := c.owner(tp)
	if err != nil {
		if !refresh.missingAttempted {
			refresh.missingAttempted = true
			refresh.missingErr = c.refreshRoutes(ctx)
		}
		if refresh.missingErr != nil {
			return broker.FetchResult{}, fmt.Errorf("refresh missing route: %w", refresh.missingErr)
		}
		target, err = c.owner(tp)
		if err != nil {
			return broker.FetchResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return broker.FetchResult{}, err
	}
	result, err := c.fetcher.Fetch(ctx, target, tp, offset, 1)
	if ctx.Err() != nil || !errors.Is(err, broker.ErrUnknownTopicOrPartition) {
		return result, err
	}
	if !refresh.ownershipAttempted {
		refresh.ownershipAttempted = true
		refresh.ownershipErr = c.refreshRoutes(ctx)
		if refresh.ownershipErr == nil {
			refresh.missingErr = nil
		}
	}
	if refresh.ownershipErr != nil {
		return broker.FetchResult{}, fmt.Errorf("refresh ownership: %w", refresh.ownershipErr)
	}
	target, err = c.owner(tp)
	if err != nil {
		return broker.FetchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return broker.FetchResult{}, err
	}
	return c.fetcher.Fetch(ctx, target, tp, offset, 1)
}

func validateFetch(result broker.FetchResult, offset int64) error {
	if len(result.Records) > 1 || result.LogEndOffset < offset {
		return ErrInvalidFetchResponse
	}
	if len(result.Records) == 0 {
		if result.LogEndOffset != offset {
			return ErrInvalidFetchResponse
		}
	} else if result.Records[0].Offset != offset || offset >= result.LogEndOffset {
		return ErrInvalidFetchResponse
	}
	return nil
}

func (c *Consumer) owner(tp broker.TopicPartition) (broker.BrokerInfo, error) {
	owners := c.routes[tp.Topic]
	if tp.Partition < 0 || tp.Partition >= len(owners) {
		return broker.BrokerInfo{}, fmt.Errorf("%w: %s", broker.ErrUnknownTopicOrPartition, tp)
	}
	return owners[tp.Partition], nil
}

func (c *Consumer) refreshRoutes(ctx context.Context) error {
	snapshot, err := c.metadata.Metadata(ctx)
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
	for topic, owners := range routes {
		if count, known := c.counts[topic]; known && count != len(owners) {
			return fmt.Errorf("%w: topic %q partition count changed from %d to %d", ErrInvalidMetadata, topic, count, len(owners))
		}
	}
	for topic, owners := range routes {
		c.counts[topic] = len(owners)
	}
	c.routes = routes
	return nil
}
