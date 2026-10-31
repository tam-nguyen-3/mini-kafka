package broker

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrUnknownTopicOrPartition indicates that this broker does not host a partition.
var ErrUnknownTopicOrPartition = errors.New("unknown topic or partition")

// ErrInvalidClock indicates a missing record timestamp source.
var ErrInvalidClock = errors.New("record clock must not be nil")

// Broker holds local partition storage. Its zero value is ready for assignment.
// A Broker must not be copied after first use.
type Broker struct {
	ID         BrokerID
	mu         sync.RWMutex
	partitions map[TopicPartition]*Partition
	now        func() time.Time
}

// NewWithClock supplies observational record timestamps for clock experiments.
// now must be safe for concurrent calls across partitions and must not call back
// into this broker. It never controls offsets, routing, or network deadlines.
// A nil clock returns ErrInvalidClock; the zero-value Broker uses time.Now.
func NewWithClock(id BrokerID, now func() time.Time) (*Broker, error) {
	if now == nil {
		return nil, ErrInvalidClock
	}
	return &Broker{ID: id, now: now}, nil
}

// AssignPartition creates local storage if tp is not already hosted.
// Repeated assignments preserve existing records. It is safe for concurrent use.
func (b *Broker) AssignPartition(tp TopicPartition) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.partitions == nil {
		b.partitions = make(map[TopicPartition]*Partition)
	}
	if _, exists := b.partitions[tp]; exists {
		return
	}
	partition := NewPartition(tp.Partition)
	partition.Log.now = b.now
	b.partitions[tp] = partition
}

// Produce appends to a hosted partition and returns its assigned offset.
// It returns an error wrapping ErrUnknownTopicOrPartition if tp is not hosted.
func (b *Broker) Produce(tp TopicPartition, key, value string) (int64, error) {
	b.mu.RLock()
	p, exists := b.partitions[tp]
	b.mu.RUnlock()
	if !exists {
		return 0, fmt.Errorf("%w: %s", ErrUnknownTopicOrPartition, tp)
	}

	record := p.Append(key, value)
	return record.Offset, nil
}

// Fetch returns a copy of up to maxRecords records starting at offset.
// It returns an error wrapping ErrUnknownTopicOrPartition if tp is not hosted;
// otherwise it propagates ReadFrom's offset and limit errors.
func (b *Broker) Fetch(tp TopicPartition, offset int64, maxRecords int) ([]Record, error) {
	result, err := b.FetchWithLEO(tp, offset, maxRecords)
	return result.Records, err
}

// FetchResult is one partition's copied records and log-end offset at fetch time.
// LEO is the next append offset, not a cross-partition clock or committed offset.
type FetchResult struct {
	Records      []Record `json:"records"`
	LogEndOffset int64    `json:"logEndOffset"`
}

// FetchWithLEO returns an atomic log snapshot with the same errors as Fetch.
func (b *Broker) FetchWithLEO(tp TopicPartition, offset int64, maxRecords int) (FetchResult, error) {
	b.mu.RLock()
	p, exists := b.partitions[tp]
	b.mu.RUnlock()
	if !exists {
		return FetchResult{}, fmt.Errorf("%w: %s", ErrUnknownTopicOrPartition, tp)
	}

	records, leo, err := p.Log.ReadFromWithLEO(offset, maxRecords)
	return FetchResult{Records: records, LogEndOffset: leo}, err
}

// PartitionState describes a hosted partition's next append offset, without
// exposing its mutable storage.
type PartitionState struct {
	TopicPartition
	LogEndOffset int64 `json:"logEndOffset"`
}

// State returns a fresh slice of hosted partitions, sorted by topic and then
// partition number. An empty broker returns an empty slice.
// Each log-end offset is read safely, but concurrent appends mean the result
// is not an atomic snapshot of all logs at one instant.
func (b *Broker) State() []PartitionState {
	b.mu.RLock()
	state := make([]PartitionState, 0, len(b.partitions))
	for tp, p := range b.partitions {
		state = append(state, PartitionState{
			TopicPartition: tp,
			LogEndOffset:   p.Log.NextOffset(),
		})
	}
	b.mu.RUnlock()

	sort.Slice(state, func(i, j int) bool {
		if state[i].Topic != state[j].Topic {
			return state[i].Topic < state[j].Topic
		}
		return state[i].Partition < state[j].Partition
	})
	return state
}
