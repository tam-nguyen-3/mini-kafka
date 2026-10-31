package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
)

type fetchFunc func(context.Context, broker.BrokerInfo, broker.TopicPartition, int64, int) (broker.FetchResult, error)

func (f fetchFunc) Fetch(ctx context.Context, b broker.BrokerInfo, tp broker.TopicPartition, o int64, n int) (broker.FetchResult, error) {
	return f(ctx, b, tp, o, n)
}
func newConsumer(t *testing.T, m MetadataClient, f FetchClient) *Consumer {
	t.Helper()
	c, err := NewConsumer(m, f)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func assign(t *testing.T, c *Consumer, tps ...broker.TopicPartition) {
	t.Helper()
	if err := c.Assign(context.Background(), tps...); err != nil {
		t.Fatal(err)
	}
}
func poll(t *testing.T, c *Consumer, max int) PollResult {
	t.Helper()
	r, err := c.Poll(context.Background(), max)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func noFetch(t *testing.T) fetchFunc {
	return func(context.Context, broker.BrokerInfo, broker.TopicPartition, int64, int) (broker.FetchResult, error) {
		t.Error("unexpected fetch")
		return broker.FetchResult{}, errors.New("unexpected fetch")
	}
}
func localConsumer(t *testing.T, topics map[string]int, counts map[broker.TopicPartition]int) (*Consumer, *broker.Broker) {
	t.Helper()
	b := &broker.Broker{}
	for topic, n := range topics {
		for i := 0; i < n; i++ {
			tp := broker.TopicPartition{Topic: topic, Partition: i}
			b.AssignPartition(tp)
			for j := 0; j < counts[tp]; j++ {
				if _, err := b.Produce(tp, "k", fmt.Sprintf("%s/%d", tp, j)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return snapshot(topics), nil }), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, o int64, n int) (broker.FetchResult, error) {
		return b.FetchWithLEO(tp, o, n)
	}))
	return c, b
}
func TestConsumerConstructionAndEmptyPoll(t *testing.T) {
	m := metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
		t.Error("unexpected metadata")
		return broker.ClusterMetadata{}, nil
	})
	f := noFetch(t)
	for _, tc := range []struct {
		m MetadataClient
		f FetchClient
	}{{nil, f}, {m, nil}, {metadataFunc(nil), f}, {m, fetchFunc(nil)}} {
		_, err := NewConsumer(tc.m, tc.f)
		wantError(t, err, ErrInvalidClient)
	}
	c := newConsumer(t, m, f)
	for _, max := range []int{0, -1} {
		r, err := c.Poll(context.Background(), max)
		wantError(t, err, broker.ErrInvalidMaxRecords)
		if r.Records == nil || r.PartitionErrors == nil {
			t.Fatal("nil error result lists")
		}
	}
	r := poll(t, c, 3)
	if r.Records == nil || r.PartitionErrors == nil || len(r.Records) != 0 || len(r.PartitionErrors) != 0 {
		t.Fatalf("empty=%+v", r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Poll(ctx, 1)
	wantError(t, err, context.Canceled)
	wantError(t, c.Assign(ctx, broker.TopicPartition{Topic: "t"}), context.Canceled)
	wantError(t, c.Seek(ctx, broker.TopicPartition{Topic: "t"}, 0), context.Canceled)
}

func TestConsumerAssignmentAndSeek(t *testing.T) {
	ctx := context.Background()
	a := broker.TopicPartition{Topic: "a"}
	b := broker.TopicPartition{Topic: "b", Partition: 1}
	c, _ := localConsumer(t, map[string]int{"a": 1, "b": 2}, map[broker.TopicPartition]int{a: 4, b: 4})
	input := []broker.TopicPartition{b, a, b}
	assign(t, c, input...)
	input[0].Topic = "changed"
	first := poll(t, c, 2)
	if len(first.Records) != 2 || first.Records[0].TopicPartition != a || first.Records[1].TopicPartition != b {
		t.Fatalf("sorted/deduped assignment: %+v", first)
	}
	for _, tp := range []broker.TopicPartition{{Topic: " "}, {Topic: "a", Partition: -1}} {
		err := c.Assign(ctx, tp)
		if tp.Topic == " " {
			wantError(t, err, ErrInvalidTopicName)
		} else {
			wantError(t, err, ErrInvalidAssignment)
		}
	}
	// Invalid replacement leaves the old set and positions intact.
	next := poll(t, c, 2)
	if len(next.Records) != 2 || next.Records[0].Offset != 1 || next.Records[1].Offset != 1 {
		t.Fatalf("invalid assignment mutated positions: %+v", next)
	}
	assign(t, c, b)
	if got := poll(t, c, 1); len(got.Records) != 1 || got.Records[0].Offset != 2 {
		t.Fatalf("retained position: %+v", got)
	}
	assign(t, c, a, b)
	got := poll(t, c, 2)
	if len(got.Records) != 2 || got.Records[0].TopicPartition != a || got.Records[0].Offset != 0 || got.Records[1].Offset != 3 {
		t.Fatalf("overlap/new positions: %+v", got)
	}
	wantError(t, c.Seek(ctx, broker.TopicPartition{Topic: "absent"}, 0), ErrPartitionNotAssigned)
	wantError(t, c.Seek(ctx, a, -1), broker.ErrOffsetOutOfRange)
	if err := c.Seek(ctx, a, 0); err != nil {
		t.Fatal(err)
	}
	assign(t, c, a)
	replay := poll(t, c, 1)
	if len(replay.Records) != 1 || replay.Records[0].Offset != 0 {
		t.Fatalf("seek failed: %+v", replay)
	}
	if err := c.Seek(ctx, a, 10); err != nil {
		t.Fatal("Seek must not contact broker:", err)
	}
	for i := 0; i < 2; i++ {
		r := poll(t, c, 5)
		if len(r.Records) != 0 || len(r.PartitionErrors) != 1 {
			t.Fatalf("beyond LEO: %+v", r)
		}
		wantError(t, r.PartitionErrors[0], broker.ErrOffsetOutOfRange)
	}
	if err := c.Seek(ctx, a, 0); err != nil {
		t.Fatal(err)
	}
	if r := poll(t, c, 1); len(r.Records) != 1 || r.Records[0].Offset != 0 {
		t.Fatalf("explicit recovery failed: %+v", r)
	}
	assign(t, c)
	if r := poll(t, c, 5); len(r.Records) != 0 || len(r.PartitionErrors) != 0 {
		t.Fatalf("clear failed: %+v", r)
	}
	assign(t, c, a)
	if r := poll(t, c, 1); len(r.Records) != 1 || r.Records[0].Offset != 0 {
		t.Fatal("clear retained a removed position")
	}
}

func TestConsumerFairnessAndNoSkippedRecords(t *testing.T) {
	tps := []broker.TopicPartition{{Topic: "t", Partition: 0}, {Topic: "t", Partition: 1}, {Topic: "t", Partition: 2}}
	counts := map[broker.TopicPartition]int{tps[0]: 5, tps[1]: 2, tps[2]: 4}
	c, _ := localConsumer(t, map[string]int{"t": 3}, counts)
	assign(t, c, tps...)
	for i := 0; i < 3; i++ {
		r := poll(t, c, 1)
		if len(r.Records) != 1 || r.Records[0].TopicPartition != tps[i] || r.Records[0].Offset != 0 {
			t.Fatalf("rotating start %d: %+v", i, r)
		}
	}
	next := map[broker.TopicPartition]int64{tps[0]: 1, tps[1]: 1, tps[2]: 1}
	for i := 0; i < 6; i++ {
		r := poll(t, c, 3)
		if len(r.Records) > 3 || len(r.PartitionErrors) != 0 {
			t.Fatalf("limit/errors: %+v", r)
		}
		for _, record := range r.Records {
			if record.Offset != next[record.TopicPartition] {
				t.Fatalf("skip/duplicate: %+v next=%v", record, next)
			}
			next[record.TopicPartition]++
		}
	}
	for tp, n := range counts {
		if next[tp] != int64(n) {
			t.Fatalf("%s consumed %d want %d", tp, next[tp], n)
		}
	}
	// Result ownership: editing a prior return cannot affect the broker or position.
	if err := c.Seek(context.Background(), tps[0], 0); err != nil {
		t.Fatal(err)
	}
	r := poll(t, c, 1)
	r.Records[0].Value = "edited"
	if err := c.Seek(context.Background(), tps[0], 0); err != nil {
		t.Fatal(err)
	}
	if got := poll(t, c, 1); got.Records[0].Value == "edited" {
		t.Fatal("result aliases stored data")
	}
}

func TestConsumerEmptyPartitionsAndCatchUp(t *testing.T) {
	a, b := broker.TopicPartition{Topic: "t"}, broker.TopicPartition{Topic: "t", Partition: 1}
	c, store := localConsumer(t, map[string]int{"t": 2}, map[broker.TopicPartition]int{b: 3})
	assign(t, c, a, b)
	r := poll(t, c, 100)
	if len(r.Records) != 3 || len(r.PartitionErrors) != 0 {
		t.Fatalf("uneven backlog: %+v", r)
	}
	if _, err := store.Produce(a, "", "new"); err != nil {
		t.Fatal(err)
	}
	r = poll(t, c, 100)
	if len(r.Records) != 1 || r.Records[0].TopicPartition != a || r.Records[0].Offset != 0 {
		t.Fatalf("new data: %+v", r)
	}
}

func TestConsumerPartialFailureAndRecovery(t *testing.T) {
	failure := errors.New("node unavailable")
	var failed = true
	var offsets []int64
	c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return snapshot(map[string]int{"t": 2}), nil }), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, o int64, n int) (broker.FetchResult, error) {
		if n != 1 {
			t.Errorf("requested %d records", n)
		}
		if tp.Partition == 0 {
			offsets = append(offsets, o)
			if failed {
				return broker.FetchResult{}, failure
			}
		}
		if o == 3 {
			return broker.FetchResult{LogEndOffset: 3}, nil
		}
		return broker.FetchResult{Records: []broker.Record{{Offset: o, Timestamp: 100 - o}}, LogEndOffset: 3}, nil
	}))
	assign(t, c, broker.TopicPartition{Topic: "t"}, broker.TopicPartition{Topic: "t", Partition: 1})
	r := poll(t, c, 10)
	if len(r.Records) != 3 || len(r.PartitionErrors) != 1 || r.PartitionErrors[0].Partition != 0 {
		t.Fatalf("partial=%+v", r)
	}
	wantError(t, r.PartitionErrors[0], failure)
	if !reflect.DeepEqual(offsets, []int64{0}) {
		t.Fatalf("failed partition retried within poll: %v", offsets)
	}
	failed = false
	r = poll(t, c, 10)
	if len(r.Records) != 3 || len(r.PartitionErrors) != 0 || r.Records[0].Offset != 0 {
		t.Fatalf("recovery=%+v", r)
	}
	if !reflect.DeepEqual(offsets, []int64{0, 0, 1, 2}) {
		t.Fatalf("failed position changed: %v", offsets)
	}
}

func TestConsumerInvalidFetchResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result broker.FetchResult
	}{
		{"too many", broker.FetchResult{Records: []broker.Record{{Offset: 0}, {Offset: 1}}, LogEndOffset: 2}},
		{"wrong offset", broker.FetchResult{Records: []broker.Record{{Offset: 1}}, LogEndOffset: 2}},
		{"negative offset", broker.FetchResult{Records: []broker.Record{{Offset: -1}}, LogEndOffset: 2}},
		{"record at LEO", broker.FetchResult{Records: []broker.Record{{Offset: 0}}, LogEndOffset: 0}},
		{"negative LEO", broker.FetchResult{LogEndOffset: -1}},
		{"empty before LEO", broker.FetchResult{LogEndOffset: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return snapshot(map[string]int{"t": 1}), nil }), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, _ broker.TopicPartition, o int64, _ int) (broker.FetchResult, error) {
				calls++
				if o != 0 {
					t.Fatal("malformed response advanced position")
				}
				return tc.result, nil
			}))
			assign(t, c, broker.TopicPartition{Topic: "t"})
			for i := 0; i < 2; i++ {
				r := poll(t, c, 10)
				if len(r.Records) != 0 || len(r.PartitionErrors) != 1 {
					t.Fatalf("bad result accepted: %+v", r)
				}
				wantError(t, r.PartitionErrors[0], ErrInvalidFetchResponse)
			}
			if calls != 2 {
				t.Fatal("malformed responses retried")
			}
		})
	}
	// Validate boundary arithmetic without allocating enormous logs.
	if err := validateFetch(broker.FetchResult{LogEndOffset: math.MaxInt64}, math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	wantError(t, validateFetch(broker.FetchResult{Records: []broker.Record{{Offset: math.MaxInt64}}, LogEndOffset: math.MaxInt64}, math.MaxInt64), ErrInvalidFetchResponse)
	if err := validateFetch(broker.FetchResult{Records: []broker.Record{{Offset: math.MaxInt64 - 1}}, LogEndOffset: math.MaxInt64}, math.MaxInt64-1); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerMetadataCacheAndMisses(t *testing.T) {
	md := snapshot(map[string]int{"t": 1})
	var metadataErr error
	calls := 0
	c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { calls++; return md, metadataErr }), fetchFunc(func(_ context.Context, b broker.BrokerInfo, _ broker.TopicPartition, o int64, _ int) (broker.FetchResult, error) {
		if b.Addr != "b0" {
			t.Fatalf("cache aliased metadata: %+v", b)
		}
		return broker.FetchResult{Records: []broker.Record{{Offset: o}}, LogEndOffset: 100}, nil
	}))
	tp := broker.TopicPartition{Topic: "t"}
	assign(t, c, tp)
	if r := poll(t, c, 1); len(r.Records) != 1 || calls != 1 {
		t.Fatalf("initial=%+v calls=%d", r, calls)
	}
	md.Brokers[0].Addr = "mutated"
	md.Partitions[0].Leader = 1
	if r := poll(t, c, 1); len(r.Records) != 1 || calls != 1 {
		t.Fatalf("cached=%+v calls=%d", r, calls)
	}
	failure := errors.New("metadata unavailable")
	metadataErr = failure
	missing := broker.TopicPartition{Topic: "missing"}
	assign(t, c, tp, missing)
	r := poll(t, c, 2)
	if len(r.Records) != 2 || len(r.PartitionErrors) != 1 || r.PartitionErrors[0].TopicPartition != missing || calls != 2 {
		t.Fatalf("partial missing=%+v calls=%d", r, calls)
	}
	wantError(t, r.PartitionErrors[0], failure)
	metadataErr = nil
	md = snapshot(map[string]int{"t": 1})
	r = poll(t, c, 1)
	if len(r.PartitionErrors) != 1 {
		t.Fatalf("missing after successful refresh: %+v", r)
	}
	wantError(t, r.PartitionErrors[0], broker.ErrUnknownTopicOrPartition)
	md = snapshot(map[string]int{"t": 1, "missing": 1})
	r = poll(t, c, 2)
	if len(r.Records) != 2 || len(r.PartitionErrors) != 0 {
		t.Fatalf("new topic not discovered: %+v", r)
	}
}

func TestConsumerMissingRouteCancellationAllowsHealthyProgress(t *testing.T) {
	healthy := broker.TopicPartition{Topic: "healthy"}
	missing := broker.TopicPartition{Topic: "absent"} // Sorted before healthy.
	entered := make(chan struct{})
	var calls atomic.Int32
	var blockMetadata atomic.Bool
	c := newConsumer(t, metadataFunc(func(ctx context.Context) (broker.ClusterMetadata, error) {
		if calls.Add(1) == 1 {
			return snapshot(map[string]int{"healthy": 1}), nil
		}
		if !blockMetadata.Load() {
			return broker.ClusterMetadata{}, errors.New("unexpected metadata refresh during healthy visit")
		}
		entered <- struct{}{}
		<-ctx.Done()
		return broker.ClusterMetadata{}, ctx.Err()
	}), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, offset int64, _ int) (broker.FetchResult, error) {
		if tp != healthy {
			t.Errorf("unexpected fetch: %s", tp)
		}
		return broker.FetchResult{Records: []broker.Record{{Offset: offset}}, LogEndOffset: 100}, nil
	}))
	assign(t, c, healthy)
	poll(t, c, 1) // Cache the healthy route and consume offset zero.
	assign(t, c, missing, healthy)
	for offset := int64(1); offset <= 3; offset++ {
		blockMetadata.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		done := pollAsync(c, ctx, 1)
		await(t, entered)
		cancel()
		got := await(t, done)
		wantError(t, got.err, context.Canceled)
		if len(got.result.Records) != 0 || len(got.result.PartitionErrors) != 0 {
			t.Fatalf("canceled missing visit: %+v", got)
		}
		blockMetadata.Store(false)
		before := calls.Load()
		r := poll(t, c, 1)
		if len(r.Records) != 1 || r.Records[0].TopicPartition != healthy || r.Records[0].Offset != offset || len(r.PartitionErrors) != 0 {
			t.Fatalf("healthy progress at offset %d: %+v", offset, r)
		}
		if calls.Load() != before {
			t.Fatal("healthy visit refreshed metadata before reaching record limit")
		}
	}
}

func TestConsumerMissingRouteCancellationRetainsPartialResults(t *testing.T) {
	healthy := broker.TopicPartition{Topic: "healthy"}
	missing := broker.TopicPartition{Topic: "missing"} // Sorted after healthy.
	entered := make(chan struct{})
	var calls atomic.Int32
	c := newConsumer(t, metadataFunc(func(ctx context.Context) (broker.ClusterMetadata, error) {
		if calls.Add(1) == 1 {
			return snapshot(map[string]int{"healthy": 1}), nil
		}
		close(entered)
		<-ctx.Done()
		return broker.ClusterMetadata{}, ctx.Err()
	}), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, _ broker.TopicPartition, offset int64, _ int) (broker.FetchResult, error) {
		return broker.FetchResult{Records: []broker.Record{{Offset: offset}}, LogEndOffset: 100}, nil
	}))
	assign(t, c, healthy)
	poll(t, c, 1)
	assign(t, c, healthy, missing)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := pollAsync(c, ctx, 2)
	await(t, entered)
	cancel()
	got := await(t, done)
	wantError(t, got.err, context.Canceled)
	if len(got.result.Records) != 1 || got.result.Records[0].Offset != 1 || len(got.result.PartitionErrors) != 0 {
		t.Fatalf("lost partial record: %+v", got)
	}
	if c.positions[healthy] != 2 || c.positions[missing] != 0 {
		t.Fatalf("incorrect positions after cancellation: %v", c.positions)
	}
	if r := poll(t, c, 1); len(r.Records) != 1 || r.Records[0].Offset != 2 || len(r.PartitionErrors) != 0 || calls.Load() != 2 {
		t.Fatalf("redelivery or eager refresh: %+v, metadata calls=%d", r, calls.Load())
	}
}

func TestConsumerInvalidMetadataPreservesCache(t *testing.T) {
	for _, kind := range []string{"duplicate broker", "count change"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
				calls++
				if calls == 1 {
					return snapshot(map[string]int{"t": 1}), nil
				}
				md := snapshot(map[string]int{"t": 2, "missing": 1})
				if kind == "duplicate broker" {
					md.Brokers = append(md.Brokers, md.Brokers[0])
				}
				return md, nil
			}), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, _ broker.TopicPartition, o int64, _ int) (broker.FetchResult, error) {
				return broker.FetchResult{Records: []broker.Record{{Offset: o}}, LogEndOffset: 10}, nil
			}))
			tp := broker.TopicPartition{Topic: "t"}
			assign(t, c, tp)
			poll(t, c, 1)
			assign(t, c, tp, broker.TopicPartition{Topic: "missing"})
			r := poll(t, c, 2)
			if len(r.Records) != 2 || len(r.PartitionErrors) != 1 {
				t.Fatalf("cached routes discarded: %+v", r)
			}
			wantError(t, r.PartitionErrors[0], ErrInvalidMetadata)
			if len(c.routes["t"]) != 1 {
				t.Fatal("invalid snapshot was published")
			}
		})
	}
	// An empty snapshot is valid, but all assigned partitions are unresolved.
	c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return broker.ClusterMetadata{}, nil }), noFetch(t))
	assign(t, c, broker.TopicPartition{Topic: "t"})
	r := poll(t, c, 1)
	if len(r.PartitionErrors) != 1 {
		t.Fatal(r)
	}
	wantError(t, r.PartitionErrors[0], broker.ErrUnknownTopicOrPartition)
}

func TestConsumerOwnershipRefreshClearsMissingRouteError(t *testing.T) {
	failure := errors.New("metadata unavailable")
	metadataCalls, fetchCalls := 0, 0
	c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
		metadataCalls++
		if metadataCalls == 2 {
			return broker.ClusterMetadata{}, failure
		}
		return snapshot(map[string]int{"t": 1}), nil
	}), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, offset int64, _ int) (broker.FetchResult, error) {
		fetchCalls++
		if tp.Topic != "t" {
			t.Errorf("unexpected fetch: %s", tp)
		}
		if fetchCalls == 2 {
			return broker.FetchResult{}, broker.ErrUnknownTopicOrPartition
		}
		return broker.FetchResult{Records: []broker.Record{{Offset: offset}}, LogEndOffset: 2}, nil
	}))
	tp := broker.TopicPartition{Topic: "t"}
	assign(t, c, tp)
	poll(t, c, 1)
	// The first miss fails; t's ownership refresh succeeds before the next miss.
	absent := broker.TopicPartition{Topic: "absent"}
	missing := broker.TopicPartition{Topic: "missing"}
	later := broker.TopicPartition{Topic: "z_missing"}
	assign(t, c, absent, missing, tp, later)
	r := poll(t, c, 10)
	if metadataCalls != 3 || fetchCalls != 3 || len(r.Records) != 1 || r.Records[0].Offset != 1 || len(r.PartitionErrors) != 3 {
		t.Fatalf("refresh bounds/partial results: %+v metadata=%d fetch=%d", r, metadataCalls, fetchCalls)
	}
	for i, tp := range []broker.TopicPartition{absent, missing, later} {
		if r.PartitionErrors[i].TopicPartition != tp {
			t.Fatalf("wrong failed partition: %+v", r.PartitionErrors[i])
		}
		if tp == later {
			wantError(t, r.PartitionErrors[i], broker.ErrUnknownTopicOrPartition)
		} else {
			wantError(t, r.PartitionErrors[i], failure)
		}
	}
}

func TestConsumerOwnershipRefreshBound(t *testing.T) {
	failure := errors.New("refresh unavailable")
	for _, tc := range []struct {
		name                 string
		refreshErr           error
		rejectAgain, missing bool
		want                 error
	}{
		{name: "new owner"},
		{name: "second rejection", rejectAgain: true, want: broker.ErrUnknownTopicOrPartition},
		{name: "refresh failed", refreshErr: failure, want: failure},
		{name: "topic disappeared", missing: true, want: broker.ErrUnknownTopicOrPartition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadataCalls := 0
			attempts := map[int]int{}
			c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
				metadataCalls++
				if metadataCalls > 1 {
					if tc.refreshErr != nil {
						return broker.ClusterMetadata{}, tc.refreshErr
					}
					if tc.missing {
						return broker.ClusterMetadata{}, nil
					}
				}
				md := snapshot(map[string]int{"t": 2})
				if metadataCalls > 1 {
					for i := range md.Partitions {
						md.Partitions[i].Leader = 1 - md.Partitions[i].Leader
					}
				}
				return md, nil
			}), fetchFunc(func(_ context.Context, b broker.BrokerInfo, tp broker.TopicPartition, o int64, n int) (broker.FetchResult, error) {
				attempts[tp.Partition]++
				if o != 0 || n != 1 {
					t.Fatalf("retry changed position/limit: %d %d", o, n)
				}
				old := broker.BrokerID(tp.Partition % 2)
				if b.ID == old || tc.rejectAgain {
					return broker.FetchResult{}, fmt.Errorf("old owner: %w", broker.ErrUnknownTopicOrPartition)
				}
				return broker.FetchResult{Records: []broker.Record{{Offset: 0}}, LogEndOffset: 1}, nil
			}))
			assign(t, c, broker.TopicPartition{Topic: "t"}, broker.TopicPartition{Topic: "t", Partition: 1})
			r := poll(t, c, 10)
			if metadataCalls != 2 {
				t.Fatalf("metadata refresh storm: %d", metadataCalls)
			}
			if tc.want == nil {
				if len(r.Records) != 2 || len(r.PartitionErrors) != 0 || attempts[0] != 2 || attempts[1] != 1 {
					t.Fatalf("retry success: %+v attempts=%v", r, attempts)
				}
			} else {
				if len(r.Records) != 0 || len(r.PartitionErrors) != 2 {
					t.Fatalf("failure=%+v", r)
				}
				for _, e := range r.PartitionErrors {
					wantError(t, e, tc.want)
				}
				if attempts[0] > 2 || attempts[1] > 2 {
					t.Fatalf("too many retries %v", attempts)
				}
			}
		})
	}
}

func TestConsumerRetriesLaterRejectionAfterSharedRefresh(t *testing.T) {
	metadataCalls := 0
	attempts := map[int]int{}
	c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
		metadataCalls++
		return snapshot(map[string]int{"t": 2}), nil
	}), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, o int64, _ int) (broker.FetchResult, error) {
		attempts[tp.Partition]++
		if attempts[tp.Partition] == 1 {
			return broker.FetchResult{}, broker.ErrUnknownTopicOrPartition
		}
		return broker.FetchResult{Records: []broker.Record{{Offset: o}}, LogEndOffset: 1}, nil
	}))
	assign(t, c, broker.TopicPartition{Topic: "t"}, broker.TopicPartition{Topic: "t", Partition: 1})
	r := poll(t, c, 10)
	if metadataCalls != 2 || attempts[0] != 2 || attempts[1] != 2 || len(r.Records) != 2 || len(r.PartitionErrors) != 0 {
		t.Fatalf("shared refresh=%+v calls=%d attempts=%v", r, metadataCalls, attempts)
	}
}

func TestConsumerNoRetryOtherFetchErrors(t *testing.T) {
	for _, cause := range []error{errors.New("node unavailable"), context.DeadlineExceeded, context.Canceled, broker.ErrOffsetOutOfRange} {
		t.Run(cause.Error(), func(t *testing.T) {
			calls := 0
			metadataCalls := 0
			c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
				metadataCalls++
				return snapshot(map[string]int{"t": 1}), nil
			}), fetchFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, int64, int) (broker.FetchResult, error) {
				calls++
				return broker.FetchResult{}, cause
			}))
			assign(t, c, broker.TopicPartition{Topic: "t"})
			r := poll(t, c, 10)
			if len(r.PartitionErrors) != 1 || calls != 1 || metadataCalls != 1 {
				t.Fatalf("unexpected retry: %+v fetch=%d metadata=%d", r, calls, metadataCalls)
			}
			wantError(t, r.PartitionErrors[0], cause)
		})
	}
}

type pollOutcome struct {
	result PollResult
	err    error
}

func pollAsync(c *Consumer, ctx context.Context, max int) <-chan pollOutcome {
	done := make(chan pollOutcome, 1)
	go func() { r, e := c.Poll(ctx, max); done <- pollOutcome{r, e} }()
	return done
}

func TestConsumerOperationsSerializeAndCancel(t *testing.T) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	defer close(unblock)
	var calls atomic.Int32
	c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return snapshot(map[string]int{"t": 1}), nil }), fetchFunc(func(ctx context.Context, _ broker.BrokerInfo, _ broker.TopicPartition, o int64, _ int) (broker.FetchResult, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-unblock:
			case <-ctx.Done():
				return broker.FetchResult{}, ctx.Err()
			}
		}
		return broker.FetchResult{Records: []broker.Record{{Offset: o}}, LogEndOffset: 3}, nil
	}))
	tp := broker.TopicPartition{Topic: "t"}
	assign(t, c, tp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := pollAsync(c, ctx, 1)
	await(t, entered)
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{"Assign", func(ctx context.Context) error { return c.Assign(ctx, broker.TopicPartition{Topic: "replacement"}) }},
		{"Seek", func(ctx context.Context) error { return c.Seek(ctx, tp, 0) }},
		{"Poll", func(ctx context.Context) error { _, err := c.Poll(ctx, 1); return err }},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			waitCtx, cancelWait := context.WithCancel(ctx)
			defer cancelWait()
			observed := observe(waitCtx)
			done := make(chan error, 1)
			go func() { done <- op.call(observed) }()
			await(t, observed.waiting)
			cancelWait()
			wantError(t, await(t, done), context.Canceled)
		})
	}
	// A live queued seek must apply only after the running Poll finishes.
	waiting := observe(ctx)
	seekDone := make(chan error, 1)
	go func() { seekDone <- c.Seek(waiting, tp, 0) }()
	await(t, waiting.waiting)
	unblock <- struct{}{}
	if got := await(t, first); got.err != nil || len(got.result.Records) != 1 || got.result.Records[0].Offset != 0 {
		t.Fatalf("first=%+v", got)
	}
	if err := await(t, seekDone); err != nil {
		t.Fatal(err)
	}
	if got := poll(t, c, 1); got.Records[0].Offset != 0 {
		t.Fatalf("queued seek raced poll: %+v", got)
	}
}

func TestConsumerCancellationRetainsPartialResults(t *testing.T) {
	for _, phase := range []string{"fetch", "ownership refresh"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			var stalled atomic.Bool
			var mdCalls atomic.Int32
			failure := errors.New("partition zero unavailable")
			c := newConsumer(t, metadataFunc(func(ctx context.Context) (broker.ClusterMetadata, error) {
				if mdCalls.Add(1) == 2 && phase == "ownership refresh" {
					close(entered)
					<-ctx.Done()
					return broker.ClusterMetadata{}, ctx.Err()
				}
				return snapshot(map[string]int{"t": 3}), nil
			}), fetchFunc(func(ctx context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, o int64, _ int) (broker.FetchResult, error) {
				if tp.Partition == 0 {
					return broker.FetchResult{}, failure
				}
				if tp.Partition == 2 && !stalled.Swap(true) {
					if phase == "ownership refresh" {
						return broker.FetchResult{}, broker.ErrUnknownTopicOrPartition
					}
					close(entered)
					<-ctx.Done()
					return broker.FetchResult{}, ctx.Err()
				}
				return broker.FetchResult{Records: []broker.Record{{Offset: o}}, LogEndOffset: 3}, nil
			}))
			tps := []broker.TopicPartition{{Topic: "t"}, {Topic: "t", Partition: 1}, {Topic: "t", Partition: 2}}
			assign(t, c, tps...)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := pollAsync(c, ctx, 10)
			await(t, entered)
			cancel()
			got := await(t, done)
			wantError(t, got.err, context.Canceled)
			if len(got.result.Records) != 1 || got.result.Records[0].Partition != 1 || len(got.result.PartitionErrors) != 1 {
				t.Fatalf("lost partial results: %+v", got)
			}
			wantError(t, got.result.PartitionErrors[0], failure)
			if c.positions[tps[0]] != 0 || c.positions[tps[1]] != 1 || c.positions[tps[2]] != 0 {
				t.Fatalf("wrong cancellation positions: %v", c.positions)
			}
			next := poll(t, c, 10)
			if len(next.Records) != 5 || next.Records[0].Partition != 1 || next.Records[0].Offset != 1 {
				t.Fatalf("positions after cancellation: %+v", next)
			}
		})
	}
}

func TestConsumerInitialMetadataCancellationAndError(t *testing.T) {
	entered := make(chan struct{})
	c := newConsumer(t, metadataFunc(func(ctx context.Context) (broker.ClusterMetadata, error) {
		close(entered)
		<-ctx.Done()
		return broker.ClusterMetadata{}, ctx.Err()
	}), noFetch(t))
	assign(t, c, broker.TopicPartition{Topic: "t"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := pollAsync(c, ctx, 1)
	await(t, entered)
	cancel()
	got := await(t, done)
	wantError(t, got.err, context.Canceled)
	if len(got.result.Records) != 0 || len(got.result.PartitionErrors) != 0 || len(c.routes) != 0 {
		t.Fatalf("initial cancel: %+v", got)
	}
	failure := errors.New("controller unavailable")
	c = newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return broker.ClusterMetadata{}, failure }), noFetch(t))
	assign(t, c, broker.TopicPartition{Topic: "t"}, broker.TopicPartition{Topic: "t", Partition: 1})
	r := poll(t, c, 5)
	if len(r.PartitionErrors) != 2 || len(r.Records) != 0 {
		t.Fatalf("metadata failure: %+v", r)
	}
	for _, err := range r.PartitionErrors {
		wantError(t, err, failure)
	}
}

func TestConsumerConcurrentPolls(t *testing.T) {
	tp := broker.TopicPartition{Topic: "t"}
	c, _ := localConsumer(t, map[string]int{"t": 1}, map[broker.TopicPartition]int{tp: 30})
	assign(t, c, tp)
	var wg sync.WaitGroup
	offsets := make(chan int64, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Poll(context.Background(), 1)
			if err != nil || len(r.Records) != 1 {
				t.Errorf("poll=%+v %v", r, err)
				return
			}
			offsets <- r.Records[0].Offset
		}()
	}
	wg.Wait()
	close(offsets)
	seen := map[int64]bool{}
	for off := range offsets {
		if seen[off] {
			t.Errorf("duplicate %d", off)
		}
		seen[off] = true
	}
	if len(seen) != 30 {
		t.Fatalf("consumed %d unique offsets", len(seen))
	}
}

func TestConsumerBrokerRestartRequiresExplicitSeek(t *testing.T) {
	ctx := context.Background()
	tp := broker.TopicPartition{Topic: "t"}
	store := &broker.Broker{}
	store.AssignPartition(tp)
	for i := 0; i < 3; i++ {
		if _, err := store.Produce(tp, "", "old"); err != nil {
			t.Fatal(err)
		}
	}
	c := newConsumer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return snapshot(map[string]int{"t": 1}), nil }), fetchFunc(func(_ context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, o int64, n int) (broker.FetchResult, error) {
		return store.FetchWithLEO(tp, o, n)
	}))
	assign(t, c, tp)
	if r := poll(t, c, 10); len(r.Records) != 3 {
		t.Fatal(r)
	}
	// Simulate the same owner restarting with empty in-memory storage.
	store = &broker.Broker{}
	store.AssignPartition(tp)
	if _, err := store.Produce(tp, "", "new after restart"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		r := poll(t, c, 10)
		if len(r.Records) != 0 || len(r.PartitionErrors) != 1 {
			t.Fatalf("silently reset position: %+v", r)
		}
		wantError(t, r.PartitionErrors[0], broker.ErrOffsetOutOfRange)
	}
	if err := c.Seek(ctx, tp, 0); err != nil {
		t.Fatal(err)
	}
	r := poll(t, c, 10)
	if len(r.Records) != 1 || r.Records[0].Offset != 0 || r.Records[0].Value != "new after restart" {
		t.Fatalf("explicit reset failed: %+v", r)
	}
}
