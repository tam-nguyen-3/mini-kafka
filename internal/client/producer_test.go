package client

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
)

type metadataFunc func(context.Context) (broker.ClusterMetadata, error)

func (f metadataFunc) Metadata(ctx context.Context) (broker.ClusterMetadata, error) { return f(ctx) }

type produceFunc func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error)

func (f produceFunc) Produce(ctx context.Context, b broker.BrokerInfo, tp broker.TopicPartition, k, v string) (int64, error) {
	return f(ctx, b, tp, k, v)
}
func snapshot(topics map[string]int) broker.ClusterMetadata {
	m := broker.ClusterMetadata{Brokers: []broker.BrokerInfo{{ID: 0, Addr: "b0"}, {ID: 1, Addr: "b1"}}}
	for topic, n := range topics {
		for i := 0; i < n; i++ {
			m.Partitions = append(m.Partitions, broker.PartitionMetadata{TopicPartition: broker.TopicPartition{Topic: topic, Partition: i}, Leader: broker.BrokerID(i % 2)})
		}
	}
	return m
}
func newProducer(t *testing.T, m MetadataClient, b BrokerClient) *Producer {
	t.Helper()
	p, err := NewProducer(m, b)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func wantError(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("got %v want %v", err, want)
	}
}
func await[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for synchronization")
		var zero T
		return zero
	}
}
func TestProducerDependenciesAndValidation(t *testing.T) {
	m := metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
		t.Fatal("unexpected metadata lookup")
		return broker.ClusterMetadata{}, nil
	})
	b := produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
		t.Fatal("unexpected produce")
		return 0, nil
	})
	for _, tc := range []struct {
		m MetadataClient
		b BrokerClient
	}{{nil, b}, {m, nil}, {nil, nil}, {metadataFunc(nil), b}, {m, produceFunc(nil)}} {
		_, err := NewProducer(tc.m, tc.b)
		wantError(t, err, ErrInvalidClient)
	}
	p := newProducer(t, m, b)
	part, offset, err := p.Send(context.Background(), " ", "", "")
	wantError(t, err, ErrInvalidTopicName)
	if part != -1 || offset != -1 {
		t.Fatalf("failed routing = %d,%d", part, offset)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	part, offset, err = p.Send(ctx, "t", "", "")
	wantError(t, err, context.Canceled)
	if part != -1 || offset != -1 {
		t.Fatalf("canceled routing = %d,%d", part, offset)
	}
}

func TestProducerCachedRoutingAndIndependentTopics(t *testing.T) {
	md := snapshot(map[string]int{"a": 3, "b": 3})
	calls := 0
	stores := map[broker.TopicPartition][]string{}
	p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { calls++; return md, nil }), produceFunc(func(_ context.Context, b broker.BrokerInfo, tp broker.TopicPartition, k, v string) (int64, error) {
		if b.ID != broker.BrokerID(tp.Partition%2) || b.Addr != fmt.Sprintf("b%d", b.ID) {
			t.Fatalf("route %s to %+v", tp, b)
		}
		off := int64(len(stores[tp]))
		stores[tp] = append(stores[tp], v)
		return off, nil
	}))
	for i := 0; i < 6; i++ {
		for _, topic := range []string{"a", "b"} {
			part, off, err := p.Send(context.Background(), topic, "", fmt.Sprint(i))
			if err != nil || part != i%3 || off != int64(i/3) {
				t.Fatalf("send %s %d = %d,%d,%v", topic, i, part, off, err)
			}
		}
	}
	if calls != 1 {
		t.Fatalf("metadata calls=%d", calls)
	}
	// The cache must not alias slices supplied by the metadata implementation.
	md.Brokers[0].Addr = "changed"
	md.Partitions[0].Leader = 1
	var partitioner DefaultPartitioner
	wantPartition := partitioner.Partition("hello", 3)
	previous := int64(len(stores[broker.TopicPartition{Topic: "a", Partition: wantPartition}])) - 1
	for i := 0; i < 4; i++ {
		part, off, err := p.Send(context.Background(), "a", "hello", "keyed")
		if err != nil || part != wantPartition || off != previous+1 {
			t.Fatalf("keyed send=%d,%d,%v", part, off, err)
		}
		previous = off
	}
	if calls != 1 {
		t.Fatal("cache hit contacted metadata")
	}
}

func TestMetadataValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*broker.ClusterMetadata)
	}{
		{"duplicate broker", func(m *broker.ClusterMetadata) { m.Brokers = append(m.Brokers, m.Brokers[0]) }},
		{"blank address", func(m *broker.ClusterMetadata) { m.Brokers[0].Addr = " " }},
		{"duplicate partition", func(m *broker.ClusterMetadata) { m.Partitions = append(m.Partitions, m.Partitions[0]) }},
		{"blank topic", func(m *broker.ClusterMetadata) { m.Partitions[0].Topic = " " }},
		{"negative partition", func(m *broker.ClusterMetadata) { m.Partitions[0].Partition = -1 }},
		{"gap", func(m *broker.ClusterMetadata) { m.Partitions[1].Partition = 3 }},
		{"no zero", func(m *broker.ClusterMetadata) { m.Partitions[0].Partition = 2 }},
		{"missing leader", func(m *broker.ClusterMetadata) { m.Partitions[0].Leader = 99 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			md := snapshot(map[string]int{"t": 2})
			tc.edit(&md)
			p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return md, nil }), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
				t.Fatal("invalid routing reached broker")
				return 0, nil
			}))
			part, off, err := p.Send(context.Background(), "t", "", "")
			wantError(t, err, ErrInvalidMetadata)
			if part != -1 || off != -1 || len(p.routes) != 0 || len(p.topics) != 0 {
				t.Fatal("invalid snapshot was published")
			}
		})
	}
	// An empty cluster is valid metadata, but contains no routable topics.
	p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return broker.ClusterMetadata{}, nil }), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
		t.Fatal("unexpected produce for missing topic")
		return 0, nil
	}))
	_, _, err := p.Send(context.Background(), "missing", "", "")
	wantError(t, err, broker.ErrUnknownTopicOrPartition)
}

func TestProducerRefreshRetryOriginalPartition(t *testing.T) {
	var metadataCalls int
	md := snapshot(map[string]int{"t": 3})
	var targets []broker.BrokerID
	var partitions []int
	reject := true
	p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
		metadataCalls++
		next := md
		next.Partitions = append([]broker.PartitionMetadata(nil), md.Partitions...)
		if metadataCalls > 1 {
			next.Partitions[0].Leader = 1
		}
		return next, nil
	}), produceFunc(func(_ context.Context, b broker.BrokerInfo, tp broker.TopicPartition, k, v string) (int64, error) {
		targets = append(targets, b.ID)
		partitions = append(partitions, tp.Partition)
		if reject {
			reject = false
			return 0, fmt.Errorf("old owner: %w", broker.ErrUnknownTopicOrPartition)
		}
		if k != "" || v != "payload" {
			t.Fatal("retry changed payload")
		}
		return 7, nil
	}))
	part, off, err := p.Send(context.Background(), "t", "", "payload")
	if err != nil || part != 0 || off != 7 || metadataCalls != 2 || !reflect.DeepEqual(targets, []broker.BrokerID{0, 1}) || !reflect.DeepEqual(partitions, []int{0, 0}) {
		t.Fatalf("send=%d,%d,%v metadata=%d targets=%v partitions=%v", part, off, err, metadataCalls, targets, partitions)
	}
	part, _, err = p.Send(context.Background(), "t", "", "payload")
	if err != nil || part != 1 {
		t.Fatalf("retry consumed round-robin turn: %d %v", part, err)
	}
}

func TestProducerRetryFailures(t *testing.T) {
	failure := errors.New("metadata unavailable")
	unknown := fmt.Errorf("not owner: %w", broker.ErrUnknownTopicOrPartition)
	for _, tc := range []struct {
		name         string
		refreshError error
		missing      bool
		produceError error
		want         error
		attempts     int
	}{
		{"second ownership rejection", nil, false, unknown, broker.ErrUnknownTopicOrPartition, 2},
		{"refresh failure", failure, false, unknown, failure, 1},
		{"topic disappeared", nil, true, unknown, broker.ErrUnknownTopicOrPartition, 1},
		{"ambiguous append", nil, false, failure, failure, 1},
		{"deadline", nil, false, context.DeadlineExceeded, context.DeadlineExceeded, 1},
		{"cancellation", nil, false, context.Canceled, context.Canceled, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadataCalls, attempts := 0, 0
			p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
				metadataCalls++
				if metadataCalls > 1 {
					if tc.refreshError != nil {
						return broker.ClusterMetadata{}, tc.refreshError
					}
					if tc.missing {
						return broker.ClusterMetadata{}, nil
					}
				}
				return snapshot(map[string]int{"t": 1}), nil
			}), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
				attempts++
				return 0, tc.produceError
			}))
			part, off, err := p.Send(context.Background(), "t", "", "")
			wantError(t, err, tc.want)
			if part != 0 || off != -1 || attempts != tc.attempts {
				t.Fatalf("send=%d,%d,%v attempts=%d", part, off, err, attempts)
			}
			wantMetadata := 1
			if errors.Is(tc.produceError, broker.ErrUnknownTopicOrPartition) {
				wantMetadata = 2
			}
			if metadataCalls != wantMetadata {
				t.Fatalf("metadata calls=%d", metadataCalls)
			}
		})
	}
}

func TestProducerRefreshValidationPreservesCache(t *testing.T) {
	for _, invalid := range []string{"count change", "malformed"} {
		t.Run(invalid, func(t *testing.T) {
			metadataCalls := 0
			p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
				metadataCalls++
				if metadataCalls == 1 {
					return snapshot(map[string]int{"t": 2}), nil
				}
				if invalid == "count change" {
					return snapshot(map[string]int{"t": 3}), nil
				}
				md := snapshot(map[string]int{"other": 1})
				md.Partitions[0].Leader = 99
				return md, nil
			}), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
				return 0, nil
			}))
			if _, _, err := p.Send(context.Background(), "t", "", ""); err != nil {
				t.Fatal(err)
			}
			_, _, err := p.Send(context.Background(), "other", "", "")
			wantError(t, err, ErrInvalidMetadata)
			part, _, err := p.Send(context.Background(), "t", "", "")
			if err != nil || part != 1 || metadataCalls != 2 {
				t.Fatalf("valid cache replaced/reset: %d %v, calls=%d", part, err, metadataCalls)
			}
		})
	}
}

// observedContext reports when code reaches a cancellation-aware select, so
// tests can coordinate queued operations without scheduler sleeps.
type observedContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
func observe(ctx context.Context) *observedContext {
	return &observedContext{Context: ctx, waiting: make(chan struct{})}
}

type sendResult struct {
	partition int
	offset    int64
	err       error
}

func sendAsync(p *Producer, ctx context.Context, topic, key, value string) <-chan sendResult {
	done := make(chan sendResult, 1)
	go func() { part, off, err := p.Send(ctx, topic, key, value); done <- sendResult{part, off, err} }()
	return done
}

func TestProducerPartitionSerializationAndIndependence(t *testing.T) {
	entered := make(chan string, 4)
	unblock := make(chan struct{})
	defer close(unblock)
	var mu sync.Mutex
	records := map[broker.TopicPartition][]string{}
	active := map[broker.TopicPartition]int{}
	p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
		return snapshot(map[string]int{"t": 2, "other": 1}), nil
	}), produceFunc(func(ctx context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, k, v string) (int64, error) {
		mu.Lock()
		active[tp]++
		overlap := active[tp] > 1
		mu.Unlock()
		if overlap {
			t.Error("same-partition calls overlapped")
		}
		defer func() { mu.Lock(); active[tp]--; mu.Unlock() }()
		entered <- v
		if v == "first" {
			select {
			case <-unblock:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		mu.Lock()
		defer mu.Unlock()
		off := int64(len(records[tp]))
		records[tp] = append(records[tp], v)
		return off, nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := sendAsync(p, ctx, "t", "", "first") // empty-key turn 0
	if got := await(t, entered); got != "first" {
		t.Fatal(got)
	}
	// FNV-1a("a") modulo 2 is zero: this send must queue behind first.
	waiter := observe(ctx)
	second := sendAsync(p, waiter, "t", "a", "second")
	await(t, waiter.waiting)
	third := sendAsync(p, ctx, "t", "", "healthy") // empty-key turn 1
	if got := await(t, entered); got != "healthy" {
		t.Fatalf("blocked partition leaked or stalled healthy partition: %s", got)
	}
	if got := await(t, third); got.err != nil || got.partition != 1 || got.offset != 0 {
		t.Fatalf("healthy=%+v", got)
	}
	other := sendAsync(p, ctx, "other", "", "other topic") // same numeric ID, different topic
	if got := await(t, entered); got != "other topic" {
		t.Fatal(got)
	}
	if got := await(t, other); got.err != nil || got.offset != 0 {
		t.Fatalf("other=%+v", got)
	}
	// Release the first append. Deferred close also unblocks it if the test fails.
	unblock <- struct{}{}
	if got := await(t, first); got.err != nil || got.offset != 0 {
		t.Fatalf("first=%+v", got)
	}
	if got := await(t, entered); got != "second" {
		t.Fatal(got)
	}
	if got := await(t, second); got.err != nil || got.partition != 0 || got.offset != 1 {
		t.Fatalf("second=%+v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := records[broker.TopicPartition{Topic: "t", Partition: 0}]; !reflect.DeepEqual(got, []string{"first", "second"}) {
		t.Fatalf("append order %v", got)
	}
}

func TestProducerCancellation(t *testing.T) {
	t.Run("metadata lookup", func(t *testing.T) {
		entered := make(chan struct{})
		p := newProducer(t, metadataFunc(func(ctx context.Context) (broker.ClusterMetadata, error) {
			close(entered)
			<-ctx.Done()
			return broker.ClusterMetadata{}, ctx.Err()
		}), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
			t.Fatal("unexpected broker call")
			return 0, nil
		}))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := sendAsync(p, ctx, "t", "", "")
		await(t, entered)
		cancel()
		got := await(t, done)
		wantError(t, got.err, context.Canceled)
		if got.partition != -1 || got.offset != -1 {
			t.Fatalf("result=%+v", got)
		}
	})
	t.Run("partition gate", func(t *testing.T) {
		var calls atomic.Int32
		p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return snapshot(map[string]int{"t": 1}), nil }), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
			calls.Add(1)
			return 0, nil
		}))
		if _, _, err := p.Send(context.Background(), "t", "", ""); err != nil {
			t.Fatal(err)
		}
		gate := p.topics["t"].gates[0]
		gate <- struct{}{}
		defer release(gate)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		waiting := observe(ctx)
		done := sendAsync(p, waiting, "t", "", "")
		await(t, waiting.waiting)
		cancel()
		got := await(t, done)
		wantError(t, got.err, context.Canceled)
		if got.partition != 0 || got.offset != -1 || calls.Load() != 1 {
			t.Fatalf("canceled send=%+v calls=%d", got, calls.Load())
		}
	})
	t.Run("broker call releases gate", func(t *testing.T) {
		entered := make(chan struct{})
		var calls atomic.Int32
		p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return snapshot(map[string]int{"t": 1}), nil }), produceFunc(func(ctx context.Context, _ broker.BrokerInfo, _ broker.TopicPartition, _, _ string) (int64, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return 0, nil
		}))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := sendAsync(p, ctx, "t", "", "")
		await(t, entered)
		cancel()
		got := await(t, done)
		wantError(t, got.err, context.Canceled)
		if got.partition != 0 || got.offset != -1 {
			t.Fatalf("send=%+v", got)
		}
		next, cancelNext := context.WithTimeout(context.Background(), time.Second)
		defer cancelNext()
		if _, _, err := p.Send(next, "t", "", ""); err != nil {
			t.Fatalf("gate retained after cancel: %v", err)
		}
	})
}

func TestProducerRefreshGate(t *testing.T) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	defer close(unblock)
	var calls atomic.Int32
	p := newProducer(t, metadataFunc(func(ctx context.Context) (broker.ClusterMetadata, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-unblock:
			case <-ctx.Done():
				return broker.ClusterMetadata{}, ctx.Err()
			}
		}
		return snapshot(map[string]int{"t": 1}), nil
	}), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
		return 0, nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := sendAsync(p, ctx, "t", "", "")
	await(t, entered)
	cancelCtx, cancelWaiter := context.WithCancel(ctx)
	defer cancelWaiter()
	waiter := observe(cancelCtx)
	done := sendAsync(p, waiter, "unknown", "", "")
	await(t, waiter.waiting)
	cancelWaiter()
	got := await(t, done)
	wantError(t, got.err, context.Canceled)
	if got.partition != -1 || got.offset != -1 {
		t.Fatalf("refresh cancellation=%+v", got)
	}
	// A second miss for the same topic must reuse the first completed refresh.
	secondCtx := observe(ctx)
	second := sendAsync(p, secondCtx, "t", "", "")
	await(t, secondCtx.waiting)
	unblock <- struct{}{}
	if got := await(t, first); got.err != nil {
		t.Fatal(got.err)
	}
	if got := await(t, second); got.err != nil {
		t.Fatal(got.err)
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent misses did %d metadata lookups", calls.Load())
	}
}

func TestProducerRetryKeepsPartitionGate(t *testing.T) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	defer close(unblock)
	var metadataCalls, produceCalls atomic.Int32
	p := newProducer(t, metadataFunc(func(ctx context.Context) (broker.ClusterMetadata, error) {
		if metadataCalls.Add(1) == 2 {
			close(entered)
			select {
			case <-unblock:
			case <-ctx.Done():
				return broker.ClusterMetadata{}, ctx.Err()
			}
		}
		return snapshot(map[string]int{"t": 1}), nil
	}), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
		n := produceCalls.Add(1)
		if n == 1 {
			return 0, broker.ErrUnknownTopicOrPartition
		}
		return int64(n - 2), nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := sendAsync(p, ctx, "t", "", "")
	await(t, entered)
	waiting := observe(ctx)
	second := sendAsync(p, waiting, "t", "", "")
	await(t, waiting.waiting)
	if produceCalls.Load() != 1 {
		t.Fatal("queued send overtook ownership retry")
	}
	unblock <- struct{}{}
	if got := await(t, first); got.err != nil || got.offset != 0 {
		t.Fatalf("retry=%+v", got)
	}
	if got := await(t, second); got.err != nil || got.offset != 1 {
		t.Fatalf("queued=%+v", got)
	}
}

func TestProducerConcurrentOffsets(t *testing.T) {
	b := &broker.Broker{}
	tp := broker.TopicPartition{Topic: "t"}
	b.AssignPartition(tp)
	var calls atomic.Int32
	p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) {
		calls.Add(1)
		return snapshot(map[string]int{"t": 1}), nil
	}), produceFunc(func(_ context.Context, _ broker.BrokerInfo, tp broker.TopicPartition, k, v string) (int64, error) {
		return b.Produce(tp, k, v)
	}))
	var wg sync.WaitGroup
	results := make(chan int64, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, off, err := p.Send(context.Background(), "t", "key", "v")
			if err != nil {
				t.Error(err)
				return
			}
			results <- off
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[int64]bool)
	for off := range results {
		if seen[off] {
			t.Errorf("duplicate %d", off)
		}
		seen[off] = true
	}
	if len(seen) != 100 || calls.Load() != 1 {
		t.Fatalf("offsets=%d metadata=%d", len(seen), calls.Load())
	}
	for i := 0; i < 100; i++ {
		if !seen[int64(i)] {
			t.Errorf("missing offset %d", i)
		}
	}
}

func TestProducerTopicMissRefresh(t *testing.T) {
	failure := errors.New("controller unavailable")
	var calls int
	var current broker.ClusterMetadata
	var metadataErr error
	p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { calls++; return current, metadataErr }), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
		return 0, nil
	}))
	metadataErr = failure
	part, off, err := p.Send(context.Background(), "t", "", "")
	wantError(t, err, failure)
	if part != -1 || off != -1 || calls != 1 {
		t.Fatalf("initial failure=%d,%d,%v calls=%d", part, off, err, calls)
	}
	metadataErr = nil
	_, _, err = p.Send(context.Background(), "t", "", "")
	wantError(t, err, broker.ErrUnknownTopicOrPartition)
	current = snapshot(map[string]int{"t": 2})
	part, _, err = p.Send(context.Background(), "t", "", "")
	if err != nil || part != 0 || calls != 3 {
		t.Fatalf("new topic not discovered: %d %v calls=%d", part, err, calls)
	}
	// A known route remains usable when metadata operations become unavailable.
	metadataErr = failure
	part, _, err = p.Send(context.Background(), "t", "", "")
	if err != nil || part != 1 || calls != 3 {
		t.Fatalf("cached route requires controller: %d %v calls=%d", part, err, calls)
	}
}

func TestProducerFailedAttemptConsumesOneTurn(t *testing.T) {
	var calls int
	failure := errors.New("ambiguous failure")
	p := newProducer(t, metadataFunc(func(context.Context) (broker.ClusterMetadata, error) { return snapshot(map[string]int{"t": 3}), nil }), produceFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, string, string) (int64, error) {
		calls++
		if calls == 1 {
			return 0, failure
		}
		return 0, nil
	}))
	part, off, err := p.Send(context.Background(), "t", "", "")
	wantError(t, err, failure)
	if part != 0 || off != -1 {
		t.Fatalf("failure=%d,%d", part, off)
	}
	part, _, err = p.Send(context.Background(), "t", "", "")
	if err != nil || part != 1 {
		t.Fatalf("failed attempt reset/skipped sequence: %d %v", part, err)
	}
}
