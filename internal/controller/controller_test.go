package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
)

type clientFunc func(context.Context, broker.BrokerInfo, broker.TopicPartition) error

func (f clientFunc) AssignPartition(ctx context.Context, b broker.BrokerInfo, tp broker.TopicPartition) error {
	return f(ctx, b, tp)
}

func newController(t *testing.T, client BrokerClient, ids ...broker.BrokerID) *Controller {
	t.Helper()
	c, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := c.RegisterBroker(id, fmt.Sprintf("broker-%d", id)); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func noop(context.Context, broker.BrokerInfo, broker.TopicPartition) error { return nil }

func TestValidation(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrInvalidBrokerClient) {
		t.Fatalf("New(nil): %v", err)
	}
	c := newController(t, clientFunc(noop))
	metadata := c.Metadata()
	if metadata.Brokers == nil || metadata.Partitions == nil {
		t.Fatal("empty metadata must contain non-nil slices")
	}
	for _, tc := range []struct {
		name  string
		count int
		want  error
	}{
		{"", 1, ErrInvalidTopicName},
		{" \t", 1, ErrInvalidTopicName},
		{"orders", 0, ErrInvalidPartitionCount},
		{"orders", -1, ErrInvalidPartitionCount},
		{"orders", 1, ErrNoBrokers},
	} {
		if err := c.CreateTopic(context.Background(), tc.name, tc.count); !errors.Is(err, tc.want) {
			t.Errorf("CreateTopic(%q, %d): %v, want %v", tc.name, tc.count, err, tc.want)
		}
	}
	if err := c.RegisterBroker(1, " \t"); !errors.Is(err, ErrInvalidBrokerAddress) {
		t.Fatalf("blank address: %v", err)
	}
	for _, id := range []broker.BrokerID{-1, 0, 1} {
		for range 2 {
			if err := c.RegisterBroker(id, "address"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := c.RegisterBroker(1, "changed"); !errors.Is(err, ErrBrokerConflict) {
		t.Fatalf("address conflict: %v", err)
	}
	if got := c.Metadata().Brokers; len(got) != 3 || got[2].Addr != "address" {
		t.Fatalf("registry changed after conflict: %v", got)
	}
}

func TestPlacementAndMetadataCopies(t *testing.T) {
	for _, count := range []int{1, 2, 6, 8} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var calls []assignment
			c := newController(t, clientFunc(func(_ context.Context, b broker.BrokerInfo, tp broker.TopicPartition) error {
				calls = append(calls, assignment{target: b, tp: tp})
				return nil
			}), 3, 1, 2)
			for _, topic := range []string{"zebra", "orders"} {
				if err := c.CreateTopic(context.Background(), topic, count); err != nil {
					t.Fatal(err)
				}
				if err := c.CreateTopic(context.Background(), topic, count); !errors.Is(err, ErrTopicAlreadyExists) {
					t.Fatalf("duplicate topic: %v", err)
				}
			}
			if len(calls) != count*2 {
				t.Fatalf("assignment calls: %v", calls)
			}
			for i, call := range calls {
				p := i % count
				if call.tp.Partition != p || call.target.ID != broker.BrokerID(p%3+1) || call.target.Addr != fmt.Sprintf("broker-%d", p%3+1) {
					t.Fatalf("incorrect assignment: %+v", call)
				}
			}
			want := c.Metadata()
			if len(want.Partitions) != 2*count {
				t.Fatalf("published %d partitions, want %d", len(want.Partitions), 2*count)
			}
			for i, b := range want.Brokers {
				if b.ID != broker.BrokerID(i+1) {
					t.Fatal("unsorted brokers")
				}
			}
			for i, p := range want.Partitions {
				topic := "orders"
				if i >= count {
					topic = "zebra"
				}
				if p.Topic != topic || p.Partition != i%count || p.Leader != broker.BrokerID((i%count)%3+1) {
					t.Fatalf("incorrect metadata: %+v", want)
				}
			}
			copy := c.Metadata()
			copy.Brokers[0].Addr = "mutated"
			copy.Partitions[0].Leader = 99
			if !reflect.DeepEqual(c.Metadata(), want) {
				t.Fatal("snapshot aliases state")
			}
			if err := c.RegisterBroker(4, "broker-4"); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.Metadata().Partitions, want.Partitions) {
				t.Fatal("joining broker moved partitions")
			}
		})
	}
}

func TestPartialFailureRetryPreservesPlanAndRecords(t *testing.T) {
	lostResponse := errors.New("response lost")
	brokers := map[broker.BrokerID]*broker.Broker{1: {ID: 1}, 2: {ID: 2}}
	var calls []assignment
	fail := true
	c := newController(t, clientFunc(func(_ context.Context, target broker.BrokerInfo, tp broker.TopicPartition) error {
		calls = append(calls, assignment{target: target, tp: tp})
		brokers[target.ID].AssignPartition(tp)
		if fail && tp.Partition == 1 {
			return lostResponse
		}
		return nil
	}), 2, 1)
	if err := c.CreateTopic(context.Background(), "orders", 4); !errors.Is(err, lostResponse) || !strings.Contains(err.Error(), "orders-1") || !strings.Contains(err.Error(), "broker 2") {
		t.Fatalf("assignment error lacks cause/context: %v", err)
	}
	if len(c.Metadata().Partitions) != 0 {
		t.Fatal("pending topic advertised")
	}
	tp := broker.TopicPartition{Topic: "orders", Partition: 1}
	if _, err := brokers[2].Produce(tp, "key", "preserved"); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterBroker(3, "broker-3"); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateTopic(context.Background(), "orders", 5); !errors.Is(err, ErrPendingTopicConflict) {
		t.Fatalf("pending conflict: %v", err)
	}
	if len(calls) != 2 {
		t.Fatal("conflicting create performed assignments")
	}
	fail = false
	if err := c.CreateTopic(context.Background(), "orders", 4); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 6 || !reflect.DeepEqual(calls[:2], calls[2:4]) {
		t.Fatalf("retry did not replay plan: %v", calls)
	}
	for _, p := range c.Metadata().Partitions {
		if p.Leader != broker.BrokerID(p.Partition%2+1) {
			t.Fatal("retry changed placement")
		}
	}
	records, err := brokers[2].Fetch(tp, 0, 10)
	if err != nil || len(records) != 1 || records[0].Value != "preserved" {
		t.Fatalf("retry lost records: %v, %v", records, err)
	}
}

// await bounds synchronization failures without relying on sleeps for ordering.
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for test synchronization")
	}
	var zero T
	return zero
}

func TestBlockedAssignmentAllowsMetadataRegistrationAndCancellation(t *testing.T) {
	entered := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	c := newController(t, clientFunc(func(ctx context.Context, _ broker.BrokerInfo, _ broker.TopicPartition) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}), 1)
	done := make(chan error, 1)
	go func() { done <- c.CreateTopic(ctx, "orders", 2) }()
	await(t, entered)
	readDone := make(chan broker.ClusterMetadata, 1)
	go func() {
		if err := c.RegisterBroker(2, "broker-2"); err != nil {
			readDone <- broker.ClusterMetadata{}
			return
		}
		readDone <- c.Metadata()
	}()
	if metadata := await(t, readDone); len(metadata.Brokers) != 2 || len(metadata.Partitions) != 0 {
		t.Fatalf("state unavailable or pending published: %+v", metadata)
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	waiting := make(chan error, 1)
	go func() { waiting <- c.CreateTopic(waitCtx, "other", 1) }()
	cancelWait()
	if err := await(t, waiting); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting cancellation: %v", err)
	}
	cancel()
	if err := await(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("assignment cancellation: %v", err)
	}
	if len(c.Metadata().Partitions) != 0 {
		t.Fatal("canceled topic published")
	}
	if err := c.CreateTopic(context.Background(), "orders", 3); !errors.Is(err, ErrPendingTopicConflict) {
		t.Fatalf("cancellation discarded pending plan: %v", err)
	}
}

func TestConcurrentCreatesSerialize(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls []broker.TopicPartition // all callbacks must be serialized
	c := newController(t, clientFunc(func(_ context.Context, _ broker.BrokerInfo, tp broker.TopicPartition) error {
		calls = append(calls, tp)
		if len(calls) == 1 {
			close(entered)
			<-release
		}
		return nil
	}), 1, 2)
	first := make(chan error, 1)
	go func() { first <- c.CreateTopic(context.Background(), "orders", 3) }()
	await(t, entered)
	results := make(chan error, 3)
	for _, request := range []struct {
		name  string
		count int
	}{{"orders", 3}, {"orders", 4}, {"other", 2}} {
		go func() { results <- c.CreateTopic(context.Background(), request.name, request.count) }()
	}
	once.Do(func() { close(release) })
	if err := await(t, first); err != nil {
		t.Fatal(err)
	}
	duplicates := 0
	for range 3 {
		err := await(t, results)
		if errors.Is(err, ErrTopicAlreadyExists) {
			duplicates++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if duplicates != 2 || len(calls) != 5 {
		t.Fatalf("duplicates=%d, calls=%v", duplicates, calls)
	}
	for i, tp := range calls {
		want := "orders"
		if i >= 3 {
			want = "other"
		}
		if tp.Topic != want {
			t.Fatalf("interleaved assignment: %v", calls)
		}
	}
}

func TestCancellationBeforeAndBetweenAssignments(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			c := newController(t, clientFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition) error {
				calls++
				cancel()
				return nil // cancellation can race with a successful acknowledgment
			}), 1)
			if err := c.CreateTopic(ctx, "orders", count); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			if calls != 1 || len(c.Metadata().Partitions) != 0 {
				t.Fatalf("continued after cancellation: calls=%d, metadata=%v", calls, c.Metadata())
			}
			if err := c.CreateTopic(context.Background(), "orders", count+1); !errors.Is(err, ErrPendingTopicConflict) {
				t.Fatalf("pending plan discarded: %v", err)
			}
		})
	}
	calls := 0
	c := newController(t, clientFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition) error {
		calls++
		return nil
	}), 1)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := c.CreateTopic(ctx, "orders", 3); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired deadline: %v", err)
	}
	if err := c.CreateTopic(context.Background(), "orders", 1); err != nil {
		t.Fatalf("pre-canceled create reserved a plan or kept the gate: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expired context performed assignments: %d calls", calls)
	}
}
