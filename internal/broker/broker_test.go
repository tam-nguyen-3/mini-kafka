package broker

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestAssignPartitionPreservesRecords(t *testing.T) {
	var b Broker
	tp := TopicPartition{Topic: "orders", Partition: 0}
	b.AssignPartition(tp)
	p := b.partitions[tp]
	if p == nil || p.ID != tp.Partition {
		t.Fatalf("assigned partition = %+v", p)
	}
	p.Append("key", "value")
	b.AssignPartition(tp)
	if b.partitions[tp] != p || p.Log.Len() != 1 {
		t.Fatal("repeated assignment replaced storage or lost records")
	}
	other := TopicPartition{Topic: "payments", Partition: 0}
	b.AssignPartition(other)
	if b.partitions[other] == p || b.partitions[other].Log.Len() != 0 {
		t.Fatal("different topics share partition storage")
	}
}

func TestAssignPartitionConcurrent(t *testing.T) {
	var b Broker
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range 4 {
				b.AssignPartition(TopicPartition{Topic: "orders", Partition: id})
			}
		}()
	}
	wg.Wait()
	if len(b.partitions) != 4 {
		t.Fatalf("partition count = %d, want 4", len(b.partitions))
	}
}

func TestBrokerProduceFetch(t *testing.T) {
	var b Broker
	tp := TopicPartition{Topic: "orders", Partition: 0}
	b.AssignPartition(tp)
	for i, value := range []string{"first", "second", "third"} {
		offset, err := b.Produce(tp, "key", value)
		if err != nil || offset != int64(i) {
			t.Fatalf("Produce = (%d, %v), want (%d, nil)", offset, err, i)
		}
	}
	records, err := b.Fetch(tp, 1, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("Fetch = (%v, %v)", records, err)
	}
	if records[0].Offset != 1 || records[0].Key != "key" || records[0].Value != "second" {
		t.Fatalf("unexpected record: %+v", records[0])
	}
	records[0].Value = "changed"
	records, err = b.Fetch(tp, 1, 10)
	if err != nil || len(records) != 2 || records[0].Value != "second" || records[1].Offset != 2 {
		t.Fatalf("Fetch copy/order = (%v, %v)", records, err)
	}
	records, err = b.Fetch(tp, 3, 1)
	if err != nil || len(records) != 0 {
		t.Fatalf("Fetch at LEO = (%v, %v)", records, err)
	}
	for _, tc := range []struct {
		offset int64
		limit  int
		want   error
	}{
		{-1, 1, ErrOffsetOutOfRange}, {4, 1, ErrOffsetOutOfRange}, {0, 0, ErrInvalidMaxRecords}, {0, -1, ErrInvalidMaxRecords},
	} {
		if _, err := b.Fetch(tp, tc.offset, tc.limit); !errors.Is(err, tc.want) {
			t.Errorf("Fetch(%d, %d) error = %v, want %v", tc.offset, tc.limit, err, tc.want)
		}
	}
}

func TestBrokerOwnershipAndIsolation(t *testing.T) {
	var a, b, empty Broker
	tp0 := TopicPartition{Topic: "orders", Partition: 0}
	tp1 := TopicPartition{Topic: "orders", Partition: 1}
	a.AssignPartition(tp0)
	b.AssignPartition(tp1)
	for _, tc := range []struct {
		broker *Broker
		tp     TopicPartition
	}{{&a, tp1}, {&b, tp0}, {&empty, tp0}} {
		if _, err := tc.broker.Produce(tc.tp, "key", "value"); !errors.Is(err, ErrUnknownTopicOrPartition) {
			t.Fatalf("Produce ownership error = %v", err)
		}
		if _, err := tc.broker.Fetch(tc.tp, 0, 1); !errors.Is(err, ErrUnknownTopicOrPartition) {
			t.Fatalf("Fetch ownership error = %v", err)
		}
	}
	b.AssignPartition(tp0)
	if _, err := a.Produce(tp0, "key", "only a"); err != nil {
		t.Fatal(err)
	}
	records, err := b.Fetch(tp0, 0, 1)
	if err != nil || len(records) != 0 {
		t.Fatalf("brokers share storage: (%v, %v)", records, err)
	}
	if len(empty.partitions) != 0 {
		t.Fatal("unowned request created storage")
	}
}

func TestBrokerConcurrentProduceFetchAndAssign(t *testing.T) {
	var b Broker
	tp := TopicPartition{Topic: "orders", Partition: 0}
	b.AssignPartition(tp)
	const count = 100
	offsets := make(chan int64, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.AssignPartition(tp)
			offset, err := b.Produce(tp, "key", "value")
			if err != nil {
				t.Error(err)
				return
			}
			offsets <- offset
			if _, err := b.Fetch(tp, 0, count); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(offsets)
	seen := make(map[int64]bool)
	for offset := range offsets {
		if seen[offset] {
			t.Errorf("duplicate offset %d", offset)
		}
		seen[offset] = true
	}
	records, err := b.Fetch(tp, 0, count)
	if err != nil || len(records) != count {
		t.Fatalf("Fetch = (%d records, %v)", len(records), err)
	}
	for i, record := range records {
		if record.Offset != int64(i) || !seen[int64(i)] {
			t.Fatalf("missing or unordered offset %d", i)
		}
	}
}

func TestBrokerState(t *testing.T) {
	var b Broker
	if state := b.State(); state == nil || len(state) != 0 {
		t.Fatalf("empty State = %#v, want empty slice", state)
	}
	// Insert out of order; partition numbers must sort numerically.
	for _, tp := range []TopicPartition{
		{Topic: "payments", Partition: 0},
		{Topic: "orders", Partition: 10},
		{Topic: "orders", Partition: 2},
	} {
		b.AssignPartition(tp)
	}
	tp := TopicPartition{Topic: "orders", Partition: 2}
	for range 3 {
		if _, err := b.Produce(tp, "key", "value"); err != nil {
			t.Fatal(err)
		}
	}
	want := []PartitionState{
		{TopicPartition: tp, LogEndOffset: 3},
		{TopicPartition: TopicPartition{Topic: "orders", Partition: 10}, LogEndOffset: 0},
		{TopicPartition: TopicPartition{Topic: "payments", Partition: 0}, LogEndOffset: 0},
	}
	state := b.State()
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("State = %+v, want %+v", state, want)
	}
	if _, err := b.Produce(tp, "key", "new"); err != nil {
		t.Fatal(err)
	}
	if state[0].LogEndOffset != 3 {
		t.Fatal("previous snapshot changed after append")
	}
	state[0].Topic = "changed"
	state[0].LogEndOffset = -1
	next := b.State()
	if next[0].TopicPartition != tp || next[0].LogEndOffset != 4 {
		t.Fatalf("State did not preserve isolation or reflect append: %+v", next)
	}
	var other Broker
	other.AssignPartition(tp)
	if got := other.State(); len(got) != 1 || got[0].LogEndOffset != 0 {
		t.Fatalf("other broker State = %+v", got)
	}
}

func TestBrokerStateConcurrent(t *testing.T) {
	var b Broker
	const count = 100
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for id := range count {
			tp := TopicPartition{Topic: "orders", Partition: id}
			b.AssignPartition(tp)
			if _, err := b.Produce(tp, "key", "value"); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range count {
			state := b.State()
			for i, entry := range state {
				if entry.LogEndOffset < 0 || entry.LogEndOffset > 1 {
					t.Errorf("unexpected LEO %d", entry.LogEndOffset)
				}
				if i > 0 && state[i-1].Partition >= entry.Partition {
					t.Error("State is not sorted")
				}
			}
		}
	}()
	wg.Wait()
	state := b.State()
	if len(state) != count {
		t.Fatalf("State length = %d, want %d", len(state), count)
	}
	for _, entry := range state {
		if entry.LogEndOffset != 1 {
			t.Errorf("final LEO = %d, want 1", entry.LogEndOffset)
		}
	}
}
