package broker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrokerClockDoesNotOrderRecords(t *testing.T) {
	if _, err := NewWithClock(1, nil); !errors.Is(err, ErrInvalidClock) {
		t.Fatal(err)
	}
	values := []int64{100, 100, -5}
	var next atomic.Int64
	b, err := NewWithClock(1, func() time.Time { return time.UnixMilli(values[next.Add(1)-1]) })
	if err != nil {
		t.Fatal(err)
	}
	tp := TopicPartition{Topic: "x"}
	b.AssignPartition(tp)
	for i := range values {
		offset, err := b.Produce(tp, "k", "v")
		if err != nil || offset != int64(i) {
			t.Fatalf("%d %v", offset, err)
		}
		b.AssignPartition(tp)
	}
	records, err := b.Fetch(tp, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range records {
		if r.Offset != int64(i) || r.Timestamp != values[i] {
			t.Fatalf("%+v", records)
		}
	}
	other, _ := NewWithClock(2, func() time.Time { return time.UnixMilli(999) })
	other.AssignPartition(tp)
	other.Produce(tp, "", "")
	got, _ := other.Fetch(tp, 0, 1)
	if got[0].Timestamp != 999 || b.State()[0].LogEndOffset != 3 {
		t.Fatal("clock or storage leaked across brokers")
	}
}
func TestClockConcurrentPartitionsAndDefault(t *testing.T) {
	var ticks atomic.Int64
	b, _ := NewWithClock(1, func() time.Time { return time.UnixMilli(-ticks.Add(1)) })
	var wg sync.WaitGroup
	for p := 0; p < 4; p++ {
		tp := TopicPartition{Topic: "x", Partition: p}
		b.AssignPartition(tp)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if _, err := b.Produce(tp, "", ""); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if ticks.Load() != 100 {
		t.Fatal(ticks.Load())
	}
	for _, s := range b.State() {
		records, _ := b.Fetch(s.TopicPartition, 0, 100)
		for i, r := range records {
			if r.Offset != int64(i) || r.Timestamp >= 0 {
				t.Fatal(r)
			}
		}
	}
	before := time.Now().UnixMilli()
	var normal Broker
	tp := TopicPartition{Topic: "x"}
	normal.AssignPartition(tp)
	normal.Produce(tp, "", "")
	records, _ := normal.Fetch(tp, 0, 1)
	after := time.Now().UnixMilli()
	if records[0].Timestamp < before || records[0].Timestamp > after {
		t.Fatal(records)
	}
}
