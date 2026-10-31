package broker

import (
	"errors"
	"math"
	"sync"
	"testing"
)

func TestFetchSnapshot(t *testing.T) {
	b := &Broker{}
	tp := TopicPartition{Topic: "events", Partition: 0}
	b.AssignPartition(tp)
	for i := 0; i < 3; i++ {
		if _, err := b.Produce(tp, "k", "v"); err != nil {
			t.Fatal(err)
		}
	}
	result, err := b.FetchWithLEO(tp, 1, 1)
	if err != nil || result.LogEndOffset != 3 || len(result.Records) != 1 || result.Records[0].Offset != 1 {
		t.Fatalf("snapshot = %+v, %v", result, err)
	}
	result.Records[0].Value = "changed"
	again, err := b.FetchWithLEO(tp, 1, math.MaxInt)
	if err != nil || len(again.Records) != 2 || again.Records[0].Value != "v" {
		t.Fatalf("copy = %+v, %v", again, err)
	}
	empty, err := b.FetchWithLEO(tp, 3, math.MaxInt)
	if err != nil || empty.Records == nil || len(empty.Records) != 0 || empty.LogEndOffset != 3 {
		t.Fatalf("empty = %+v, %v", empty, err)
	}
	for _, tc := range []struct {
		tp     TopicPartition
		offset int64
		max    int
		want   error
	}{
		{tp, -1, 1, ErrOffsetOutOfRange}, {tp, 4, 1, ErrOffsetOutOfRange}, {tp, 0, 0, ErrInvalidMaxRecords},
		{TopicPartition{Topic: "other"}, 0, 1, ErrUnknownTopicOrPartition},
	} {
		if _, err := b.FetchWithLEO(tc.tp, tc.offset, tc.max); !errors.Is(err, tc.want) {
			t.Fatalf("got %v, want %v", err, tc.want)
		}
	}
}

func TestFetchSnapshotConcurrentAppend(t *testing.T) {
	var log Log
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3000; i++ {
			log.Append("", "v")
		}
	}()
	defer wg.Wait()
	for i := 0; i < 3000; i++ {
		records, leo, err := log.ReadFromWithLEO(0, math.MaxInt)
		// Fetching from zero with an unlimited-enough count distinguishes an atomic
		// snapshot from separately reading records and then a newer LEO.
		if err != nil || int64(len(records)) != leo {
			t.Fatalf("records=%d LEO=%d error=%v", len(records), leo, err)
		}
		for j, r := range records {
			if r.Offset != int64(j) {
				t.Fatalf("offset %d at index %d", r.Offset, j)
			}
		}
	}
}
