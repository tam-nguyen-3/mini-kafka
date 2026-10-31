package broker

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
)

// newLogWith returns a log holding n records with values "v0".."v{n-1}".
func newLogWith(n int) *Log {
	l := NewLog()
	for i := range n {
		l.Append("", fmt.Sprintf("v%d", i))
	}
	return l
}

func TestAppendAssignsSequentialOffsetsFromZero(t *testing.T) {
	l := NewLog()
	if got := l.NextOffset(); got != 0 {
		t.Fatalf("empty log NextOffset = %d, want 0", got)
	}
	for want := int64(0); want < 5; want++ {
		rec := l.Append("k", "v")
		if rec.Offset != want {
			t.Fatalf("Append offset = %d, want %d", rec.Offset, want)
		}
		if got := l.NextOffset(); got != want+1 {
			t.Fatalf("NextOffset after append = %d, want %d", got, want+1)
		}
	}
}

func TestReadFromReturnsRecordsInOrder(t *testing.T) {
	l := newLogWith(5)
	recs, err := l.ReadFrom(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range recs {
		if r.Offset != int64(i) || r.Value != fmt.Sprintf("v%d", i) {
			t.Fatalf("record %d = %+v, want offset %d value v%d", i, r, i, i)
		}
	}
}

func TestReadFrom(t *testing.T) {
	tests := []struct {
		name      string
		offset    int64
		max       int
		wantFirst int64 // offset of first record, if any
		wantLen   int
		wantErr   error
	}{
		{"whole log", 0, 10, 0, 5, nil},
		{"from middle", 2, 10, 2, 3, nil},
		{"max limits count", 1, 2, 1, 2, nil},
		{"last record", 4, 10, 4, 1, nil},
		{"at LEO is empty", 5, 10, 0, 0, nil},
		{"largest limit from middle", 2, math.MaxInt, 2, 3, nil},
		{"largest limit at LEO", 5, math.MaxInt, 0, 0, nil},
		{"past LEO", 6, 10, 0, 0, ErrOffsetOutOfRange},
		{"negative offset", -1, 10, 0, 0, ErrOffsetOutOfRange},
		{"zero max", 0, 0, 0, 0, ErrInvalidMaxRecords},
		{"negative max", 0, -1, 0, 0, ErrInvalidMaxRecords},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := newLogWith(5)
			recs, err := l.ReadFrom(tc.offset, tc.max)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if len(recs) != tc.wantLen {
				t.Fatalf("len = %d, want %d", len(recs), tc.wantLen)
			}
			if tc.wantLen > 0 && recs[0].Offset != tc.wantFirst {
				t.Fatalf("first offset = %d, want %d", recs[0].Offset, tc.wantFirst)
			}
		})
	}
}

func TestReadFromReturnsCopy(t *testing.T) {
	l := NewLog()
	l.Append("k", "original")

	recs, err := l.ReadFrom(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	recs[0].Value = "hacked"

	again, _ := l.ReadFrom(0, 10)
	if again[0].Value != "original" {
		t.Fatalf("log was mutated through returned slice: got %q", again[0].Value)
	}
}

// Run with -race: without the mutex this both races and produces duplicate offsets.
func TestConcurrentAppendsGetUniqueDenseOffsets(t *testing.T) {
	l := NewLog()
	const n = 1000

	var wg sync.WaitGroup
	offsets := make(chan int64, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			offsets <- l.Append("", "v").Offset
		}()
	}
	wg.Wait()
	close(offsets)

	seen := make(map[int64]bool, n)
	for off := range offsets {
		if seen[off] {
			t.Fatalf("offset %d assigned twice", off)
		}
		seen[off] = true
	}
	for off := int64(0); off < n; off++ {
		if !seen[off] {
			t.Fatalf("offset %d missing: offsets are not dense", off)
		}
	}
	if got := l.NextOffset(); got != n {
		t.Fatalf("NextOffset = %d, want %d", got, n)
	}
}
