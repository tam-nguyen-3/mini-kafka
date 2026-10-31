package client

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestDefaultPartitioner(t *testing.T) {
	var p DefaultPartitioner
	// Standard FNV-1a vectors, modulo counts large enough to preserve the hash.
	for _, tc := range []struct {
		key  string
		hash uint32
	}{{"a", 0xe40c292c}, {"foobar", 0xbf9cf968}, {"hello", 0x4f9f2cab}} {
		for _, n := range []int{1, 3, 6, 101, math.MaxInt} {
			want := int(uint64(tc.hash) % uint64(n))
			if got := p.Partition(tc.key, n); got != want {
				t.Fatalf("Partition(%q,%d)=%d want %d", tc.key, n, got, want)
			}
		}
	}
	for _, n := range []int{-1, 0} {
		if got := p.Partition("", n); got != -1 {
			t.Fatalf("invalid count got %d", got)
		}
	}
	for i := 0; i < 12; i++ {
		if got := p.Partition("", 3); got != i%3 {
			t.Fatalf("round-robin %d got %d", i, got)
		}
		p.Partition("key does not advance round-robin", 3)
	}
	seen := map[int]bool{}
	for i := 0; i < 100; i++ {
		seen[p.Partition(fmt.Sprint(i), 6)] = true
	}
	if len(seen) != 6 {
		t.Fatalf("keys cover %d partitions", len(seen))
	}
}

func TestPartitionerConcurrent(t *testing.T) {
	var p DefaultPartitioner
	var wg sync.WaitGroup
	counts := make([]int, 4)
	var mu sync.Mutex
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); id := p.Partition("", 4); mu.Lock(); counts[id]++; mu.Unlock() }()
	}
	wg.Wait()
	for i, n := range counts {
		if n != 25 {
			t.Fatalf("partition %d got %d turns", i, n)
		}
	}
}
