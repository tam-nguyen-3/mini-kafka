package client

import (
	"hash/fnv"
	"sync"
)

// Partitioner selects an ID in [0, numPartitions), or -1 for a nonpositive count.
// Stable keyed routing requires a fixed partition count.
type Partitioner interface {
	Partition(key string, numPartitions int) int
}

// DefaultPartitioner uses FNV-1a (32-bit) for keys and round-robin for empty keys.
// Its zero value is ready for concurrent use; do not copy it after first use.
// A Producer keeps one instance per topic so empty-key sequences are independent.
// This simplifies Kafka's sticky partitioning and is not Kafka wire compatible.
type DefaultPartitioner struct {
	mu   sync.Mutex
	next int
}

var _ Partitioner = (*DefaultPartitioner)(nil)

// Partition returns -1 for an invalid count without advancing the sequence.
// Keyed calls do not advance the empty-key sequence. Changing the count can
// remap keys and break per-key ordering; Producer rejects such metadata changes.
func (p *DefaultPartitioner) Partition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return -1
	}
	if key != "" {
		h := fnv.New32a()
		_, _ = h.Write([]byte(key))
		return int(uint64(h.Sum32()) % uint64(numPartitions))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	partition := p.next % numPartitions
	p.next = (partition + 1) % numPartitions
	return partition
}
