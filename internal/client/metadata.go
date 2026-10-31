package client

import (
	"fmt"
	"strings"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
)

// metadataRoutes validates a complete snapshot before returning private,
// value-only routing tables. Address syntax belongs to the transport adapter.
func metadataRoutes(snapshot broker.ClusterMetadata) (map[string][]broker.BrokerInfo, error) {
	brokers := make(map[broker.BrokerID]broker.BrokerInfo, len(snapshot.Brokers))
	for _, b := range snapshot.Brokers {
		if _, exists := brokers[b.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate broker %d", ErrInvalidMetadata, b.ID)
		}
		if strings.TrimSpace(b.Addr) == "" {
			return nil, fmt.Errorf("%w: blank address for broker %d", ErrInvalidMetadata, b.ID)
		}
		brokers[b.ID] = b
	}
	partitions := make(map[string]map[int]broker.BrokerInfo)
	for _, p := range snapshot.Partitions {
		if strings.TrimSpace(p.Topic) == "" || p.Partition < 0 {
			return nil, fmt.Errorf("%w: invalid partition %q/%d", ErrInvalidMetadata, p.Topic, p.Partition)
		}
		target, ok := brokers[p.Leader]
		if !ok {
			return nil, fmt.Errorf("%w: missing leader %d for %s", ErrInvalidMetadata, p.Leader, p.TopicPartition)
		}
		if partitions[p.Topic] == nil {
			partitions[p.Topic] = make(map[int]broker.BrokerInfo)
		}
		if _, exists := partitions[p.Topic][p.Partition]; exists {
			return nil, fmt.Errorf("%w: duplicate partition %s", ErrInvalidMetadata, p.TopicPartition)
		}
		partitions[p.Topic][p.Partition] = target
	}
	routes := make(map[string][]broker.BrokerInfo, len(partitions))
	for topic, byID := range partitions {
		owners := make([]broker.BrokerInfo, len(byID))
		for i := range owners {
			target, ok := byID[i]
			if !ok {
				return nil, fmt.Errorf("%w: non-dense partition IDs for topic %q", ErrInvalidMetadata, topic)
			}
			owners[i] = target
		}
		routes[topic] = owners
	}
	return routes, nil
}
