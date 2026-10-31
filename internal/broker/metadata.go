package broker

// BrokerID identifies a broker independently of its network address.
type BrokerID int

// BrokerInfo describes how to identify and reach a broker.
type BrokerInfo struct {
	ID   BrokerID `json:"id"`
	Addr string   `json:"addr"`
}

// PartitionMetadata identifies a partition and its owning broker. In this
// iteration the leader is the sole owner; there are no replicas or failover.
type PartitionMetadata struct {
	TopicPartition
	Leader BrokerID `json:"leader"`
}

// ClusterMetadata describes brokers and partition ownership without holding
// record storage. Topics are represented by names in the partition list.
type ClusterMetadata struct {
	Brokers    []BrokerInfo        `json:"brokers"`
	Partitions []PartitionMetadata `json:"partitions"`
}
