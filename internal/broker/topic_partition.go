package broker

import "fmt"

// TopicPartition identifies one partition of a named topic. It is comparable
// and can be used as a map key independently of broker-local record storage.
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// String returns Kafka's topic-partition display name, such as orders-2.
func (tp TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}
