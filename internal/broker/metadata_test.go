package broker

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestClusterMetadataJSON(t *testing.T) {
	want := ClusterMetadata{
		Brokers: []BrokerInfo{
			{ID: 1, Addr: "localhost:9091"},
			{ID: 2, Addr: "localhost:9092"},
		},
		Partitions: []PartitionMetadata{
			{TopicPartition: TopicPartition{Topic: "orders", Partition: 0}, Leader: 1},
			{TopicPartition: TopicPartition{Topic: "orders", Partition: 1}, Leader: 2},
			{TopicPartition: TopicPartition{Topic: "payments", Partition: 0}, Leader: 2},
		},
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got ClusterMetadata
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON round-trip = %+v, want %+v", got, want)
	}

	// Check the wire shape separately: a symmetric tag mistake could otherwise
	// survive a round-trip. Embedded partition identity must stay flat.
	const wire = `{"brokers":[{"id":1,"addr":"localhost:9091"},{"id":2,"addr":"localhost:9092"}],"partitions":[{"topic":"orders","partition":0,"leader":1},{"topic":"orders","partition":1,"leader":2},{"topic":"payments","partition":0,"leader":2}]}`
	var actual, expected any
	if err := json.Unmarshal(data, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(wire), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("JSON = %s, want %s", data, wire)
	}
}
