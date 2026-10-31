package broker

import "testing"

func TestTopicPartitionEqualityAndMapLookup(t *testing.T) {
	tp := TopicPartition{Topic: "orders", Partition: 2}
	equal := TopicPartition{Topic: "orders", Partition: 2}
	if tp != equal {
		t.Fatal("equivalent partition identities are unequal")
	}
	owners := map[TopicPartition]BrokerID{tp: 1}
	if owner, ok := owners[equal]; !ok || owner != 1 {
		t.Fatalf("equivalent key lookup = (%d, %t), want (1, true)", owner, ok)
	}
	for _, other := range []TopicPartition{
		{Topic: "payments", Partition: 2},
		{Topic: "orders", Partition: 3},
	} {
		if tp == other {
			t.Fatalf("distinct identity %v equals %v", other, tp)
		}
		if _, ok := owners[other]; ok {
			t.Fatalf("distinct identity %v found in map", other)
		}
	}
}

func TestTopicPartitionString(t *testing.T) {
	for _, tc := range []struct {
		tp   TopicPartition
		want string
	}{
		{TopicPartition{Topic: "orders", Partition: 2}, "orders-2"},
		{TopicPartition{Topic: "orders", Partition: 0}, "orders-0"},
		{TopicPartition{Topic: "customer-orders", Partition: 12}, "customer-orders-12"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.tp.String(); got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}
