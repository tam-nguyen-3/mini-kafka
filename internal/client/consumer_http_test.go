package client_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/client"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

var _ client.FetchClient = (*transport.HTTPBrokerClient)(nil)

func TestConsumerHTTPClusterAndBrokerFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bc, err := transport.NewHTTPBrokerClient(0)
	if err != nil {
		t.Fatal(err)
	}
	core, err := controller.New(bc)
	if err != nil {
		t.Fatal(err)
	}
	cs := newHTTPServer4(t, transport.NewControllerServer(core))
	defer cs.Close()
	cc, err := transport.NewHTTPControllerClient(cs.URL, 0)
	if err != nil {
		t.Fatal(err)
	}
	servers := make([]*httptest.Server, 3)
	for i := range servers {
		b := &broker.Broker{ID: broker.BrokerID(i)}
		servers[i] = newHTTPServer4(t, transport.NewBrokerServer(b))
		defer servers[i].Close()
		if err := cc.RegisterBroker(ctx, b.ID, servers[i].URL); err != nil {
			t.Fatal(err)
		}
	}
	if err := cc.CreateTopic(ctx, "orders", 6); err != nil {
		t.Fatal(err)
	}
	producer, err := client.NewProducer(cc, bc)
	if err != nil {
		t.Fatal(err)
	}
	type payload struct{ key, value string }
	expected := make(map[broker.TopicPartition][]payload)
	produce := func(key, value string) int {
		t.Helper()
		partition, offset, err := producer.Send(ctx, "orders", key, value)
		if err != nil {
			t.Fatal(err)
		}
		tp := broker.TopicPartition{Topic: "orders", Partition: partition}
		if offset != int64(len(expected[tp])) {
			t.Fatalf("offset=%d want %d for %s", offset, len(expected[tp]), tp)
		}
		expected[tp] = append(expected[tp], payload{key, value})
		return partition
	}
	for i := 0; i < 24; i++ {
		produce(fmt.Sprintf("user-%d", i%7), fmt.Sprintf("keyed-%d", i))
	}
	for i := 0; i < 18; i++ {
		produce("", fmt.Sprintf("empty-%d", i))
	}
	fetcher, err := transport.NewHTTPBrokerClient(200 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := client.NewConsumer(cc, fetcher)
	if err != nil {
		t.Fatal(err)
	}
	tps := make([]broker.TopicPartition, 6)
	for i := range tps {
		tps[i] = broker.TopicPartition{Topic: "orders", Partition: i}
	}
	if err := consumer.Assign(ctx, tps...); err != nil {
		t.Fatal(err)
	}
	next := make(map[broker.TopicPartition]int64)
	checkRecord := func(record client.ConsumerRecord) {
		t.Helper()
		tp := record.TopicPartition
		if record.Offset != next[tp] {
			t.Fatalf("skipped/duplicated %s offset=%d want %d", tp, record.Offset, next[tp])
		}
		if record.Offset >= int64(len(expected[tp])) {
			t.Fatalf("unexpected record %+v", record)
		}
		want := expected[tp][record.Offset]
		if record.Key != want.key || record.Value != want.value {
			t.Fatalf("wrong payload: %+v want %+v", record, want)
		}
		next[tp]++
	}
	consumed := 0
	for i := 0; i < 30; i++ {
		result, err := consumer.Poll(ctx, 5)
		if err != nil || len(result.PartitionErrors) != 0 {
			t.Fatalf("poll=%+v %v", result, err)
		}
		if len(result.Records) > 5 {
			t.Fatal("poll exceeded total limit")
		}
		for _, r := range result.Records {
			checkRecord(r)
			consumed++
		}
		if len(result.Records) == 0 {
			break
		}
	}
	if consumed != 42 {
		t.Fatalf("consumed %d initial records, want 42", consumed)
	}
	for tp, records := range expected {
		if next[tp] != int64(len(records)) {
			t.Fatalf("missing records in %s", tp)
		}
	}

	// Metadata remains cached. Losing one broker and the controller must not
	// prevent independent partitions from serving the producer and consumer.
	servers[1].Close()
	cs.Close()
	for i := 0; i < 6; i++ {
		value := fmt.Sprintf("after-failure-%d", i)
		partition, offset, err := producer.Send(ctx, "orders", "", value)
		if partition != i {
			t.Fatalf("empty-key partition=%d want %d", partition, i)
		}
		if i%3 == 1 {
			if !errors.Is(err, transport.ErrUnavailable) || offset != -1 {
				t.Fatalf("dead broker send=%d,%d,%v", partition, offset, err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			tp := tps[i]
			if offset != int64(len(expected[tp])) {
				t.Fatalf("healthy offset=%d want %d", offset, len(expected[tp]))
			}
			expected[tp] = append(expected[tp], payload{"", value})
		}
	}
	started := time.Now()
	result, err := consumer.Poll(ctx, 100)
	if err != nil || len(result.Records) != 4 || len(result.PartitionErrors) != 2 {
		t.Fatalf("partial broker failure=%+v %v", result, err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("broker failure was not bounded")
	}
	for _, record := range result.Records {
		checkRecord(record)
		if record.Partition%3 == 1 {
			t.Fatal("dead broker yielded records")
		}
	}
	seen := make(map[int]bool)
	for _, failure := range result.PartitionErrors {
		if !errors.Is(failure, transport.ErrUnavailable) || failure.Partition%3 != 1 || seen[failure.Partition] {
			t.Fatalf("unexpected partition error: %v", failure)
		}
		seen[failure.Partition] = true
	}
	// Healthy records must not be redelivered, and failed partitions remain
	// assigned so subsequent calls keep reporting their availability errors.
	result, err = consumer.Poll(ctx, 100)
	if err != nil || len(result.Records) != 0 || len(result.PartitionErrors) != 2 {
		t.Fatalf("subsequent poll=%+v %v", result, err)
	}
}
