package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/client"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/protocol"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

var (
	_ client.MetadataClient = (*transport.HTTPControllerClient)(nil)
	_ client.BrokerClient   = (*transport.HTTPBrokerClient)(nil)
)

func TestProducerHTTPCluster(t *testing.T) {
	ctx := context.Background()
	bc, err := transport.NewHTTPBrokerClient(0)
	if err != nil {
		t.Fatal(err)
	}
	c, err := controller.New(bc)
	if err != nil {
		t.Fatal(err)
	}
	cs := newHTTPServer4(t, transport.NewControllerServer(c))
	defer cs.Close()
	cc, err := transport.NewHTTPControllerClient(cs.URL, 0)
	if err != nil {
		t.Fatal(err)
	}
	targets := make([]broker.BrokerInfo, 3)
	for i := range targets {
		b := &broker.Broker{ID: broker.BrokerID(i)}
		s := newHTTPServer4(t, transport.NewBrokerServer(b))
		defer s.Close()
		targets[i] = broker.BrokerInfo{ID: b.ID, Addr: s.URL}
		if err := cc.RegisterBroker(ctx, b.ID, s.URL); err != nil {
			t.Fatal(err)
		}
	}
	if err := cc.CreateTopic(ctx, "orders", 6); err != nil {
		t.Fatal(err)
	}
	p, err := client.NewProducer(cc, bc)
	if err != nil {
		t.Fatal(err)
	}
	type payload struct{ Key, Value string }
	expected := make(map[int][]payload)
	keyedPartitions := make(map[string]int)
	send := func(key, value string, wantPartition int) {
		t.Helper()
		part, off, err := p.Send(ctx, "orders", key, value)
		if err != nil {
			t.Fatal(err)
		}
		if wantPartition >= 0 && part != wantPartition {
			t.Fatalf("partition %d want %d", part, wantPartition)
		}
		if off != int64(len(expected[part])) {
			t.Fatalf("offset %d for partition %d; want %d", off, part, len(expected[part]))
		}
		expected[part] = append(expected[part], payload{key, value})
		if key != "" {
			if prev, ok := keyedPartitions[key]; ok && prev != part {
				t.Fatalf("key %q remapped", key)
			}
			keyedPartitions[key] = part
		}
	}
	for i := 0; i < 30; i++ {
		send(fmt.Sprintf("user-%d", i%7), fmt.Sprintf("keyed-%d", i), -1)
	}
	for i := 0; i < 18; i++ {
		send("", fmt.Sprintf("unkeyed-%d", i), i%6)
	}
	total := 0
	for i, target := range targets {
		state, err := bc.State(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		if len(state) != 2 {
			t.Fatalf("broker %d has %d partitions", i, len(state))
		}
		for _, local := range state {
			if local.Topic != "orders" || local.Partition%3 != i {
				t.Fatalf("wrong owner %+v", local)
			}
			want := expected[local.Partition]
			result, err := bc.Fetch(ctx, target, local.TopicPartition, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if result.LogEndOffset != int64(len(want)) || local.LogEndOffset != result.LogEndOffset {
				t.Fatalf("LEO: %+v want %d", local, len(want))
			}
			var got []payload
			for j, r := range result.Records {
				if r.Offset != int64(j) {
					t.Fatalf("out of order %+v", r)
				}
				got = append(got, payload{r.Key, r.Value})
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("partition %d got %v want %v", local.Partition, got, want)
			}
			total += len(result.Records)
		}
	}
	_, rejectErr := bc.Fetch(ctx, targets[0], broker.TopicPartition{Topic: "orders", Partition: 1}, 0, 1)
	if !errors.Is(rejectErr, broker.ErrUnknownTopicOrPartition) {
		t.Fatalf("unowned partition accepted: %v", rejectErr)
	}
	if total != 48 {
		t.Fatalf("accounted for %d records, want 48", total)
	}
}

func TestProducerHTTPAmbiguousAppend(t *testing.T) {
	ctx := context.Background()
	bc, err := transport.NewHTTPBrokerClient(0)
	if err != nil {
		t.Fatal(err)
	}
	b := &broker.Broker{}
	var attempts atomic.Int32
	handler := transport.NewBrokerServer(b)
	bs := newHTTPServer4(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/produce" {
			handler.ServeHTTP(w, r)
			return
		}
		attempts.Add(1)
		var req protocol.ProduceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if _, err := b.Produce(req.TopicPartition, req.Key, req.Value); err != nil {
			t.Error(err)
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer bs.Close()
	c, err := controller.New(bc)
	if err != nil {
		t.Fatal(err)
	}
	cs := newHTTPServer4(t, transport.NewControllerServer(c))
	defer cs.Close()
	cc, err := transport.NewHTTPControllerClient(cs.URL, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := cc.RegisterBroker(ctx, 0, bs.URL); err != nil {
		t.Fatal(err)
	}
	if err := cc.CreateTopic(ctx, "orders", 1); err != nil {
		t.Fatal(err)
	}
	p, err := client.NewProducer(cc, bc)
	if err != nil {
		t.Fatal(err)
	}
	part, off, err := p.Send(ctx, "orders", "k", "may have appended")
	if !errors.Is(err, transport.ErrUnavailable) || part != 0 || off != -1 {
		t.Fatalf("ambiguous append=%d,%d,%v", part, off, err)
	}
	result, err := b.FetchWithLEO(broker.TopicPartition{Topic: "orders"}, 0, 10)
	if err != nil || len(result.Records) != 1 || attempts.Load() != 1 {
		t.Fatalf("attempts=%d result=%+v err=%v", attempts.Load(), result, err)
	}
}

func newHTTPServer4(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	return server
}
