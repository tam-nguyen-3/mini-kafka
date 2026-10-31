package faultproxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

func server(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewUnstartedServer(h)
	s.Listener = l
	s.Start()
	t.Cleanup(s.Close)
	return s
}
func fixture(t *testing.T) (*Proxy, *broker.Broker, *transport.HTTPBrokerClient, broker.BrokerInfo, broker.TopicPartition) {
	t.Helper()
	b := &broker.Broker{ID: 1}
	tp := broker.TopicPartition{Topic: "x"}
	b.AssignPartition(tp)
	upstream := server(t, transport.NewBrokerServer(b))
	p, err := New(upstream.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	s := server(t, p)
	bc, _ := transport.NewHTTPBrokerClient(100 * time.Millisecond)
	return p, b, bc, broker.BrokerInfo{ID: 1, Addr: s.URL}, tp
}
func wait(t *testing.T, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal("condition did not become true")
		case <-ticker.C:
		}
	}
}
func TestFaultScheduleAndCounters(t *testing.T) {
	p, b, bc, target, tp := fixture(t)
	op := Operation{"POST", "/produce"}
	ctx := context.Background()
	if err := p.SetRule(Rule{Operation: op, Mode: Disconnect}); err != nil {
		t.Fatal(err)
	}
	if _, err := bc.Produce(ctx, target, tp, "", "blocked"); err == nil {
		t.Fatal("disconnect succeeded")
	}
	if state, err := bc.State(ctx, target); err != nil || len(state) != 1 {
		t.Fatalf("nonmatching state request failed: %v", err)
	}
	snapshot := p.Snapshot()
	counts := snapshot[op]
	if counts.Requests != 1 || counts.Matched != 1 || counts.Forwarded != 0 || b.State()[0].LogEndOffset != 0 {
		t.Fatal(snapshot)
	}
	delete(snapshot, op)
	if p.Snapshot()[op] != counts {
		t.Fatal("snapshot aliases state")
	}
	p.Clear()
	if off, err := bc.Produce(ctx, target, tp, "", "normal"); err != nil || off != 0 {
		t.Fatalf("%d %v", off, err)
	}
	if err := p.SetRule(Rule{Operation: op, Mode: DropResponse}); err != nil {
		t.Fatal(err)
	}
	if _, err := bc.Produce(ctx, target, tp, "", "lost"); err == nil {
		t.Fatal("lost response succeeded")
	}
	if off, err := bc.Produce(ctx, target, tp, "", "after"); err != nil || off != 2 {
		t.Fatalf("one-shot rule persisted: %d %v", off, err)
	}
	counts = p.Snapshot()[op]
	if counts.Requests != 4 || counts.Forwarded != 3 || counts.Dropped != 1 || b.State()[0].LogEndOffset != 3 {
		t.Fatal(counts)
	}
	records, _ := b.Fetch(tp, 0, 10)
	if records[1].Value != "lost" {
		t.Fatal(records)
	}
}
func TestCanceledDelayNeverAppends(t *testing.T) {
	p, b, bc, target, tp := fixture(t)
	op := Operation{"POST", "/produce"}
	if err := p.SetRule(Rule{Operation: op, Mode: Delay, Delay: time.Second}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := bc.Produce(ctx, target, tp, "", "canceled"); done <- err }()
	wait(t, func() bool { return p.Snapshot()[op].Requests == 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation blocked")
	}
	wait(t, func() bool { return p.Snapshot()[op].Canceled == 1 })
	if p.Snapshot()[op].Forwarded != 0 || b.State()[0].LogEndOffset != 0 {
		t.Fatal("canceled delay appended")
	}
	// A client timeout must cancel the same delay even without explicit cancel.
	if _, err := bc.Produce(context.Background(), target, tp, "", "timed out"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	wait(t, func() bool { return p.Snapshot()[op].Canceled == 2 })
	p.Clear()
	if off, err := bc.Produce(context.Background(), target, tp, "", "ok"); err != nil || off != 0 {
		t.Fatalf("%d %v", off, err)
	}
}
func TestConcurrentControlsAndValidation(t *testing.T) {
	if _, err := New(":9091", time.Second); err == nil {
		t.Fatal("invalid target accepted")
	}
	if _, err := New("localhost:9091", 0); err == nil {
		t.Fatal("unbounded timeout accepted")
	}
	p, _, bc, target, _ := fixture(t)
	for _, rule := range []Rule{{Mode: "invalid"}, {Operation: Operation{"GET", "/fetch"}, Mode: Delay}, {Operation: Operation{"GET", "/fetch"}, Mode: DropResponse}} {
		if err := p.SetRule(rule); err == nil {
			t.Fatal(rule)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				p.SetRule(Rule{Operation: Operation{"GET", "/unused"}, Mode: Disconnect})
				p.Snapshot()
				p.Clear()
				if _, err := bc.State(context.Background(), target); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestDropResponseRequiresSuccessfulAppend(t *testing.T) {
	p, b, bc, target, _ := fixture(t)
	op := Operation{"POST", "/produce"}
	if err := p.SetRule(Rule{Operation: op, Mode: DropResponse}); err != nil {
		t.Fatal(err)
	}
	_, err := bc.Produce(context.Background(), target, broker.TopicPartition{Topic: "x", Partition: 9}, "", "")
	if !errors.Is(err, broker.ErrUnknownTopicOrPartition) {
		t.Fatal(err)
	}
	if p.Snapshot()[op].Dropped != 0 || b.State()[0].LogEndOffset != 0 {
		t.Fatal("rejection treated as an append")
	}
}
