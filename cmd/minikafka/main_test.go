package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

type safeBuffer struct {
	mu     sync.Mutex
	b      bytes.Buffer
	notify chan struct{}
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, e := b.b.Write(p)
	if b.notify != nil {
		select {
		case b.notify <- struct{}{}:
		default:
		}
	}
	return n, e
}
func (b *safeBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }
func waitFor(t *testing.T, b *safeBuffer, s string) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		if strings.Contains(b.String(), s) {
			return
		}
		select {
		case <-b.notify:
		case <-deadline.C:
			t.Fatalf("waiting for %q; got %s", s, b.String())
		}
	}
}
func invoke(t *testing.T, args []string, in string, want int) (string, string) {
	t.Helper()
	var out, diag bytes.Buffer
	code := run(context.Background(), args, strings.NewReader(in), &out, &diag)
	if code != want {
		t.Fatalf("%v: exit %d, want %d; stdout=%s stderr=%s", args, code, want, out.String(), diag.String())
	}
	return out.String(), diag.String()
}
func TestUsage(t *testing.T) {
	cases := []struct {
		args []string
		code int
	}{
		{nil, 0}, {[]string{"help"}, 0}, {[]string{"wat"}, 2}, {[]string{"topic"}, 2}, {[]string{"topic", "delete"}, 2},
		{[]string{"topic", "create", "x", "--partitions", "0"}, 2}, {[]string{"produce", "x", "--value"}, 2},
		{[]string{"produce-bulk", "x", "--keys", "0"}, 2}, {[]string{"consume", "x", "--partition", "-1"}, 2},
		{[]string{"consume", "x", "--from", "-1"}, 2}, {[]string{"consume", "x", "--poll-interval", "0s"}, 2},
		{[]string{"describe", "--timeout", "0s"}, 2}, {[]string{"dump"}, 2},
		{[]string{"broker", "--listen", ":9091", "--advertise", "localhost:9091"}, 2},
		{[]string{"broker", "--id", "0", "--listen", ":9091", "--advertise", ":9091"}, 2},
		{[]string{"describe", "extra"}, 2}, {[]string{"produce", "x", "--wat"}, 2},
	}
	for _, c := range cases {
		t.Run(fmt.Sprint(c.args), func(t *testing.T) { invoke(t, c.args, "", c.code) })
	}
	for _, cmd := range []string{"controller", "broker", "topic create", "produce", "produce-bulk", "consume", "describe", "dump"} {
		t.Run(cmd+" help", func(t *testing.T) {
			out, _ := invoke(t, append(strings.Fields(cmd), "--help"), "", 0)
			if !strings.Contains(out, "Usage:") {
				t.Fatal(out)
			}
		})
	}
}

type cluster struct {
	url     string
	cores   []*broker.Broker
	servers []*httptest.Server
	bc      *transport.HTTPBrokerClient
	mc      *transport.HTTPControllerClient
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	c := &cluster{}
	c.bc, _ = transport.NewHTTPBrokerClient(time.Second)
	core, _ := controller.New(c.bc)
	for i := 0; i < 3; i++ {
		b := &broker.Broker{ID: broker.BrokerID(i)}
		s := httptest.NewServer(transport.NewBrokerServer(b))
		t.Cleanup(s.Close)
		c.cores = append(c.cores, b)
		c.servers = append(c.servers, s)
		if err := core.RegisterBroker(b.ID, s.URL); err != nil {
			t.Fatal(err)
		}
	}
	s := httptest.NewServer(transport.NewControllerServer(core))
	t.Cleanup(s.Close)
	c.url = s.URL
	c.mc, _ = transport.NewHTTPControllerClient(s.URL, time.Second)
	if err := c.mc.CreateTopic(context.Background(), "orders", 6); err != nil {
		t.Fatal(err)
	}
	return c
}
func (c *cluster) args(args ...string) []string {
	return append(args, "--controller", c.url, "--timeout", "200ms")
}
func TestCommandsHTTP(t *testing.T) {
	c := newCluster(t)
	invoke(t, c.args("topic", "create", "other", "--partitions", "1"), "", 0)
	out, _ := invoke(t, c.args("produce", "orders", "--key", "u\t1", "--value", "hello\nworld"), "ignored", 0)
	if !strings.Contains(out, `"u\t1"`) || !strings.Contains(out, `"hello\nworld"`) {
		t.Fatal(out)
	}
	invoke(t, c.args("produce", "--key", "", "--value=", "orders"), "ignored", 0)
	invoke(t, c.args("produce", "orders"), "first\n\nlast", 0)
	bulk, _ := invoke(t, c.args("produce-bulk", "orders", "--count", "12", "--keys", "3"), "", 0)
	if strings.Count(bulk, "\n") != 13 || !strings.Contains(bulk, `"key-2"`) {
		t.Fatal(bulk)
	}
	consumed, _ := invoke(t, c.args("consume", "orders"), "", 0)
	if strings.Count(consumed, "\n") != 18 {
		t.Fatalf("expected 17 records: %s", consumed)
	}
	desc, _ := invoke(t, c.args("describe"), "", 0)
	if !strings.Contains(desc, "LEO") || strings.Contains(desc, "missing") {
		t.Fatal(desc)
	}
	dumped, _ := invoke(t, c.args("dump", "--broker", "0"), "", 0)
	if !strings.Contains(dumped, "LEO=") {
		t.Fatal(dumped)
	}
	invoke(t, c.args("consume", "orders", "--partition", "0", "--from", "9999"), "", 1)
	invoke(t, c.args("consume", "orders", "--partition", "6"), "", 1)
	invoke(t, c.args("dump", "--broker", "999"), "", 1)
	c.servers[0].Close()
	partial, diag := invoke(t, c.args("consume", "orders"), "", 1)
	if strings.Count(partial, "\n") < 2 || !strings.Contains(diag, "unavailable") {
		t.Fatalf("%s %s", partial, diag)
	}
	partial, _ = invoke(t, c.args("describe"), "", 1)
	if !strings.Contains(partial, "unavailable") {
		t.Fatal(partial)
	}
}
func TestStdinBoundariesAndCanceledRead(t *testing.T) {
	c := newCluster(t)
	invoke(t, c.args("produce", "orders"), strings.Repeat("x", 256*1024), 0)
	invoke(t, c.args("produce", "orders"), strings.Repeat("x", 256*1024+1), 1)
	invoke(t, c.args("produce", "orders"), "", 0)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- run(ctx, c.args("produce", "orders"), reader, io.Discard, io.Discard) }()
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatal(code)
		}
	case <-time.After(time.Second):
		t.Fatal("idle stdin blocked cancellation")
	}
}

type fetchFunc func(context.Context, broker.BrokerInfo, broker.TopicPartition, int64, int) (broker.FetchResult, error)

func (f fetchFunc) Fetch(ctx context.Context, b broker.BrokerInfo, tp broker.TopicPartition, o int64, n int) (broker.FetchResult, error) {
	return f(ctx, b, tp, o, n)
}
func TestSnapshotCapAndTruncation(t *testing.T) {
	tp := broker.TopicPartition{Topic: "x"}
	f := &snapshotFetcher{ends: map[broker.TopicPartition]int64{tp: 2}, owners: make(map[broker.TopicPartition]broker.BrokerID)}
	f.base = fetchFunc(func(_ context.Context, _ broker.BrokerInfo, _ broker.TopicPartition, offset int64, max int) (broker.FetchResult, error) {
		if offset != 1 || max != 1 {
			t.Fatalf("offset=%d max=%d", offset, max)
		}
		return broker.FetchResult{Records: []broker.Record{{Offset: 1}}, LogEndOffset: 50}, nil
	})
	r, e := f.Fetch(context.Background(), broker.BrokerInfo{ID: 9}, tp, 1, 100)
	if e != nil || r.LogEndOffset != 2 || len(r.Records) != 1 || f.owners[tp] != 9 {
		t.Fatalf("%+v %v", r, e)
	}
	f.base = fetchFunc(func(context.Context, broker.BrokerInfo, broker.TopicPartition, int64, int) (broker.FetchResult, error) {
		return broker.FetchResult{Records: []broker.Record{}, LogEndOffset: 1}, nil
	})
	_, e = f.Fetch(context.Background(), broker.BrokerInfo{}, tp, 1, 1)
	if !errors.Is(e, broker.ErrOffsetOutOfRange) {
		t.Fatal(e)
	}
}
func TestSnapshotExcludesConcurrentAppendsHTTP(t *testing.T) {
	c := newCluster(t)
	tp := broker.TopicPartition{Topic: "orders", Partition: 0}
	c.cores[0].Produce(tp, "", "before")
	var once sync.Once
	// Appending on the first fetch occurs after state capture and simulates a
	// producer that continues after the one-shot command has chosen endpoints.
	original := transport.NewBrokerServer(c.cores[0])
	c.servers[0].Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fetch" {
			once.Do(func() { c.cores[0].Produce(tp, "", "after") })
		}
		original.ServeHTTP(w, r)
	})
	out, _ := invoke(t, c.args("consume", "orders", "--partition", "0"), "", 0)
	if !strings.Contains(out, `"before"`) || strings.Contains(out, `"after"`) {
		t.Fatal(out)
	}
}
func TestFollowContinuesHealthyPartitions(t *testing.T) {
	c := newCluster(t)
	c.servers[0].Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &safeBuffer{notify: make(chan struct{}, 1)}
	diag := &safeBuffer{notify: make(chan struct{}, 1)}
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, c.args("consume", "orders", "--follow", "--poll-interval", "5ms"), strings.NewReader(""), out, diag)
	}()
	waitFor(t, diag, "poll orders-0")
	tp := broker.TopicPartition{Topic: "orders", Partition: 1}
	c.cores[1].Produce(tp, "k", "live")
	waitFor(t, out, `"live"`)
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit=%d %s", code, diag.String())
		}
	case <-time.After(time.Second):
		t.Fatal("follow did not stop")
	}
	if strings.Count(out.String(), `"live"`) != 1 {
		t.Fatal(out.String())
	}
	if strings.Count(diag.String(), "poll orders-0") != 1 {
		t.Fatalf("duplicate error: %s", diag.String())
	}
}
func TestLostProduceResponseIsNotRetried(t *testing.T) {
	c := newCluster(t)
	tp := broker.TopicPartition{Topic: "orders", Partition: 0}
	var calls atomic.Int32
	// Empty keys choose partition zero in each new producer.
	c.servers[0].Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/produce" {
			transport.NewBrokerServer(c.cores[0]).ServeHTTP(w, r)
			return
		}
		calls.Add(1)
		c.cores[0].Produce(tp, "", "stored")
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	})
	_, diag := invoke(t, c.args("produce", "orders", "--value", "stored"), "", 1)
	if calls.Load() != 1 || c.cores[0].State()[0].LogEndOffset != 1 || !strings.Contains(diag, "outcome may be unknown") {
		t.Fatalf("calls=%d %s", calls.Load(), diag)
	}
}

func TestServerLifecycle(t *testing.T) {
	// An already bound port fails without waiting for a signal.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	invoke(t, []string{"controller", "--listen", l.Addr().String()}, "", 1)
	ctx, cancel := context.WithCancel(context.Background())
	diag := &safeBuffer{notify: make(chan struct{}, 1)}
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"controller", "--listen", "127.0.0.1:0"}, strings.NewReader(""), io.Discard, diag)
	}()
	waitFor(t, diag, "controller ready")
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatal(diag.String())
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
	// Registration failure must close a broker's already-bound listener.
	slot, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := slot.Addr().String()
	slot.Close()
	invoke(t, []string{"broker", "--id", "0", "--listen", addr, "--advertise", addr, "--controller", l.Addr().String(), "--timeout", "20ms"}, "", 1)
	rebound, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener leaked: %v", err)
	}
	rebound.Close()
}
func TestBrokerRequestLogs(t *testing.T) {
	var logs bytes.Buffer
	b := &broker.Broker{ID: 7}
	tp := broker.TopicPartition{Topic: "x"}
	b.AssignPartition(tp)
	s := httptest.NewServer(transport.NewBrokerServerWithLogger(b, slog.New(slog.NewTextHandler(&logs, nil))))
	defer s.Close()
	bc, _ := transport.NewHTTPBrokerClient(time.Second)
	target := broker.BrokerInfo{ID: 7, Addr: s.URL}
	bc.Produce(context.Background(), target, tp, "secret-key", "secret-value")
	bc.Fetch(context.Background(), target, tp, 0, 1)
	text := logs.String()
	if !strings.Contains(text, "broker=7") || !strings.Contains(text, "operation=produce") || !strings.Contains(text, "operation=fetch") || strings.Contains(text, "secret") {
		t.Fatal(text)
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestOutputFailure(t *testing.T) {
	if code := run(context.Background(), []string{"--help"}, strings.NewReader(""), brokenWriter{}, io.Discard); code != 1 {
		t.Fatal(code)
	}
}

func TestCanceledAttemptStillReportsUnknownOutcome(t *testing.T) {
	c := newCluster(t)
	appended := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	c.servers[0].Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/produce" {
			transport.NewBrokerServer(c.cores[0]).ServeHTTP(w, r)
			return
		}
		c.cores[0].Produce(broker.TopicPartition{Topic: "orders"}, "", "stored")
		close(appended)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var diag bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, c.args("produce", "orders", "--value", "stored"), strings.NewReader(""), io.Discard, &diag)
	}()
	select {
	case <-appended:
	case <-time.After(time.Second):
		t.Fatal("produce did not reach broker")
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 || !strings.Contains(diag.String(), "outcome may be unknown") {
			t.Fatalf("exit=%d %s", code, diag.String())
		}
	case <-time.After(time.Second):
		t.Fatal("canceled produce did not finish")
	}
}

func TestSimulationClockAndTimestampOutput(t *testing.T) {
	clock := recordClock(time.Hour, -time.Second)
	first, second := clock(), clock()
	if first.Sub(second) != time.Second || time.Until(first) < 59*time.Minute {
		t.Fatalf("%v %v", first, second)
	}
	live := recordClock(-time.Hour, 0)()
	if delta := time.Since(live); delta < 59*time.Minute || delta > 61*time.Minute {
		t.Fatal(live)
	}
	var wg sync.WaitGroup
	seen := sync.Map{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, loaded := seen.LoadOrStore(clock().UnixMilli(), true); loaded {
				t.Error("duplicate stepped timestamp")
			}
		}()
	}
	wg.Wait()
	c := newCluster(t)
	c.cores[0].Produce(broker.TopicPartition{Topic: "orders"}, "key", "value")
	for _, args := range [][]string{{"consume", "orders", "--partition", "0", "--timestamps"}, {"dump", "--broker", "0", "--timestamps"}} {
		out, _ := invoke(t, c.args(args...), "", 0)
		if !strings.Contains(out, "TIMESTAMP_MS") {
			t.Fatal(out)
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, `"orders"`) {
				fields := strings.Split(line, "\t")
				if len(fields) != 7 || fields[6] == "0" {
					t.Fatal(line)
				}
			}
		}
	}
	invoke(t, []string{"broker", "--clock-step", "bad"}, "", 2)
}
