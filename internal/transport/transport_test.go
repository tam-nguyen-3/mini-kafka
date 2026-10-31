package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/protocol"
)

func brokerClient(t *testing.T, timeout time.Duration) *HTTPBrokerClient {
	t.Helper()
	c, err := NewHTTPBrokerClient(timeout)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func controllerClient(t *testing.T, addr string) *HTTPControllerClient {
	t.Helper()
	c, err := NewHTTPControllerClient(addr, 0)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func requireError(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("got %v; want errors.Is(_, %v)", err, want)
	}
}

func TestClusterHTTPRoundTrip(t *testing.T) {
	ctx := context.Background()
	bc := brokerClient(t, 0)
	core, err := controller.New(bc)
	if err != nil {
		t.Fatal(err)
	}
	cs := httptest.NewServer(NewControllerServer(core))
	defer cs.Close()
	cc := controllerClient(t, cs.URL)
	initial, err := cc.Metadata(ctx)
	if err != nil || initial.Brokers == nil || initial.Partitions == nil || len(initial.Partitions) != 0 {
		t.Fatalf("initial metadata=%+v %v", initial, err)
	}
	var targets []broker.BrokerInfo
	for i := 0; i < 3; i++ {
		b := &broker.Broker{ID: broker.BrokerID(i)}
		s := httptest.NewServer(NewBrokerServer(b))
		defer s.Close()
		target := broker.BrokerInfo{ID: b.ID, Addr: s.URL}
		targets = append(targets, target)
		state, err := bc.State(ctx, target)
		if err != nil || state == nil || len(state) != 0 {
			t.Fatalf("empty state=%v %v", state, err)
		}
		// Equivalent shorthand and URL addresses must be idempotent.
		if err := cc.RegisterBroker(ctx, b.ID, strings.TrimPrefix(s.URL, "http://")); err != nil {
			t.Fatal(err)
		}
		if err := cc.RegisterBroker(ctx, b.ID, s.URL+"/"); err != nil {
			t.Fatal(err)
		}
	}
	if err := cc.CreateTopic(ctx, "orders & events", 6); err != nil {
		t.Fatal(err)
	}
	metadata, err := cc.Metadata(ctx)
	if err != nil || len(metadata.Brokers) != 3 || len(metadata.Partitions) != 6 {
		t.Fatalf("metadata=%+v %v", metadata, err)
	}
	for _, p := range metadata.Partitions {
		target := targets[int(p.Leader)]
		if p.Leader != broker.BrokerID(p.Partition%3) {
			t.Fatalf("placement=%+v", p)
		}
		for i := 0; i < 4; i++ {
			off, err := bc.Produce(ctx, target, p.TopicPartition, "key", fmt.Sprint(i))
			if err != nil || off != int64(i) {
				t.Fatalf("produce=%d %v", off, err)
			}
		}
		if err := bc.AssignPartition(ctx, target, p.TopicPartition); err != nil {
			t.Fatal(err)
		}
		result, err := bc.Fetch(ctx, target, p.TopicPartition, 0, math.MaxInt)
		if err != nil || len(result.Records) != 4 || result.LogEndOffset != 4 {
			t.Fatalf("fetch=%+v %v", result, err)
		}
		for i, r := range result.Records {
			if r.Offset != int64(i) || r.Value != fmt.Sprint(i) {
				t.Fatalf("record=%+v", r)
			}
		}
		empty, err := bc.Fetch(ctx, target, p.TopicPartition, 4, 1)
		if err != nil || empty.Records == nil || len(empty.Records) != 0 {
			t.Fatalf("empty fetch=%+v %v", empty, err)
		}
		wrong := targets[(int(p.Leader)+1)%3]
		_, err = bc.Produce(ctx, wrong, p.TopicPartition, "", "")
		requireError(t, err, broker.ErrUnknownTopicOrPartition)
		_, err = bc.Fetch(ctx, wrong, p.TopicPartition, 0, 1)
		requireError(t, err, broker.ErrUnknownTopicOrPartition)
	}
	for i, target := range targets {
		state, err := bc.State(ctx, target)
		if err != nil || len(state) != 2 {
			t.Fatalf("state=%+v %v", state, err)
		}
		for _, p := range state {
			if p.Partition%3 != i || p.LogEndOffset != 4 {
				t.Fatalf("ownership state=%+v", p)
			}
		}
	}
}

func TestHTTPDomainErrors(t *testing.T) {
	ctx := context.Background()
	bc := brokerClient(t, 0)
	b := &broker.Broker{}
	tp := broker.TopicPartition{Topic: "t"}
	b.AssignPartition(tp)
	bs := httptest.NewServer(NewBrokerServer(b))
	defer bs.Close()
	target := broker.BrokerInfo{Addr: bs.URL}
	c, err := controller.New(bc)
	if err != nil {
		t.Fatal(err)
	}
	cs := httptest.NewServer(NewControllerServer(c))
	defer cs.Close()
	cc := controllerClient(t, cs.URL)
	tests := []struct {
		name   string
		call   func() error
		want   error
		status int
	}{
		{"offset", func() error { _, e := bc.Fetch(ctx, target, tp, 1, 1); return e }, broker.ErrOffsetOutOfRange, 400},
		{"negative offset", func() error { _, e := bc.Fetch(ctx, target, tp, -1, 1); return e }, broker.ErrOffsetOutOfRange, 400},
		{"limit", func() error { _, e := bc.Fetch(ctx, target, tp, 0, 0); return e }, broker.ErrInvalidMaxRecords, 400},
		{"unowned", func() error {
			_, e := bc.Produce(ctx, target, broker.TopicPartition{Topic: "missing"}, "", "")
			return e
		}, broker.ErrUnknownTopicOrPartition, 404},
		{"blank topic", func() error { return cc.CreateTopic(ctx, " ", 1) }, controller.ErrInvalidTopicName, 400},
		{"count", func() error { return cc.CreateTopic(ctx, "x", 0) }, controller.ErrInvalidPartitionCount, 400},
		{"no brokers", func() error { return cc.CreateTopic(ctx, "x", 1) }, controller.ErrNoBrokers, 503},
		{"bad address", func() error { return cc.RegisterBroker(ctx, 0, ":9091") }, controller.ErrInvalidBrokerAddress, 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			requireError(t, err, tc.want)
			var remote *RemoteError
			if !errors.As(err, &remote) || remote.StatusCode != tc.status {
				t.Fatalf("remote=%+v, err=%v", remote, err)
			}
		})
	}
	if err := cc.RegisterBroker(ctx, 0, bs.URL); err != nil {
		t.Fatal(err)
	}
	requireError(t, cc.RegisterBroker(ctx, 0, "localhost:1"), controller.ErrBrokerConflict)
	if err := cc.CreateTopic(ctx, "x", 1); err != nil {
		t.Fatal(err)
	}
	requireError(t, cc.CreateTopic(ctx, "x", 1), controller.ErrTopicAlreadyExists)
}

func TestAllErrorMappingsRoundTrip(t *testing.T) {
	for _, m := range errorMappings {
		t.Run(string(m.code), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeError(w, fmt.Errorf("assignment identity: %w", m.err))
			}))
			defer s.Close()
			_, err := controllerClient(t, s.URL).Metadata(context.Background())
			requireError(t, err, m.err)
			var remote *RemoteError
			if !errors.As(err, &remote) || remote.StatusCode != m.status || remote.Code != m.code || !strings.Contains(remote.Message, "assignment identity") {
				t.Fatalf("%+v %v", remote, err)
			}
		})
	}
}

func TestInvalidRequests(t *testing.T) {
	b := &broker.Broker{}
	c, err := controller.New(brokerClient(t, 0))
	if err != nil {
		t.Fatal(err)
	}
	bh, ch := NewBrokerServer(b), NewControllerServer(c)
	tests := []struct {
		name                      string
		h                         http.Handler
		method, path, body, ctype string
		status                    int
		code                      protocol.ErrorCode
	}{
		{"missing partition", bh, "POST", "/partitions", `{"topic":"t"}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"null partition", bh, "POST", "/partitions", `{"topic":"t","partition":null}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"wrong type", bh, "POST", "/partitions", `{"topic":"t","partition":"0"}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"overflow", bh, "POST", "/partitions", `{"topic":"t","partition":999999999999999999999999}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"negative partition", bh, "POST", "/partitions", `{"topic":"t","partition":-1}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"blank topic", bh, "POST", "/partitions", `{"topic":" ","partition":0}`, "application/json", 400, protocol.CodeInvalidTopicName},
		{"unknown field", bh, "POST", "/partitions", `{"topic":"t","partition":0,"extra":1}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"trailing object", bh, "POST", "/partitions", `{"topic":"t","partition":0} {}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"array", bh, "POST", "/partitions", `[]`, "application/json", 400, protocol.CodeInvalidRequest},
		{"null", bh, "POST", "/partitions", `null`, "application/json", 400, protocol.CodeInvalidRequest},
		{"malformed", bh, "POST", "/partitions", `{`, "application/json", 400, protocol.CodeInvalidRequest},
		{"missing type", bh, "POST", "/partitions", `{}`, "", 415, protocol.CodeUnsupportedMediaType},
		{"wrong type header", bh, "POST", "/partitions", `{}`, "text/plain", 415, protocol.CodeUnsupportedMediaType},
		{"oversized", bh, "POST", "/partitions", strings.Repeat(" ", MaxRequestBodyBytes+1), "application/json", 413, protocol.CodeBodyTooLarge},
		{"missing id", ch, "POST", "/brokers", `{"addr":"localhost:1"}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"missing count", ch, "POST", "/topics", `{"name":"x"}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"missing value", bh, "POST", "/produce", `{"topic":"t","partition":0,"key":""}`, "application/json", 400, protocol.CodeInvalidRequest},
		{"missing offset", bh, "GET", "/fetch?topic=t&partition=0&max=1", "", "", 400, protocol.CodeInvalidRequest},
		{"overflow query", bh, "GET", "/fetch?topic=t&partition=99999999999999999999999&offset=0&max=1", "", "", 400, protocol.CodeInvalidRequest},
		{"overflow offset", bh, "GET", "/fetch?topic=t&partition=0&offset=99999999999999999999999&max=1", "", "", 400, protocol.CodeInvalidRequest},
		{"overflow max", bh, "GET", "/fetch?topic=t&partition=0&offset=0&max=99999999999999999999999", "", "", 400, protocol.CodeInvalidRequest},
		{"duplicate query", bh, "GET", "/fetch?topic=t&partition=0&offset=0&max=1&max=2", "", "", 400, protocol.CodeInvalidRequest},
		{"bad escape", bh, "GET", "/fetch?topic=%zz&partition=0&offset=0&max=1", "", "", 400, protocol.CodeInvalidRequest},
		{"method", bh, "POST", "/state", "", "", 405, protocol.CodeMethodNotAllowed},
		{"path", bh, "GET", "/missing", "", "", 404, protocol.CodeNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.ctype)
			w := httptest.NewRecorder()
			tc.h.ServeHTTP(w, req)
			var out protocol.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.status || out.Code != tc.code {
				t.Fatalf("got %d %+v; want %d %s", w.Code, out, tc.status, tc.code)
			}
			if tc.status == 405 && w.Header().Get("Allow") != "GET" {
				t.Fatal("missing Allow")
			}
		})
	}
	if len(b.State()) != 0 {
		t.Fatal("invalid assignments changed broker")
	}
}

func TestNormalizeAddress(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"localhost:9091", "http://localhost:9091"}, {"http://LOCALHOST:09091/", "http://localhost:9091"},
		{"https://example.com:443", "https://example.com:443"}, {"[::1]:9091", "http://[::1]:9091"},
	} {
		out, err := NormalizeAddress(tc.in)
		if err != nil || out != tc.want {
			t.Errorf("%q = %q, %v", tc.in, out, err)
		}
	}
	for _, addr := range []string{"", " ", " localhost:1", "localhost:1 ", ":9091", "0.0.0.0:9091", "[::]:9091", "*:9091", "localhost", "localhost:0", "localhost:65536", "localhost:-1", "localhost:abc", "ftp://localhost:1", "http://user:pass@localhost:1", "localhost:1/a", "localhost:1?x=1", "localhost:1?", "localhost:1#f"} {
		_, err := NormalizeAddress(addr)
		if !errors.Is(err, controller.ErrInvalidBrokerAddress) {
			t.Errorf("accepted %q: %v", addr, err)
		}
	}
	if _, err := NewHTTPBrokerClient(-1); err == nil {
		t.Fatal("negative timeout accepted")
	}
	if _, err := NewHTTPControllerClient("localhost:1", -1); err == nil {
		t.Fatal("negative timeout accepted")
	}
	if _, err := NewHTTPControllerClient(":1", 0); err == nil {
		t.Fatal("invalid controller accepted")
	}
	if c := brokerClient(t, 0); c.client.Timeout != DefaultTimeout {
		t.Fatal("no default timeout")
	}
}

func TestClientFailures(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, body, ctype string
		status            int
		remote            bool
	}{
		{"malformed", `{`, "application/json", 200, false},
		{"trailing", `{} {}`, "application/json", 200, false},
		{"missing fields", `{}`, "application/json", 200, false},
		{"null", `null`, "application/json", 200, false},
		{"null lists", `{"brokers":null,"partitions":[]}`, "application/json", 200, false},
		{"wrong types", `{"brokers":1,"partitions":[]}`, "application/json", 200, false},
		{"wrong content type", `{}`, "text/plain", 200, false},
		{"malformed error", `{"code":"unavailable"}`, "application/json", 503, false},
		{"unknown code", `{"code":"future_error","message":"future details"}`, "application/json", 502, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.ctype)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer s.Close()
			_, err := controllerClient(t, s.URL).Metadata(ctx)
			if err == nil {
				t.Fatal("expected failure")
			}
			var remote *RemoteError
			if errors.As(err, &remote) != tc.remote {
				t.Fatalf("unexpected error %T: %v", err, err)
			}
			if tc.remote && (remote.Code != "future_error" || remote.StatusCode != 502 || errors.Unwrap(remote) != nil) {
				t.Fatalf("remote=%+v", remote)
			}
		})
	}
	var redirected atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		writeJSON(w, 200, broker.ClusterMetadata{})
	}))
	defer dest.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, dest.URL, 307) }))
	defer source.Close()
	if _, err := controllerClient(t, source.URL).Metadata(ctx); err == nil {
		t.Fatal("accepted redirect")
	}
	if redirected.Load() != 0 {
		t.Fatal("followed redirect")
	}
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	dead.Close()
	_, err := controllerClient(t, dead.URL).Metadata(ctx)
	requireError(t, err, ErrUnavailable)
}

func TestCancellationAndTimeout(t *testing.T) {
	// Read the POST body before waiting so the server observes the peer closing.
	entered := make(chan struct{}, 3)
	canceled := make(chan struct{}, 3)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		<-r.Context().Done()
		canceled <- struct{}{}
	}))
	defer s.Close()
	target := broker.BrokerInfo{Addr: s.URL}
	tp := broker.TopicPartition{Topic: "t"}
	c := brokerClient(t, 50*time.Millisecond)
	start := time.Now()
	err := c.AssignPartition(context.Background(), target, tp)
	requireError(t, err, context.DeadlineExceeded)
	if time.Since(start) > 2*time.Second {
		t.Fatal("client timeout was not bounded")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe timeout cancellation")
	}
	<-entered
	c = brokerClient(t, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.AssignPartition(ctx, target, tp) }()
	<-entered
	cancel()
	requireError(t, <-done, context.Canceled)
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe explicit cancellation")
	}
	// A shorter caller deadline wins over the client's five-second bound.
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = c.AssignPartition(ctx, target, tp)
	requireError(t, err, context.DeadlineExceeded)
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe caller deadline")
	}
	pre, cancelPre := context.WithCancel(context.Background())
	cancelPre()
	requireError(t, c.AssignPartition(pre, target, tp), context.Canceled)
}

func TestDroppedProduceResponseDoesNotRetry(t *testing.T) {
	var calls atomic.Int32
	b := &broker.Broker{}
	tp := broker.TopicPartition{Topic: "t"}
	b.AssignPartition(tp)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
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
	defer s.Close()
	_, err := brokerClient(t, 0).Produce(context.Background(), broker.BrokerInfo{Addr: s.URL}, tp, "k", "v")
	requireError(t, err, ErrUnavailable)
	if calls.Load() != 1 || b.State()[0].LogEndOffset != 1 {
		t.Fatalf("calls=%d state=%+v", calls.Load(), b.State())
	}
}

func TestPartialCreateRetryOverHTTP(t *testing.T) {
	ctx := context.Background()
	bc := brokerClient(t, 0)
	b0, b1 := &broker.Broker{ID: 0}, &broker.Broker{ID: 1}
	s0 := httptest.NewServer(NewBrokerServer(b0))
	defer s0.Close()
	var loseAck atomic.Bool
	loseAck.Store(true)
	handler := NewBrokerServer(b1)
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/partitions" && loseAck.Swap(false) {
			// Assignment happened, but its acknowledgment is lost. The controller must
			// keep metadata unpublished and replay its original plan on explicit retry.
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			if recorder.Code != 200 {
				t.Errorf("assignment: %s", recorder.Body.String())
			}
			writeError(w, ErrUnavailable)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer s1.Close()
	c, err := controller.New(bc)
	if err != nil {
		t.Fatal(err)
	}
	cs := httptest.NewServer(NewControllerServer(c))
	defer cs.Close()
	cc := controllerClient(t, cs.URL)
	for _, target := range []broker.BrokerInfo{{ID: 0, Addr: s0.URL}, {ID: 1, Addr: s1.URL}} {
		if err := cc.RegisterBroker(ctx, target.ID, target.Addr); err != nil {
			t.Fatal(err)
		}
	}
	err = cc.CreateTopic(ctx, "orders", 4)
	requireError(t, err, ErrUnavailable)
	if !strings.Contains(err.Error(), "orders-1") || !strings.Contains(err.Error(), "broker 1") {
		t.Fatalf("lost assignment context: %v", err)
	}
	md, err := cc.Metadata(ctx)
	if err != nil || len(md.Partitions) != 0 {
		t.Fatalf("published partial metadata: %+v %v", md, err)
	}
	if len(b0.State()) != 1 || len(b1.State()) != 1 {
		t.Fatalf("missing orphan storage: %+v %+v", b0.State(), b1.State())
	}
	tp := broker.TopicPartition{Topic: "orders", Partition: 1}
	if _, err := bc.Produce(ctx, broker.BrokerInfo{Addr: s1.URL}, tp, "k", "survives replay"); err != nil {
		t.Fatal(err)
	}
	requireError(t, cc.CreateTopic(ctx, "orders", 5), controller.ErrPendingTopicConflict)
	// A newly registered broker must not change the retained placement plan.
	if err := cc.RegisterBroker(ctx, 2, "localhost:1"); err != nil {
		t.Fatal(err)
	}
	if err := cc.CreateTopic(ctx, "orders", 4); err != nil {
		t.Fatal(err)
	}
	md, err = cc.Metadata(ctx)
	if err != nil || len(md.Partitions) != 4 {
		t.Fatalf("metadata: %+v %v", md, err)
	}
	for _, p := range md.Partitions {
		if p.Leader != broker.BrokerID(p.Partition%2) {
			t.Fatalf("retry placement changed: %+v", p)
		}
	}
	records, err := bc.Fetch(ctx, broker.BrokerInfo{Addr: s1.URL}, tp, 0, 1)
	if err != nil || len(records.Records) != 1 || records.Records[0].Value != "survives replay" {
		t.Fatalf("replay lost records: %+v %v", records, err)
	}
}

func TestConcurrentHTTPClients(t *testing.T) {
	b := &broker.Broker{}
	tp := broker.TopicPartition{Topic: "concurrent"}
	s := httptest.NewServer(NewBrokerServer(b))
	defer s.Close()
	c := brokerClient(t, 0)
	target := broker.BrokerInfo{Addr: s.URL}
	ctx := context.Background()
	if err := c.AssignPartition(ctx, target, tp); err != nil {
		t.Fatal(err)
	}
	const count = 40
	offsets := make(chan int64, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			offset, err := c.Produce(ctx, target, tp, "", "")
			if err != nil {
				t.Error(err)
				return
			}
			offsets <- offset
			if _, err := c.Fetch(ctx, target, tp, 0, count); err != nil {
				t.Error(err)
			}
			if _, err := c.State(ctx, target); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(offsets)
	seen := make(map[int64]bool)
	for offset := range offsets {
		if seen[offset] {
			t.Errorf("duplicate offset %d", offset)
		}
		seen[offset] = true
	}
	for i := 0; i < count; i++ {
		if !seen[int64(i)] {
			t.Errorf("missing offset %d", i)
		}
	}
}

func TestCreateCancellationReachesAssignment(t *testing.T) {
	bc := brokerClient(t, 0)
	var stall atomic.Bool
	stall.Store(true)
	entered, canceled := make(chan struct{}), make(chan struct{})
	b := &broker.Broker{}
	handler := NewBrokerServer(b)
	bs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stall.Swap(false) {
			_, _ = io.Copy(io.Discard, r.Body)
			close(entered)
			<-r.Context().Done()
			close(canceled)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer bs.Close()
	c, err := controller.New(bc)
	if err != nil {
		t.Fatal(err)
	}
	cs := httptest.NewServer(NewControllerServer(c))
	defer cs.Close()
	cc := controllerClient(t, cs.URL)
	if err := cc.RegisterBroker(context.Background(), 0, bs.URL); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cc.CreateTopic(ctx, "t", 1) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("assignment did not start")
	}
	cancel()
	select {
	case err := <-done:
		requireError(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("create did not cancel")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not reach broker")
	}
	if err := cc.CreateTopic(context.Background(), "t", 1); err != nil {
		t.Fatalf("retry after cancellation: %v", err)
	}
	if len(c.Metadata().Partitions) != 1 || len(b.State()) != 1 {
		t.Fatal("retry failed to publish")
	}
}
