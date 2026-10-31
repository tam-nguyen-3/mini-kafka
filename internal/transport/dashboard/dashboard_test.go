package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

type fixture struct {
	ui         *Server
	brokers    []*httptest.Server
	cores      []*broker.Broker
	controller *httptest.Server
}

func setup(t *testing.T) fixture {
	t.Helper()
	bc, _ := transport.NewHTTPBrokerClient(100 * time.Millisecond)
	cc, _ := controller.New(bc)
	f := fixture{}
	for i := 1; i <= 2; i++ {
		b := &broker.Broker{ID: broker.BrokerID(i)}
		srv := httptest.NewServer(transport.NewBrokerServer(b))
		t.Cleanup(srv.Close)
		f.brokers = append(f.brokers, srv)
		f.cores = append(f.cores, b)
		if err := cc.RegisterBroker(b.ID, srv.URL); err != nil {
			t.Fatal(err)
		}
	}
	if err := cc.CreateTopic(context.Background(), "orders", 4); err != nil {
		t.Fatal(err)
	}
	cs := httptest.NewServer(transport.NewControllerServer(cc))
	t.Cleanup(cs.Close)
	f.controller = cs
	mc, _ := transport.NewHTTPControllerClient(cs.URL, 100*time.Millisecond)
	ui, err := New(mc, bc, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ui.Close)
	f.ui = ui
	return f
}
func call(t *testing.T, s http.Handler, method, path string, v any) (int, []byte) {
	t.Helper()
	var body []byte
	if v != nil {
		body, _ = json.Marshal(v)
	}
	r := httptest.NewRequest(method, "http://localhost"+path, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", "http://localhost")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w.Code, w.Body.Bytes()
}
func ok(t *testing.T, s http.Handler, method, path string, v any) []byte {
	t.Helper()
	status, b := call(t, s, method, path, v)
	if status < 200 || status >= 300 {
		t.Fatalf("%s: status %d: %s", path, status, b)
	}
	return b
}
func sessionID(t *testing.T, s *Server) string {
	t.Helper()
	var v map[string]string
	_ = json.Unmarshal(ok(t, s, "POST", "/api/sessions", struct{}{}), &v)
	return v["id"]
}
func poll(t *testing.T, s *Server, op operation) pollResponse {
	t.Helper()
	var v pollResponse
	if err := json.Unmarshal(ok(t, s, "POST", "/api/consumer", op), &v); err != nil {
		t.Fatal(err)
	}
	return v
}
func TestSessionRoutingReplaySeekAndIsolation(t *testing.T) {
	f := setup(t)
	id := sessionID(t, f.ui)
	other := sessionID(t, f.ui)
	for i := 0; i < 8; i++ {
		var v struct {
			Partition int    `json:"partition"`
			Offset    string `json:"offset"`
		}
		_ = json.Unmarshal(ok(t, f.ui, "POST", "/api/produce", operation{ID: id, Topic: "orders", Value: fmt.Sprint(i)}), &v)
		if v.Partition != i%4 || v.Offset != strconv.Itoa(i/4) {
			t.Fatalf("round robin: %+v", v)
		}
	}
	assign := operation{ID: id, Sequence: 1, Action: "assign", Topic: "orders", Partitions: []int{0, 1, 2, 3}, Offset: "0"}
	poll(t, f.ui, assign)
	op := operation{ID: id, Sequence: 2, Action: "poll"}
	first := ok(t, f.ui, "POST", "/api/consumer", op)
	second := ok(t, f.ui, "POST", "/api/consumer", op)
	if !bytes.Equal(first, second) {
		t.Fatal("lost response replay differs")
	}
	var result pollResponse
	_ = json.Unmarshal(first, &result)
	if len(result.Records) != 8 {
		t.Fatalf("records %d", len(result.Records))
	}
	for _, p := range result.Positions {
		if p.Offset != "2" {
			t.Fatal(p)
		}
	}
	if status, _ := call(t, f.ui, "POST", "/api/consumer", operation{ID: id, Sequence: 2, Action: "seek", Topic: "orders", Partitions: []int{0}, Offset: "0"}); status != 409 {
		t.Fatal(status)
	}
	if status, _ := call(t, f.ui, "POST", "/api/consumer", operation{ID: id, Sequence: 4, Action: "poll"}); status != 409 {
		t.Fatal(status)
	}
	if v := poll(t, f.ui, operation{ID: id, Sequence: 3, Action: "poll"}); len(v.Records) != 0 {
		t.Fatal("duplicate delivery")
	}
	assign.ID = other
	poll(t, f.ui, assign)
	if v := poll(t, f.ui, operation{ID: other, Sequence: 2, Action: "poll"}); len(v.Records) != 8 {
		t.Fatal("sessions share positions")
	}
	poll(t, f.ui, operation{ID: id, Sequence: 4, Action: "seek", Topic: "orders", Partitions: []int{0}, Offset: "1"})
	if v := poll(t, f.ui, operation{ID: id, Sequence: 5, Action: "poll"}); len(v.Records) != 1 || v.Records[0].Offset != "1" {
		t.Fatalf("seek: %+v", v)
	}
}
func TestStaleObservationsAndHealthyProgress(t *testing.T) {
	f := setup(t)
	id := sessionID(t, f.ui)
	for i, b := range f.cores {
		_, _ = b.Produce(broker.TopicPartition{Topic: "orders", Partition: i}, "k", "v")
	}
	ok(t, f.ui, "GET", "/api/cluster", nil)
	poll(t, f.ui, operation{ID: id, Sequence: 1, Action: "assign", Topic: "orders", Partitions: []int{0, 1}, Offset: "0"})
	poll(t, f.ui, operation{ID: id, Sequence: 2, Action: "poll"}) // prime consumer routes
	f.brokers[0].Close()
	f.controller.Close()
	_, _ = f.cores[1].Produce(broker.TopicPartition{Topic: "orders", Partition: 1}, "k", "healthy")
	var snap snapshot
	_ = json.Unmarshal(ok(t, f.ui, "GET", "/api/cluster", nil), &snap)
	if snap.ControllerError == "" || snap.ControllerSuccess == "" || len(snap.Metadata.Partitions) != 4 {
		t.Fatalf("metadata cache: %+v", snap)
	}
	if snap.Brokers[0].Error == "" || snap.Brokers[0].LastSuccess == "" || snap.Brokers[0].Partitions[0].LEO != "1" {
		t.Fatalf("stale broker: %+v", snap.Brokers[0])
	}
	if snap.Brokers[1].Error != "" {
		t.Fatal(snap.Brokers[1].Error)
	}
	result := poll(t, f.ui, operation{ID: id, Sequence: 3, Action: "poll"})
	if len(result.Records) != 1 || result.Records[0].Value != "healthy" || len(result.Errors) != 1 {
		t.Fatalf("partial result: %+v", result)
	}
}
func TestInspectionBoundedAndIndependent(t *testing.T) {
	f := setup(t)
	tp := broker.TopicPartition{Topic: "orders", Partition: 0}
	for i := 0; i < 60; i++ {
		_, _ = f.cores[0].Produce(tp, "<script>", "value")
	}
	ok(t, f.ui, "GET", "/api/cluster", nil)
	var v struct {
		Records []record `json:"records"`
		LEO     string   `json:"leo"`
	}
	_ = json.Unmarshal(ok(t, f.ui, "GET", "/api/records?topic=orders&partition=0&offset=0", nil), &v)
	if len(v.Records) != 50 || v.LEO != "60" {
		t.Fatalf("page: %+v", v)
	}
	id := sessionID(t, f.ui)
	poll(t, f.ui, operation{ID: id, Sequence: 1, Action: "assign", Topic: "orders", Partitions: []int{0}, Offset: "0"})
	if v := poll(t, f.ui, operation{ID: id, Sequence: 2, Action: "poll"}); len(v.Records) != 50 || v.Records[0].Offset != "0" {
		t.Fatal("inspection advanced consumer")
	}
	if status, _ := call(t, f.ui, "GET", "/api/records?topic=orders&partition=0&offset=9223372036854775808", nil); status != 400 {
		t.Fatal(status)
	}
}
func TestSessionExpirationCapacityAndSecurity(t *testing.T) {
	f := setup(t)
	now := time.Now()
	f.ui.now = func() time.Time { return now }
	id := sessionID(t, f.ui)
	for i := 1; i < 32; i++ {
		sessionID(t, f.ui)
	}
	if status, _ := call(t, f.ui, "POST", "/api/sessions", struct{}{}); status != 429 {
		t.Fatal(status)
	}
	now = now.Add(30 * time.Minute)
	if status, _ := call(t, f.ui, "POST", "/api/produce", operation{ID: id, Topic: "orders"}); status != 410 {
		t.Fatal(status)
	}
	sessionID(t, f.ui)
	r := httptest.NewRequest("POST", "http://localhost/api/sessions", strings.NewReader("{}"))
	r.Header.Set("Origin", "http://evil.example")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.ui.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
	if status, _ := call(t, f.ui, "POST", "/api/sessions", map[string]string{"unknown": "field"}); status != 400 {
		t.Fatal(status)
	}
	for _, path := range []string{"/", "/app.js", "/style.css"} {
		ok(t, f.ui, "GET", path, nil)
	}
}
func TestTopicPartialFailureIsExplicit(t *testing.T) {
	f := setup(t)
	f.brokers[1].Close()
	status, b := call(t, f.ui, "POST", "/api/topics", map[string]any{"name": "partial", "partitions": 2})
	if status != 502 || !strings.Contains(string(b), "partial assignment") {
		t.Fatalf("%d %s", status, b)
	}
}

// Exact decimal strings preserve offsets above JavaScript's safe integer limit.
func TestExactOffsets(t *testing.T) {
	v := wireRecord(broker.TopicPartition{}, broker.Record{Offset: 9007199254740993, Timestamp: 9007199254740995})
	b, _ := json.Marshal(v)
	if !strings.Contains(string(b), `"offset":"9007199254740993"`) {
		t.Fatal(string(b))
	}
	if _, err := parseOffset("9223372036854775807"); err != nil {
		t.Fatal(err)
	}
}

type lostResponse struct {
	Brokers
	calls int
}

func (l *lostResponse) Produce(ctx context.Context, b broker.BrokerInfo, tp broker.TopicPartition, key, value string) (int64, error) {
	l.calls++
	if _, err := l.Brokers.Produce(ctx, b, tp, key, value); err != nil {
		return -1, err
	}
	return -1, fmt.Errorf("response lost")
}
func TestAmbiguousProduceIsNotRetried(t *testing.T) {
	f := setup(t)
	lost := &lostResponse{Brokers: f.ui.brokers}
	f.ui.brokers = lost
	id := sessionID(t, f.ui)
	status, b := call(t, f.ui, "POST", "/api/produce", operation{ID: id, Topic: "orders", Value: "once"})
	if status != 502 || !strings.Contains(string(b), `"ambiguous":true`) || lost.calls != 1 {
		t.Fatalf("%d %s calls=%d", status, b, lost.calls)
	}
	if f.cores[0].State()[0].LogEndOffset != 1 {
		t.Fatal("append missing or repeated")
	}
}
