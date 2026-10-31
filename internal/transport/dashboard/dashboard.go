// Package dashboard adapts the existing learning clients to a local browser.
// It owns observation caches and ephemeral browser sessions, never broker storage.
package dashboard

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/client"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

//go:embed assets/*
var assets embed.FS

type Controller interface {
	client.MetadataClient
	CreateTopic(context.Context, string, int) error
}
type Brokers interface {
	client.BrokerClient
	client.FetchClient
	State(context.Context, broker.BrokerInfo) ([]broker.PartitionState, error)
}

type record struct {
	broker.TopicPartition
	Offset    string `json:"offset"`
	Timestamp string `json:"timestamp"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

func wireRecord(tp broker.TopicPartition, r broker.Record) record {
	return record{tp, strconv.FormatInt(r.Offset, 10), strconv.FormatInt(r.Timestamp, 10), r.Key, r.Value}
}

type partitionState struct {
	broker.TopicPartition
	LEO string `json:"leo"`
}
type observation struct {
	broker.BrokerInfo
	Partitions  []partitionState `json:"partitions"`
	LastSuccess string           `json:"lastSuccess"`
	Error       string           `json:"error"`
}
type snapshot struct {
	Metadata          broker.ClusterMetadata `json:"metadata"`
	ControllerError   string                 `json:"controllerError"`
	ControllerSuccess string                 `json:"controllerSuccess"`
	Brokers           []observation          `json:"brokers"`
}
type session struct {
	gate         chan struct{}
	producer     *client.Producer
	consumer     *client.Consumer
	positions    map[broker.TopicPartition]int64
	seq          uint64
	lastRequest  string
	lastResponse any
	touched      time.Time // guarded by Server.mu
	refs         int       // prevents expiration during a request
}

// Server must be closed when its owning HTTP server stops.
type Server struct {
	controller Controller
	brokers    Brokers
	timeout    time.Duration
	mu         sync.Mutex
	sessions   map[string]*session
	cached     snapshot // immutable after publication
	refresh    chan struct{}
	stop       chan struct{}
	closeOnce  sync.Once
	now        func() time.Time
}

func New(c Controller, b Brokers, timeout time.Duration) (*Server, error) {
	if c == nil || b == nil || timeout <= 0 {
		return nil, fmt.Errorf("dashboard requires clients and a positive timeout")
	}
	s := &Server{controller: c, brokers: b, timeout: timeout, sessions: map[string]*session{}, refresh: make(chan struct{}, 1), stop: make(chan struct{}), now: time.Now}
	s.cached = snapshot{Metadata: broker.ClusterMetadata{Brokers: []broker.BrokerInfo{}, Partitions: []broker.PartitionMetadata{}}, Brokers: []observation{}}
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.mu.Lock()
				s.expire()
				s.mu.Unlock()
			case <-s.stop:
				return
			}
		}
	}()
	return s, nil
}
func (s *Server) Close() { s.closeOnce.Do(func() { close(s.stop) }) }
func (s *Server) expire() {
	for id, v := range s.sessions {
		if v.refs == 0 && s.now().Sub(v.touched) >= 30*time.Minute {
			delete(s.sessions, id)
		}
	}
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, err error) {
	respond(w, status, map[string]string{"error": err.Error()})
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/json" {
		return fmt.Errorf("expected application/json")
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected one JSON object")
	}
	return nil
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")
	if r.Method != "GET" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		origin, err := url.Parse(r.Header.Get("Origin"))
		if err != nil || origin.Host != r.Host || origin.Scheme != scheme || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			fail(w, 403, fmt.Errorf("same-origin request required"))
			return
		}
	}
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		if r.Method != "GET" || (r.URL.Path != "/" && r.URL.Path != "/app.js" && r.URL.Path != "/style.css") {
			http.NotFound(w, r)
			return
		}
		files, _ := fs.Sub(assets, "assets")
		http.FileServer(http.FS(files)).ServeHTTP(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	switch r.Method + " " + r.URL.Path {
	case "GET /api/cluster":
		s.cluster(w, r)
	case "GET /api/records":
		s.records(w, r)
	case "POST /api/topics":
		var v struct {
			Name       string `json:"name"`
			Partitions int    `json:"partitions"`
		}
		if err := decode(w, r, &v); err != nil {
			fail(w, 400, err)
			return
		}
		if err := s.controller.CreateTopic(ctx, v.Name, v.Partitions); err != nil {
			fail(w, 502, fmt.Errorf("%w; partial assignment may remain; explicitly retry the same name and count", err))
			return
		}
		respond(w, 200, map[string]bool{"created": true})
	case "POST /api/sessions":
		s.createSession(w, r)
	case "POST /api/consumer", "POST /api/produce", "POST /api/session/delete":
		s.sessionRequest(w, r)
	default:
		fail(w, 404, fmt.Errorf("unknown dashboard endpoint or method"))
	}
}
func (s *Server) cluster(w http.ResponseWriter, r *http.Request) {
	select {
	case s.refresh <- struct{}{}:
		defer func() { <-s.refresh }()
	case <-r.Context().Done():
		fail(w, 504, r.Context().Err())
		return
	}
	s.mu.Lock()
	old := s.cached
	s.mu.Unlock()
	next := snapshot{Metadata: old.Metadata, ControllerSuccess: old.ControllerSuccess, Brokers: []observation{}}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	m, err := s.controller.Metadata(ctx)
	cancel()
	if err != nil {
		next.ControllerError = err.Error()
	} else {
		next.Metadata = m
		next.ControllerSuccess = s.now().UTC().Format(time.RFC3339)
	}
	prior := map[broker.BrokerID]observation{}
	for _, v := range old.Brokers {
		prior[v.ID] = v
	}
	next.Brokers = make([]observation, len(next.Metadata.Brokers))
	var wg sync.WaitGroup
	slots := make(chan struct{}, 4)
	for i, b := range next.Metadata.Brokers {
		wg.Add(1)
		go func(i int, b broker.BrokerInfo) {
			defer wg.Done()
			o := prior[b.ID]
			if o.Addr != b.Addr {
				o = observation{}
			}
			o.BrokerInfo = b
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-r.Context().Done():
				o.Error = r.Context().Err().Error()
				next.Brokers[i] = o
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
			defer cancel()
			states, err := s.brokers.State(ctx, b)
			if err != nil {
				o.Error = err.Error()
			} else {
				o.Error = ""
				o.LastSuccess = s.now().UTC().Format(time.RFC3339)
				o.Partitions = []partitionState{}
				for _, p := range states {
					o.Partitions = append(o.Partitions, partitionState{p.TopicPartition, strconv.FormatInt(p.LogEndOffset, 10)})
				}
			}
			next.Brokers[i] = o
		}(i, b)
	}
	wg.Wait()
	s.mu.Lock()
	s.cached = next
	s.mu.Unlock()
	respond(w, 200, next)
}
func (s *Server) records(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p, err := strconv.Atoi(q.Get("partition"))
	if err != nil || p < 0 {
		fail(w, 400, fmt.Errorf("invalid partition"))
		return
	}
	offset, err := parseOffset(q.Get("offset"))
	if err != nil {
		fail(w, 400, err)
		return
	}
	tp := broker.TopicPartition{Topic: q.Get("topic"), Partition: p}
	s.mu.Lock()
	m := s.cached.Metadata
	s.mu.Unlock()
	var target broker.BrokerInfo
	found := false
	for _, v := range m.Partitions {
		if v.TopicPartition == tp {
			for _, b := range m.Brokers {
				if b.ID == v.Leader {
					target = b
					found = true
				}
			}
		}
	}
	if !found {
		fail(w, 404, fmt.Errorf("partition is not in observed metadata; refresh cluster"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	result, err := s.brokers.Fetch(ctx, target, tp, offset, 50)
	if err != nil {
		fail(w, 502, err)
		return
	}
	out := []record{}
	for _, r := range result.Records {
		out = append(out, wireRecord(tp, r))
	}
	respond(w, 200, map[string]any{"records": out, "leo": strconv.FormatInt(result.LogEndOffset, 10)})
}
func parseOffset(v string) (int64, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("offset must be a nonnegative 64-bit integer")
	}
	return n, nil
}
func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var empty struct{}
	if err := decode(w, r, &empty); err != nil {
		fail(w, 400, err)
		return
	}
	p, err := client.NewProducer(s.controller, s.brokers)
	if err != nil {
		fail(w, 500, err)
		return
	}
	c, err := client.NewConsumer(s.controller, s.brokers)
	if err != nil {
		fail(w, 500, err)
		return
	}
	var idBytes [24]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		fail(w, 500, err)
		return
	}
	id := hex.EncodeToString(idBytes[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire()
	if len(s.sessions) >= 32 {
		fail(w, 429, fmt.Errorf("32 sessions are active; close a session or wait for expiration"))
		return
	}
	s.sessions[id] = &session{gate: make(chan struct{}, 1), producer: p, consumer: c, positions: map[broker.TopicPartition]int64{}, touched: s.now()}
	respond(w, 201, map[string]string{"id": id})
}

type operation struct {
	ID         string `json:"id"`
	Sequence   uint64 `json:"sequence"`
	Action     string `json:"action"`
	Topic      string `json:"topic"`
	Partitions []int  `json:"partitions"`
	Offset     string `json:"offset"`
	Key        string `json:"key"`
	Value      string `json:"value"`
}
type position struct {
	broker.TopicPartition
	Offset string `json:"offset"`
}
type pollResponse struct {
	Records   []record   `json:"records"`
	Positions []position `json:"positions"`
	Errors    []string   `json:"errors"`
	Sequence  uint64     `json:"sequence"`
}

func (s *Server) sessionRequest(w http.ResponseWriter, r *http.Request) {
	var v operation
	if err := decode(w, r, &v); err != nil {
		fail(w, 400, err)
		return
	}
	s.mu.Lock()
	s.expire()
	ss := s.sessions[v.ID]
	if ss != nil {
		ss.refs++
		ss.touched = s.now()
	}
	s.mu.Unlock()
	if ss == nil {
		fail(w, 410, fmt.Errorf("session expired or unavailable; start a new session explicitly"))
		return
	}
	defer func() { s.mu.Lock(); ss.refs--; ss.touched = s.now(); s.mu.Unlock() }()
	select {
	case ss.gate <- struct{}{}:
		defer func() { <-ss.gate }()
	case <-r.Context().Done():
		fail(w, 504, r.Context().Err())
		return
	}
	s.mu.Lock()
	stillActive := s.sessions[v.ID] == ss
	s.mu.Unlock()
	if !stillActive {
		fail(w, 410, fmt.Errorf("session was closed"))
		return
	}
	if r.URL.Path == "/api/session/delete" {
		s.mu.Lock()
		delete(s.sessions, v.ID)
		s.mu.Unlock()
		respond(w, 200, map[string]bool{"deleted": true})
		return
	}
	if r.URL.Path == "/api/produce" {
		ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
		defer cancel()
		p, o, err := ss.producer.Send(ctx, v.Topic, v.Key, v.Value)
		if err != nil {
			var remote *transport.RemoteError
			ambiguous := p >= 0 && !errors.As(err, &remote)
			respond(w, 502, map[string]any{"error": err.Error(), "ambiguous": ambiguous, "partition": p})
			return
		}
		respond(w, 200, map[string]any{"partition": p, "offset": strconv.FormatInt(o, 10)})
		return
	}
	fingerprint, _ := json.Marshal(v)
	if v.Sequence == ss.seq && ss.lastResponse != nil {
		if ss.lastRequest != string(fingerprint) {
			fail(w, 409, fmt.Errorf("sequence reused for a different operation"))
			return
		}
		respond(w, 200, ss.lastResponse)
		return
	}
	if v.Sequence != ss.seq+1 {
		fail(w, 409, fmt.Errorf("out-of-sequence consumer operation"))
		return
	}
	out := pollResponse{Records: []record{}, Positions: []position{}, Errors: []string{}, Sequence: v.Sequence}
	switch v.Action {
	case "assign":
		offset, err := parseOffset(v.Offset)
		if err != nil {
			fail(w, 400, err)
			return
		}
		if len(v.Partitions) == 0 || len(v.Partitions) > 1000 {
			fail(w, 400, fmt.Errorf("select between 1 and 1000 partitions"))
			return
		}
		tps := []broker.TopicPartition{}
		for _, p := range v.Partitions {
			tps = append(tps, broker.TopicPartition{Topic: v.Topic, Partition: p})
		}
		// Construct replacement state before publication; cancellation cannot leave a half-assignment.
		c, err := client.NewConsumer(s.controller, s.brokers)
		if err == nil {
			err = c.Assign(r.Context(), tps...)
		}
		if err != nil {
			fail(w, 400, err)
			return
		}
		positions := map[broker.TopicPartition]int64{}
		for _, tp := range tps {
			if err = c.Seek(r.Context(), tp, offset); err != nil {
				fail(w, 400, err)
				return
			}
			positions[tp] = offset
		}
		ss.consumer = c
		ss.positions = positions
	case "seek":
		offset, err := parseOffset(v.Offset)
		if err != nil {
			fail(w, 400, err)
			return
		}
		if len(v.Partitions) != 1 {
			fail(w, 400, fmt.Errorf("seek requires one partition"))
			return
		}
		tp := broker.TopicPartition{Topic: v.Topic, Partition: v.Partitions[0]}
		if err = ss.consumer.Seek(r.Context(), tp, offset); err != nil {
			fail(w, 400, err)
			return
		}
		ss.positions[tp] = offset
	case "poll":
		if len(ss.positions) == 0 {
			fail(w, 400, fmt.Errorf("assign partitions first"))
			return
		}
		result, err := ss.consumer.Poll(r.Context(), 50)
		for _, v := range result.Records {
			out.Records = append(out.Records, wireRecord(v.TopicPartition, v.Record))
			ss.positions[v.TopicPartition] = v.Offset + 1
		}
		for _, e := range result.PartitionErrors {
			out.Errors = append(out.Errors, e.Error())
		}
		if err != nil {
			out.Errors = append(out.Errors, err.Error())
		}
	default:
		fail(w, 400, fmt.Errorf("unknown consumer action"))
		return
	}
	for tp, o := range ss.positions {
		out.Positions = append(out.Positions, position{tp, strconv.FormatInt(o, 10)})
	}
	sort.Slice(out.Positions, func(i, j int) bool {
		a, b := out.Positions[i], out.Positions[j]
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})
	ss.seq = v.Sequence
	ss.lastRequest = string(fingerprint)
	ss.lastResponse = out
	respond(w, 200, out)
}
