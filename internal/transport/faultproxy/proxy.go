// Package faultproxy provides local, explicitly scheduled HTTP faults for
// learning experiments. It is not a production gateway or a remote control API.
package faultproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/protocol"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

// Mode selects one explicitly scheduled network failure.
type Mode string

const (
	Delay        Mode = "delay"
	Disconnect   Mode = "disconnect"
	DropResponse Mode = "drop-response"
)

// Operation selects an HTTP method and exact path; queries do not affect matching.
type Operation struct{ Method, Path string }

// Rule is captured at request entry, before its bounded body is drained.
type Rule struct {
	Operation
	Mode  Mode
	Delay time.Duration
}

// Counts records requests and upstream attempts, not guaranteed appends.
// Confirm append outcomes by fetching the broker's state and records.
type Counts struct{ Requests, Matched, Forwarded, Dropped, Canceled int }

// Proxy targets one fixed broker. Rules and snapshots are safe concurrently.
// Each request captures its rule at entry; clearing affects subsequent requests.
// DropResponse is consumed by one matching request; other rules persist.
type Proxy struct {
	mu        sync.Mutex
	target    *url.URL
	client    *http.Client
	transport *http.Transport
	rule      *Rule
	counts    map[Operation]Counts
}

// New validates a fixed destination and a positive complete-request timeout.
func New(target string, timeout time.Duration) (*Proxy, error) {
	base, err := transport.NormalizeAddress(target)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("proxy timeout must be positive")
	}
	u, _ := url.Parse(base)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DisableKeepAlives = true
	return &Proxy{target: u, client: &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, transport: tr, counts: make(map[Operation]Counts)}, nil
}

// SetRule atomically replaces the current rule after validating its operation.
func (p *Proxy) SetRule(rule Rule) error {
	if (rule.Method != "POST" && rule.Method != "GET") || rule.Path == "" || rule.Path[0] != '/' {
		return fmt.Errorf("invalid fault operation")
	}
	switch rule.Mode {
	case Delay:
		if rule.Delay <= 0 {
			return fmt.Errorf("delay must be positive")
		}
	case Disconnect:
	case DropResponse:
		if rule.Method != "POST" || rule.Path != "/produce" {
			return fmt.Errorf("response loss must target POST /produce")
		}
	default:
		return fmt.Errorf("invalid fault mode %q", rule.Mode)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rule = &rule
	return nil
}

// Clear disables faults for subsequent requests, preserving counters.
func (p *Proxy) Clear() { p.mu.Lock(); defer p.mu.Unlock(); p.rule = nil }
func (p *Proxy) Snapshot() map[Operation]Counts {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make(map[Operation]Counts, len(p.counts))
	for k, v := range p.counts {
		result[k] = v
	}
	return result
}
func (p *Proxy) Close() { p.transport.CloseIdleConnections() }
func (p *Proxy) update(op Operation, change func(*Counts)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v := p.counts[op]
	change(&v)
	p.counts[op] = v
}
func reply(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(protocol.ErrorResponse{Code: protocol.CodeUnavailable, Message: message})
}
func disconnect(w http.ResponseWriter) {
	h, ok := w.(http.Hijacker)
	if !ok {
		reply(w, 500, "fault requires HTTP/1 connection hijacking")
		return
	}
	conn, _, err := h.Hijack()
	if err == nil {
		_ = conn.Close()
	}
}
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	op := Operation{r.Method, r.URL.Path}
	p.mu.Lock()
	c := p.counts[op]
	c.Requests++
	var rule Rule
	if p.rule != nil && p.rule.Operation == op {
		rule = *p.rule
		c.Matched++
		if rule.Mode == DropResponse {
			p.rule = nil
		}
	}
	p.counts[op] = c
	p.mu.Unlock()
	// Draining a bounded body also lets net/http observe downstream disconnects
	// while a delay is waiting, instead of leaving an unread produce body pending.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, transport.MaxRequestBodyBytes))
	if err != nil {
		reply(w, 413, "proxy request body unreadable or too large")
		return
	}
	switch rule.Mode {
	case Disconnect:
		disconnect(w)
		return
	case Delay:
		timer := time.NewTimer(rule.Delay)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			p.update(op, func(c *Counts) { c.Canceled++ })
			return
		case <-timer.C:
		}
	}
	if r.Context().Err() != nil {
		p.update(op, func(c *Counts) { c.Canceled++ })
		return
	}
	dest := *p.target
	dest.Path = r.URL.Path
	dest.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(r.Context(), r.Method, dest.String(), bytes.NewReader(body))
	if err != nil {
		reply(w, 502, "proxy request construction failed")
		return
	}
	req.Header = r.Header.Clone()
	req.Header.Del("Connection")
	req.Header.Del("Proxy-Connection")
	req.GetBody = nil
	p.update(op, func(c *Counts) { c.Forwarded++ })
	response, err := p.client.Do(req)
	if err != nil {
		reply(w, 503, "proxy upstream unavailable: "+err.Error())
		return
	}
	defer response.Body.Close()
	if rule.Mode == DropResponse && response.StatusCode == http.StatusOK {
		if _, err = io.Copy(io.Discard, response.Body); err != nil {
			reply(w, 502, "proxy upstream response incomplete")
			return
		}
		p.update(op, func(c *Counts) { c.Dropped++ })
		disconnect(w)
		return
	}
	for k, vs := range response.Header {
		if k == "Connection" {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, response.Body)
}
