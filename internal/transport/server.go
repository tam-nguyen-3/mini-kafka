// Package transport adapts network-free broker/controller cores to HTTP+JSON.
// Handlers do not bind sockets. The process owner configures listeners, server
// read/write timeouts, and graceful shutdown independently of advertised URLs.
package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/protocol"
)

// MaxRequestBodyBytes bounds each incoming JSON document to 1 MiB.
const MaxRequestBodyBytes = 1 << 20

type endpoint struct {
	method string
	handle http.HandlerFunc
}

func routes(endpoints map[string]endpoint) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e, ok := endpoints[r.URL.Path]
		if !ok {
			wireError(w, 404, protocol.CodeNotFound, "unknown path")
			return
		}
		if r.Method != e.method {
			w.Header().Set("Allow", e.method)
			wireError(w, 405, protocol.CodeMethodNotAllowed, "unsupported method")
			return
		}
		if err := r.Context().Err(); err != nil {
			writeError(w, err)
			return
		}
		e.handle(w, r)
	})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Values are concrete, JSON-safe protocol types. A write failure cannot be
	// repaired with another HTTP response (and must never trigger an append retry).
	_ = json.NewEncoder(w).Encode(value)
}
func decodeBody(w http.ResponseWriter, r *http.Request, dst any, required ...string) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		wireError(w, 415, protocol.CodeUnsupportedMediaType, "expected application/json")
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxRequestBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			wireError(w, 413, protocol.CodeBodyTooLarge, "request body exceeds 1 MiB")
		} else {
			writeError(w, fmt.Errorf("%w: read body: %v", ErrInvalidRequest, err))
		}
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		writeError(w, fmt.Errorf("%w: expected one JSON object", ErrInvalidRequest))
		return false
	}
	for _, name := range required {
		raw, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			writeError(w, fmt.Errorf("%w: missing or null %s", ErrInvalidRequest, name))
			return false
		}
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, fmt.Errorf("%w: %v", ErrInvalidRequest, err))
		return false
	}
	return true
}
func validateTP(tp broker.TopicPartition) error {
	if strings.TrimSpace(tp.Topic) == "" {
		return controller.ErrInvalidTopicName
	}
	if tp.Partition < 0 {
		return fmt.Errorf("%w: negative partition", ErrInvalidRequest)
	}
	return nil
}

// NewBrokerServer serves only this broker's private partitions.
func NewBrokerServer(b *broker.Broker) http.Handler {
	return NewBrokerServerWithLogger(b, nil)
}

// NewBrokerServerWithLogger additionally logs parsed produce/fetch outcomes.
// Timestamps in these logs are observations, not a cross-broker ordering clock.
// A nil logger disables request logging. Record contents are never logged.
func NewBrokerServerWithLogger(b *broker.Broker, logger *slog.Logger) http.Handler {
	logOutcome := func(operation string, tp broker.TopicPartition, offset int64, count int, err error) {
		if logger != nil {
			logger.Info("broker request", "broker", b.ID, "operation", operation,
				"topic", tp.Topic, "partition", tp.Partition, "offset", offset, "count", count, "error", err)
		}
	}
	return routes(map[string]endpoint{
		"/partitions": {"POST", func(w http.ResponseWriter, r *http.Request) {
			var req protocol.AssignPartitionRequest
			if !decodeBody(w, r, &req, "topic", "partition") {
				return
			}
			if err := validateTP(req); err != nil {
				writeError(w, err)
				return
			}
			b.AssignPartition(req)
			writeJSON(w, 200, protocol.EmptyResponse{})
		}},
		"/produce": {"POST", func(w http.ResponseWriter, r *http.Request) {
			var req protocol.ProduceRequest
			if !decodeBody(w, r, &req, "topic", "partition", "key", "value") {
				return
			}
			if err := validateTP(req.TopicPartition); err != nil {
				writeError(w, err)
				return
			}
			offset, err := b.Produce(req.TopicPartition, req.Key, req.Value)
			count := 0
			if err == nil {
				count = 1
			} else {
				offset = -1
			}
			logOutcome("produce", req.TopicPartition, offset, count, err)
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, 200, protocol.ProduceResponse{Offset: offset})
		}},
		"/fetch": {"GET", func(w http.ResponseWriter, r *http.Request) {
			req, err := parseFetch(r)
			if err != nil {
				writeError(w, err)
				return
			}
			result, err := b.FetchWithLEO(req.TopicPartition, req.Offset, req.MaxRecords)
			logOutcome("fetch", req.TopicPartition, req.Offset, len(result.Records), err)
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, 200, result)
		}},
		"/state": {"GET", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, 200, protocol.StateResponse{Partitions: b.State()})
		}},
	})
}
func parseFetch(r *http.Request) (protocol.FetchRequest, error) {
	var req protocol.FetchRequest
	q, err := urlQuery(r)
	if err != nil {
		return req, err
	}
	req.Topic = q["topic"][0]
	req.Partition, err = strconv.Atoi(q["partition"][0])
	if err != nil {
		return req, fmt.Errorf("%w: invalid partition", ErrInvalidRequest)
	}
	if err = validateTP(req.TopicPartition); err != nil {
		return req, err
	}
	req.Offset, err = strconv.ParseInt(q["offset"][0], 10, 64)
	if err != nil {
		return req, fmt.Errorf("%w: invalid offset", ErrInvalidRequest)
	}
	if req.Offset < 0 {
		return req, broker.ErrOffsetOutOfRange
	}
	req.MaxRecords, err = strconv.Atoi(q["max"][0])
	if err != nil {
		return req, fmt.Errorf("%w: invalid max", ErrInvalidRequest)
	}
	if req.MaxRecords < 1 {
		return req, broker.ErrInvalidMaxRecords
	}
	return req, nil
}
func urlQuery(r *http.Request) (map[string][]string, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed query", ErrInvalidRequest)
	}
	for _, name := range []string{"topic", "partition", "offset", "max"} {
		if len(q[name]) != 1 || q[name][0] == "" {
			return nil, fmt.Errorf("%w: require one %s", ErrInvalidRequest, name)
		}
	}
	return q, nil
}

// NewControllerServer exposes metadata and cancellation-aware topic creation.
func NewControllerServer(c *controller.Controller) http.Handler {
	return routes(map[string]endpoint{
		"/brokers": {"POST", func(w http.ResponseWriter, r *http.Request) {
			var req protocol.RegisterBrokerRequest
			if !decodeBody(w, r, &req, "id", "addr") {
				return
			}
			addr, err := NormalizeAddress(req.Addr)
			if err == nil {
				err = c.RegisterBroker(req.ID, addr)
			}
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, 200, protocol.EmptyResponse{})
		}},
		"/topics": {"POST", func(w http.ResponseWriter, r *http.Request) {
			var req protocol.CreateTopicRequest
			if !decodeBody(w, r, &req, "name", "partitions") {
				return
			}
			if err := c.CreateTopic(r.Context(), req.Name, req.Partitions); err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, 200, protocol.EmptyResponse{})
		}},
		"/metadata": {"GET", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, c.Metadata()) }},
	})
}
