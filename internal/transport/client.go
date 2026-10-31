package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/protocol"
)

// DefaultTimeout bounds a complete HTTP request, including its response body.
const DefaultTimeout = 5 * time.Second

// HTTPBrokerClient sends single attempts to the explicitly selected broker.
// It is safe for concurrent use. Construct it with NewHTTPBrokerClient.
// A failed produce response may hide a successful append: explicit retries can
// duplicate records. This transport provides neither deduplication nor failover.
type HTTPBrokerClient struct{ client *http.Client }

// HTTPControllerClient bootstraps metadata from one controller destination.
// It is safe for concurrent use. Construct it with NewHTTPControllerClient.
type HTTPControllerClient struct {
	client  *http.Client
	baseURL string
}

var _ controller.BrokerClient = (*HTTPBrokerClient)(nil)

func newClient(timeout time.Duration) (*http.Client, error) {
	if timeout < 0 {
		return nil, fmt.Errorf("%w: negative timeout", ErrInvalidRequest)
	}
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	return &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

// NewHTTPBrokerClient uses DefaultTimeout when timeout is zero; negatives fail.
func NewHTTPBrokerClient(timeout time.Duration) (*HTTPBrokerClient, error) {
	c, err := newClient(timeout)
	if err != nil {
		return nil, err
	}
	return &HTTPBrokerClient{client: c}, nil
}

// NewHTTPControllerClient validates its destination and finite request timeout.
func NewHTTPControllerClient(addr string, timeout time.Duration) (*HTTPControllerClient, error) {
	base, err := NormalizeAddress(addr)
	if err != nil {
		return nil, err
	}
	c, err := newClient(timeout)
	if err != nil {
		return nil, err
	}
	return &HTTPControllerClient{client: c, baseURL: base}, nil
}

func request(ctx context.Context, c *http.Client, base, method, path string, body any, out any, required ...string) error {
	var input io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		input = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, input)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	// Do not replay request bodies, including after redirects or connection errors.
	req.GetBody = nil
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		return networkError(ctx, err)
	}
	defer resp.Body.Close()
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return fmt.Errorf("invalid HTTP %d response: expected application/json", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return networkError(ctx, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return fmt.Errorf("invalid HTTP %d response: expected one JSON object", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		var remote protocol.ErrorResponse
		if err := json.Unmarshal(data, &remote); err != nil || remote.Code == "" || remote.Message == "" {
			return fmt.Errorf("invalid HTTP %d error response", resp.StatusCode)
		}
		return &RemoteError{Code: remote.Code, Message: remote.Message, StatusCode: resp.StatusCode}
	}
	for _, name := range required {
		raw, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("invalid response: missing or null %s", name)
		}
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
func networkError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("HTTP request: %w", ctx.Err())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("HTTP request: %w", err)
	}
	return fmt.Errorf("HTTP request: %w", errors.Join(ErrUnavailable, err))
}
func (c *HTTPBrokerClient) call(ctx context.Context, target broker.BrokerInfo, method, path string, body, out any, required ...string) error {
	base, err := NormalizeAddress(target.Addr)
	if err != nil {
		return err
	}
	return request(ctx, c.client, base, method, path, body, out, required...)
}

// AssignPartition requests idempotent local storage assignment.
func (c *HTTPBrokerClient) AssignPartition(ctx context.Context, target broker.BrokerInfo, tp broker.TopicPartition) error {
	var out protocol.EmptyResponse
	return c.call(ctx, target, "POST", "/partitions", tp, &out)
}

// Produce sends one append attempt; an error may hide a successful append.
func (c *HTTPBrokerClient) Produce(ctx context.Context, target broker.BrokerInfo, tp broker.TopicPartition, key, value string) (int64, error) {
	var out protocol.ProduceResponse
	err := c.call(ctx, target, "POST", "/produce", protocol.ProduceRequest{TopicPartition: tp, Key: key, Value: value}, &out, "offset")
	return out.Offset, err
}

// Fetch returns copied records and their atomic log-end-offset snapshot.
func (c *HTTPBrokerClient) Fetch(ctx context.Context, target broker.BrokerInfo, tp broker.TopicPartition, offset int64, maxRecords int) (broker.FetchResult, error) {
	q := url.Values{"topic": {tp.Topic}, "partition": {strconv.Itoa(tp.Partition)}, "offset": {strconv.FormatInt(offset, 10)}, "max": {strconv.Itoa(maxRecords)}}
	var out protocol.FetchResponse
	err := c.call(ctx, target, "GET", "/fetch?"+q.Encode(), nil, &out, "records", "logEndOffset")
	return out, err
}

// State returns hosted partitions and their independently sampled LEOs.
func (c *HTTPBrokerClient) State(ctx context.Context, target broker.BrokerInfo) ([]broker.PartitionState, error) {
	var out protocol.StateResponse
	err := c.call(ctx, target, "GET", "/state", nil, &out, "partitions")
	return out.Partitions, err
}

// RegisterBroker advertises a broker destination to the controller.
func (c *HTTPControllerClient) RegisterBroker(ctx context.Context, id broker.BrokerID, addr string) error {
	var out protocol.EmptyResponse
	return request(ctx, c.client, c.baseURL, "POST", "/brokers", protocol.RegisterBrokerRequest{ID: id, Addr: addr}, &out)
}

// CreateTopic waits for all assignments; failures may leave a pending plan.
func (c *HTTPControllerClient) CreateTopic(ctx context.Context, name string, numPartitions int) error {
	var out protocol.EmptyResponse
	return request(ctx, c.client, c.baseURL, "POST", "/topics", protocol.CreateTopicRequest{Name: name, Partitions: numPartitions}, &out)
}

// Metadata returns published ownership and registered destinations.
func (c *HTTPControllerClient) Metadata(ctx context.Context) (broker.ClusterMetadata, error) {
	var out protocol.MetadataResponse
	err := request(ctx, c.client, c.baseURL, "GET", "/metadata", nil, &out, "brokers", "partitions")
	return out, err
}
