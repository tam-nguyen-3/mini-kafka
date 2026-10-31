// Package protocol defines the HTTP JSON wire format. It models a small subset
// of Kafka operations; it is not compatible with Kafka's binary protocol.
package protocol

import "github.com/tam-nguyen-3/mini-kafka/internal/broker"

// RegisterBrokerRequest advertises a broker ID and destination.
type RegisterBrokerRequest = broker.BrokerInfo

// AssignPartitionRequest identifies storage to create idempotently.
type AssignPartitionRequest = broker.TopicPartition

// CreateTopicRequest requests a fixed number of partitions.
type CreateTopicRequest struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

// ProduceRequest appends a string key/value to one partition.
type ProduceRequest struct {
	broker.TopicPartition
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ProduceResponse reports the broker-assigned offset.
type ProduceResponse struct {
	Offset int64 `json:"offset"`
}

// FetchRequest is encoded as query parameters by the HTTP transport.
type FetchRequest struct {
	broker.TopicPartition
	Offset     int64
	MaxRecords int
}

// FetchResponse contains records and LEO from the same log snapshot.
type FetchResponse = broker.FetchResult

// MetadataResponse describes destinations and published ownership.
type MetadataResponse = broker.ClusterMetadata

// StateResponse describes only the responding broker’s local storage.
type StateResponse struct {
	Partitions []broker.PartitionState `json:"partitions"`
}

// EmptyResponse acknowledges a successful operation.
type EmptyResponse struct{}

// ErrorCode is a stable machine-readable failure identifier.
type ErrorCode string

const (
	CodeInvalidRequest          ErrorCode = "invalid_request"
	CodeInvalidBrokerAddress    ErrorCode = "invalid_broker_address"
	CodeInvalidTopicName        ErrorCode = "invalid_topic_name"
	CodeInvalidPartitionCount   ErrorCode = "invalid_partition_count"
	CodeInvalidMaxRecords       ErrorCode = "invalid_max_records"
	CodeOffsetOutOfRange        ErrorCode = "offset_out_of_range"
	CodeUnknownTopicOrPartition ErrorCode = "unknown_topic_or_partition"
	CodeBrokerConflict          ErrorCode = "broker_conflict"
	CodeTopicAlreadyExists      ErrorCode = "topic_already_exists"
	CodePendingTopicConflict    ErrorCode = "pending_topic_conflict"
	CodeNoBrokers               ErrorCode = "no_brokers"
	CodeUnavailable             ErrorCode = "unavailable"
	CodeDeadlineExceeded        ErrorCode = "deadline_exceeded"
	CodeCanceled                ErrorCode = "canceled"
	CodeInternal                ErrorCode = "internal_error"
	CodeNotFound                ErrorCode = "not_found"
	CodeMethodNotAllowed        ErrorCode = "method_not_allowed"
	CodeBodyTooLarge            ErrorCode = "body_too_large"
	CodeUnsupportedMediaType    ErrorCode = "unsupported_media_type"
)

// ErrorResponse retains a stable code and human-readable diagnostics.
type ErrorResponse struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}
