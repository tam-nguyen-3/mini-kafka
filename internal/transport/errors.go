package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/protocol"
)

var (
	// ErrUnavailable means a node could not be reached or its connection failed.
	// A produce may already have appended when this error is returned.
	ErrUnavailable = errors.New("node unavailable")
	// ErrInvalidRequest indicates malformed transport input.
	ErrInvalidRequest = errors.New("invalid request")
)

// RemoteError retains remote diagnostics and unwraps recognized wire codes.
type RemoteError struct {
	Code       protocol.ErrorCode
	Message    string
	StatusCode int
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("HTTP %d %s: %s", e.StatusCode, e.Code, e.Message)
}
func (e *RemoteError) Unwrap() error {
	for _, m := range errorMappings {
		if m.code == e.Code {
			return m.err
		}
	}
	return nil
}

type errorMapping struct {
	err    error
	code   protocol.ErrorCode
	status int
}

var errorMappings = []errorMapping{
	{context.DeadlineExceeded, protocol.CodeDeadlineExceeded, http.StatusGatewayTimeout},
	{context.Canceled, protocol.CodeCanceled, http.StatusRequestTimeout},
	{broker.ErrUnknownTopicOrPartition, protocol.CodeUnknownTopicOrPartition, http.StatusNotFound},
	{broker.ErrInvalidMaxRecords, protocol.CodeInvalidMaxRecords, http.StatusBadRequest},
	{broker.ErrOffsetOutOfRange, protocol.CodeOffsetOutOfRange, http.StatusBadRequest},
	{controller.ErrInvalidBrokerAddress, protocol.CodeInvalidBrokerAddress, http.StatusBadRequest},
	{controller.ErrInvalidTopicName, protocol.CodeInvalidTopicName, http.StatusBadRequest},
	{controller.ErrInvalidPartitionCount, protocol.CodeInvalidPartitionCount, http.StatusBadRequest},
	{controller.ErrBrokerConflict, protocol.CodeBrokerConflict, http.StatusConflict},
	{controller.ErrTopicAlreadyExists, protocol.CodeTopicAlreadyExists, http.StatusConflict},
	{controller.ErrPendingTopicConflict, protocol.CodePendingTopicConflict, http.StatusConflict},
	{controller.ErrNoBrokers, protocol.CodeNoBrokers, http.StatusServiceUnavailable},
	{ErrUnavailable, protocol.CodeUnavailable, http.StatusServiceUnavailable},
	{ErrInvalidRequest, protocol.CodeInvalidRequest, http.StatusBadRequest},
}

func writeError(w http.ResponseWriter, err error) {
	for _, m := range errorMappings {
		if errors.Is(err, m.err) {
			writeJSON(w, m.status, protocol.ErrorResponse{Code: m.code, Message: err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusInternalServerError, protocol.ErrorResponse{Code: protocol.CodeInternal, Message: err.Error()})
}
func wireError(w http.ResponseWriter, status int, code protocol.ErrorCode, message string) {
	writeJSON(w, status, protocol.ErrorResponse{Code: code, Message: message})
}
