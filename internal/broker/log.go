package broker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrOffsetOutOfRange indicates an offset is negative or past the log-end offset.
	ErrOffsetOutOfRange = errors.New("offset out of range")
	// ErrInvalidMaxRecords indicates a read limit is less than one.
	ErrInvalidMaxRecords = errors.New("maxRecords must be at least 1")
)

// Record holds a key, value, offset, and wall-clock timestamp for a log entry.
// Timestamp is in Unix milliseconds and does not determine record order.
type Record struct {
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// NewRecord returns a record with the given key and value and the current timestamp.
// Its offset is assigned when appended to a log.
func NewRecord(key, value string) Record {
	return Record{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixMilli(),
	}
}

// Log is an in-memory, append-only partition log with dense offsets starting at zero.
// A Log is safe for concurrent use. Its zero value is an empty log ready for use.
// A Log must not be copied after first use.
type Log struct {
	mu      sync.RWMutex
	records []Record
	now     func() time.Time
}

// NewLog returns an empty log.
func NewLog() *Log {
	return &Log{records: make([]Record, 0)}
}

// Append adds a record with the given key and value and returns it with its assigned
// offset and the record clock's timestamp (time.Now by default). Offsets define
// append order within this log; wall-clock timestamps may move backward.
func (l *Log) Append(key, value string) Record {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now
	if now == nil {
		now = time.Now
	}
	record := Record{Key: key, Value: value, Timestamp: now().UnixMilli()}
	record.Offset = int64(len(l.records))
	l.records = append(l.records, record)
	return record
}

// ReadFrom returns up to maxRecords records starting at offset, inclusive.
// Reading at the log-end offset returns an empty slice (the reader is caught
// up). The returned slice is a copy, so callers cannot modify the log.
// It returns an error wrapping ErrInvalidMaxRecords if maxRecords is less than
// one, or ErrOffsetOutOfRange if offset is negative or past the log-end offset.
//
// This implementation limits fetches by record count and returns immediately
// when caught up, simplifying Kafka's byte limits and long polling.
func (l *Log) ReadFrom(offset int64, maxRecords int) ([]Record, error) {
	records, _, err := l.ReadFromWithLEO(offset, maxRecords)
	return records, err
}

// ReadFromWithLEO returns copied records and the log-end offset from the same
// read lock. It has the same validation and ordering contract as ReadFrom.
func (l *Log) ReadFromWithLEO(offset int64, maxRecords int) ([]Record, int64, error) {
	if maxRecords < 1 {
		return nil, 0, fmt.Errorf("%w: got %d", ErrInvalidMaxRecords, maxRecords)
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	leo := int64(len(l.records)) // log-end offset
	if offset < 0 || offset > leo {
		return nil, 0, fmt.Errorf("%w: offset %d, log end offset %d", ErrOffsetOutOfRange, offset, leo)
	}

	// Bound the count before adding so even a maximal limit cannot overflow.
	count := min(int64(maxRecords), leo-offset)
	end := offset + count
	out := make([]Record, end-offset)
	copy(out, l.records[offset:end])
	return out, leo, nil
}

// NextOffset returns the log-end offset (LEO): the offset the next appended
// record will get. It is not the offset of the last record.
func (l *Log) NextOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return int64(len(l.records))
}

// Len returns the number of records in the log.
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.records)
}
