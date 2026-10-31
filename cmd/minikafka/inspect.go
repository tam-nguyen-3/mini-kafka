package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/client"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

func metadata(ctx context.Context, mc *transport.HTTPControllerClient) (broker.ClusterMetadata, map[broker.BrokerID]broker.BrokerInfo, error) {
	m, err := mc.Metadata(ctx)
	if err != nil {
		return m, nil, err
	}
	byID := make(map[broker.BrokerID]broker.BrokerInfo)
	for _, b := range m.Brokers {
		if _, ok := byID[b.ID]; ok {
			return m, nil, client.ErrInvalidMetadata
		}
		addr, err := transport.NormalizeAddress(b.Addr)
		if err != nil {
			return m, nil, err
		}
		b.Addr = addr
		byID[b.ID] = b
	}
	seen := make(map[broker.TopicPartition]bool)
	counts := make(map[string]int)
	for _, p := range m.Partitions {
		if _, ok := byID[p.Leader]; !ok || seen[p.TopicPartition] || strings.TrimSpace(p.Topic) == "" || p.Partition < 0 {
			return m, nil, client.ErrInvalidMetadata
		}
		seen[p.TopicPartition] = true
		counts[p.Topic]++
	}
	for topic, count := range counts {
		for i := 0; i < count; i++ {
			if !seen[broker.TopicPartition{Topic: topic, Partition: i}] {
				return m, nil, client.ErrInvalidMetadata
			}
		}
	}
	sort.Slice(m.Brokers, func(i, j int) bool { return m.Brokers[i].ID < m.Brokers[j].ID })
	sort.Slice(m.Partitions, func(i, j int) bool {
		a, b := m.Partitions[i], m.Partitions[j]
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})
	return m, byID, nil
}

type stateSnapshot struct {
	ends map[broker.TopicPartition]int64
	err  error
}

func readState(ctx context.Context, bc *transport.HTTPBrokerClient, target broker.BrokerInfo) stateSnapshot {
	states, err := bc.State(ctx, target)
	s := stateSnapshot{ends: make(map[broker.TopicPartition]int64), err: err}
	if err != nil {
		return s
	}
	for _, p := range states {
		if _, ok := s.ends[p.TopicPartition]; ok || p.LogEndOffset < 0 || p.Partition < 0 || p.Topic == "" {
			s.err = client.ErrInvalidMetadata
			return s
		}
		s.ends[p.TopicPartition] = p.LogEndOffset
	}
	return s
}
func describe(ctx context.Context, mc *transport.HTTPControllerClient, bc *transport.HTTPBrokerClient, out, diag *output) error {
	m, byID, err := metadata(ctx, mc)
	if err != nil {
		return err
	}
	states := make(map[broker.BrokerID]stateSnapshot)
	var failures []error
	for _, b := range m.Brokers {
		s := readState(ctx, bc, byID[b.ID])
		states[b.ID] = s
		if s.err != nil {
			diag.printf("broker %d: %v\n", b.ID, s.err)
			failures = append(failures, s.err)
		}
	}
	out.printf("TOPIC\tPARTITION\tBROKER\tADDR\tLEO\n")
	assigned := make(map[broker.BrokerID]bool)
	for _, p := range m.Partitions {
		assigned[p.Leader] = true
		s := states[p.Leader]
		leo := "unavailable"
		if s.err == nil {
			if end, ok := s.ends[p.TopicPartition]; ok {
				leo = fmt.Sprint(end)
			} else {
				leo = "missing"
				failures = append(failures, fmt.Errorf("broker %d missing %s", p.Leader, p.TopicPartition))
			}
		}
		out.printf("%q\t%d\t%d\t%q\t%s\n", p.Topic, p.Partition, p.Leader, byID[p.Leader].Addr, leo)
	}
	for _, b := range m.Brokers {
		if !assigned[b.ID] {
			status := "no published partitions"
			if states[b.ID].err != nil {
				status += "; unavailable"
			}
			out.printf("broker %d\t%q\t%s\n", b.ID, b.Addr, status)
		}
	}
	return errors.Join(append(failures, out.err)...)
}
func dump(ctx context.Context, id broker.BrokerID, mc *transport.HTTPControllerClient, bc *transport.HTTPBrokerClient, out, diag *output) error {
	_, byID, err := metadata(ctx, mc)
	if err != nil {
		return err
	}
	target, ok := byID[id]
	if !ok {
		return fmt.Errorf("unknown broker %d", id)
	}
	s := readState(ctx, bc, target)
	if s.err != nil {
		return s.err
	}
	tps := make([]broker.TopicPartition, 0, len(s.ends))
	for tp := range s.ends {
		tps = append(tps, tp)
	}
	sortTPs(tps)
	out.header()
	var failures []error
	for _, tp := range tps {
		end := s.ends[tp]
		out.printf("partition %q/%d broker=%d LEO=%d\n", tp.Topic, tp.Partition, id, end)
		fetcher := &snapshotFetcher{base: bc, ends: s.ends, owners: make(map[broker.TopicPartition]broker.BrokerID)}
		for offset := int64(0); offset < end; {
			if ctx.Err() != nil {
				return errors.Join(append(failures, ctx.Err())...)
			}
			result, err := fetcher.Fetch(ctx, target, tp, offset, 100)
			if err == nil {
				err = validateBatch(result, offset, 100)
			}
			if err != nil {
				diag.printf("dump %s: %v\n", tp, err)
				failures = append(failures, err)
				break
			}
			for _, r := range result.Records {
				out.storedRecord(tp, id, r)
			}
			if out.err != nil {
				return out.err
			}
			offset += int64(len(result.Records))
		}
	}
	return errors.Join(append(failures, out.err)...)
}
func sortTPs(tps []broker.TopicPartition) {
	sort.Slice(tps, func(i, j int) bool {
		if tps[i].Topic != tps[j].Topic {
			return tps[i].Topic < tps[j].Topic
		}
		return tps[i].Partition < tps[j].Partition
	})
}
func validateBatch(r broker.FetchResult, offset int64, max int) error {
	if len(r.Records) > max || r.LogEndOffset < offset || int64(len(r.Records)) > r.LogEndOffset-offset {
		return client.ErrInvalidFetchResponse
	}
	if len(r.Records) == 0 && offset < r.LogEndOffset {
		return client.ErrInvalidFetchResponse
	}
	for i, v := range r.Records {
		if v.Offset != offset+int64(i) {
			return client.ErrInvalidFetchResponse
		}
	}
	return nil
}
