package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/client"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

// snapshotFetcher presents independent startup endpoints to Consumer. It also
// records the actual owner that served accepted records for CLI output. nil
// ends means follow mode. No log or core consumer state is changed.
type snapshotFetcher struct {
	base   client.FetchClient
	ends   map[broker.TopicPartition]int64
	owners map[broker.TopicPartition]broker.BrokerID
}

func (f *snapshotFetcher) Fetch(ctx context.Context, target broker.BrokerInfo, tp broker.TopicPartition, offset int64, max int) (broker.FetchResult, error) {
	end, snapshot := f.ends[tp]
	if snapshot {
		if offset > end {
			return broker.FetchResult{}, broker.ErrOffsetOutOfRange
		}
		if offset == end {
			return broker.FetchResult{Records: []broker.Record{}, LogEndOffset: end}, nil
		}
		if int64(max) > end-offset {
			max = int(end - offset)
		}
	}
	r, err := f.base.Fetch(ctx, target, tp, offset, max)
	if err != nil {
		return r, err
	}
	if err = validateBatch(r, offset, max); err != nil {
		return broker.FetchResult{}, err
	}
	if snapshot {
		if r.LogEndOffset < end {
			return broker.FetchResult{}, fmt.Errorf("%s: log shrank below startup LEO %d (now %d); in-memory data lost: %w", tp, end, r.LogEndOffset, broker.ErrOffsetOutOfRange)
		}
		r.LogEndOffset = end
	}
	f.owners[tp] = target.ID
	return r, nil
}

func consume(ctx context.Context, topic string, o options, mc *transport.HTTPControllerClient, bc *transport.HTTPBrokerClient, out, diag *output) error {
	m, byID, err := metadata(ctx, mc)
	if err != nil {
		return err
	}
	var selected []broker.PartitionMetadata
	for _, p := range m.Partitions {
		if p.Topic == topic && (o.partition < 0 || p.Partition == o.partition) {
			selected = append(selected, p)
		}
	}
	if len(selected) == 0 {
		return fmt.Errorf("%w: %q partition %d", broker.ErrUnknownTopicOrPartition, topic, o.partition)
	}
	fetcher := &snapshotFetcher{base: bc, owners: make(map[broker.TopicPartition]broker.BrokerID)}
	var active []broker.TopicPartition
	positions := make(map[broker.TopicPartition]int64)
	var failures []error
	if !o.follow {
		fetcher.ends = make(map[broker.TopicPartition]int64)
	}
	states := make(map[broker.BrokerID]stateSnapshot)
	for _, p := range selected {
		if !o.follow {
			s, ok := states[p.Leader]
			if !ok {
				s = readState(ctx, bc, byID[p.Leader])
				states[p.Leader] = s
			}
			end, exists := s.ends[p.TopicPartition]
			err := s.err
			if err == nil && !exists {
				err = fmt.Errorf("broker %d missing %s", p.Leader, p.TopicPartition)
			}
			if err == nil && o.from > end {
				err = fmt.Errorf("%s: --from %d exceeds LEO %d: %w", p.TopicPartition, o.from, end, broker.ErrOffsetOutOfRange)
			}
			if err != nil {
				diag.printf("consume %s: %v\n", p.TopicPartition, err)
				failures = append(failures, err)
				continue
			}
			fetcher.ends[p.TopicPartition] = end
			if o.from == end {
				continue
			}
		}
		active = append(active, p.TopicPartition)
		positions[p.TopicPartition] = o.from
	}
	consumer, err := client.NewConsumer(mc, fetcher)
	if err != nil {
		return err
	}
	if err = consumer.Assign(ctx, active...); err != nil {
		return err
	}
	for _, tp := range active {
		if err = consumer.Seek(ctx, tp, o.from); err != nil {
			return err
		}
	}
	out.header()
	lastErrors := make(map[broker.TopicPartition]string)
	for len(active) > 0 {
		if out.err != nil {
			return out.err
		}
		if diag.err != nil {
			return diag.err
		}
		result, pollErr := consumer.Poll(ctx, 100)
		for _, r := range result.Records {
			out.storedRecord(r.TopicPartition, fetcher.owners[r.TopicPartition], r.Record)
			positions[r.TopicPartition] = r.Offset + 1
		}
		failed := make(map[broker.TopicPartition]bool)
		for _, e := range result.PartitionErrors {
			failed[e.TopicPartition] = true
			if lastErrors[e.TopicPartition] != e.Error() {
				diag.printf("%v; no automatic position reset (restart with --from to reset)\n", e)
				lastErrors[e.TopicPartition] = e.Error()
			}
			if !o.follow {
				failures = append(failures, e)
			}
		}
		for _, tp := range active {
			if !failed[tp] && lastErrors[tp] != "" {
				diag.printf("partition %s recovered\n", tp)
				delete(lastErrors, tp)
			}
		}
		if out.err != nil {
			return out.err
		}
		if pollErr != nil {
			return errors.Join(append(failures, pollErr)...)
		}
		if !o.follow {
			remaining := make([]broker.TopicPartition, 0, len(active))
			for _, tp := range active {
				if !failed[tp] && positions[tp] < fetcher.ends[tp] {
					remaining = append(remaining, tp)
				}
			}
			active = remaining
			if err = consumer.Assign(ctx, active...); err != nil {
				return err
			}
		} else {
			timer := time.NewTimer(o.interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return errors.Join(append(failures, out.err)...)
}
