package experiment

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/client"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport/faultproxy"
)

type payload struct{ key, value string }
type row struct {
	partition, id     int
	offset, timestamp int64
	payload
}

func rows(text string) ([]row, error) {
	var result []row
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if !strings.HasPrefix(line, `"orders"`+"\t") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 6 && len(fields) != 7 {
			return nil, fmt.Errorf("bad record row %q", line)
		}
		p, e1 := strconv.Atoi(fields[1])
		id, e2 := strconv.Atoi(fields[2])
		offset, e3 := strconv.ParseInt(fields[3], 10, 64)
		key, e4 := strconv.Unquote(fields[4])
		value, e5 := strconv.Unquote(fields[5])
		var timestamp int64
		var e6 error
		if len(fields) == 7 {
			timestamp, e6 = strconv.ParseInt(fields[6], 10, 64)
		}
		if err := errors.Join(e1, e2, e3, e4, e5, e6); err != nil {
			return nil, err
		}
		result = append(result, row{partition: p, id: id, offset: offset, timestamp: timestamp, payload: payload{key, value}})
	}
	return result, nil
}
func (r *runner) send(ctx context.Context, p int, value string) error {
	out, err := r.cli(ctx, 0, "produce", "orders", "--key", r.keys[p], "--value", value)
	if err != nil {
		return err
	}
	records, err := rows(out.stdout)
	if err != nil {
		return err
	}
	if len(records) != 1 || records[0].partition != p || records[0].id != p%3+1 || records[0].offset != int64(len(r.expected[p])) || records[0].payload != (payload{r.keys[p], value}) {
		return fmt.Errorf("unexpected produce acknowledgment: %s", out.stdout)
	}
	r.expected[p] = append(r.expected[p], payload{r.keys[p], value})
	return nil
}
func (r *runner) records(ctx context.Context, p int) (broker.FetchResult, error) {
	return r.bc.Fetch(ctx, r.targets[p%3], broker.TopicPartition{Topic: "orders", Partition: p}, 0, 1000)
}
func (r *runner) assertAll(ctx context.Context) error {
	total := 0
	for i, target := range r.targets {
		states, err := r.bc.State(ctx, target)
		if err != nil {
			return err
		}
		if len(states) != 2 {
			return fmt.Errorf("broker %d hosts %d partitions, expected two", target.ID, len(states))
		}
		for _, s := range states {
			if s.Topic != "orders" || s.Partition%3 != i {
				return fmt.Errorf("unexpected owner-local state: %+v", s)
			}
			records, err := r.records(ctx, s.Partition)
			if err != nil {
				return err
			}
			expected := r.expected[s.Partition]
			if s.LogEndOffset != int64(len(expected)) || records.LogEndOffset != s.LogEndOffset || len(records.Records) != len(expected) {
				return fmt.Errorf("unexpected LEO/count: %+v", s)
			}
			for j, record := range records.Records {
				if record.Offset != int64(j) || (payload{record.Key, record.Value}) != expected[j] {
					return fmt.Errorf("partition %d record %d mismatch: %+v", s.Partition, j, record)
				}
			}
			total += len(expected)
		}
	}
	result, err := r.cli(ctx, 0, "consume", "orders")
	if err != nil {
		return err
	}
	got, err := rows(result.stdout)
	if err != nil {
		return err
	}
	if len(got) != total {
		return fmt.Errorf("consumed %d records, expected %d", len(got), total)
	}
	next := make(map[int]int)
	for _, record := range got {
		p := record.partition
		i := next[p]
		if p < 0 || p >= 6 || record.id != p%3+1 || i >= len(r.expected[p]) || record.offset != int64(i) || record.payload != r.expected[p][i] {
			return fmt.Errorf("CLI order/payload mismatch: %+v", record)
		}
		next[p]++
	}
	r.printf("ASSERT: %d records accounted for, dense offsets and payload order within each partition.\n", total)
	return nil
}
func (r *runner) table(ctx context.Context, want int) error {
	_, err := r.cli(ctx, want, "describe")
	return err
}
func (r *runner) scenarios(ctx context.Context) error {
	r.printf("\n1. Healthy distribution: one owner and ordered offsets per partition.\n")
	if _, err := r.cli(ctx, 0, "topic", "create", "orders", "--partitions", "6"); err != nil {
		return err
	}
	r.keys = make([]string, 6)
	partitioner := &client.DefaultPartitioner{}
	for i, found := 0, 0; found < 6 && i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		p := partitioner.Partition(key, 6)
		if r.keys[p] == "" {
			r.keys[p] = key
			found++
		}
	}
	for _, key := range r.keys {
		if key == "" {
			return fmt.Errorf("could not select keys for all partitions")
		}
	}
	metadata, err := r.mc.Metadata(ctx)
	if err != nil {
		return err
	}
	if len(metadata.Brokers) != 3 || len(metadata.Partitions) != 6 {
		return fmt.Errorf("unexpected cluster metadata: %+v", metadata)
	}
	for _, p := range metadata.Partitions {
		if p.Leader != broker.BrokerID(p.Partition%3+1) {
			return fmt.Errorf("unexpected assignment: %+v", p)
		}
	}
	for round := 0; round < 2; round++ {
		for p := 0; p < 6; p++ {
			if err := r.send(ctx, p, fmt.Sprintf("initial-p%d-%d", p, round)); err != nil {
				return err
			}
		}
	}
	if err = r.table(ctx, 0); err != nil {
		return err
	}
	if err = r.assertAll(ctx); err != nil {
		return err
	}
	for p := 0; p < 6; p++ {
		if _, err = r.cli(ctx, 0, "consume", "orders", "--partition", fmt.Sprint(p)); err != nil {
			return err
		}
	}
	if _, err = r.cli(ctx, 0, "dump", "--broker", "1"); err != nil {
		return err
	}
	_, err = r.bc.Fetch(ctx, r.targets[0], broker.TopicPartition{Topic: "orders", Partition: 1}, 0, 1)
	if !errors.Is(err, broker.ErrUnknownTopicOrPartition) {
		return fmt.Errorf("unowned fetch was not rejected: %v", err)
	}
	if _, err = r.bc.Produce(ctx, r.targets[0], broker.TopicPartition{Topic: "orders", Partition: 1}, "", "unowned"); !errors.Is(err, broker.ErrUnknownTopicOrPartition) {
		return fmt.Errorf("unowned append was not rejected: %v", err)
	}
	r.printf("ASSERT: broker 1 rejects partition 1; no storage was created.\n")
	if err = r.networkFaults(ctx); err != nil {
		return err
	}
	if err = r.restart(ctx, metadata); err != nil {
		return err
	}
	if err = r.clocks(ctx); err != nil {
		return err
	}
	return r.assertAll(ctx)
}
func (r *runner) networkFaults(ctx context.Context) error {
	proxy := r.proxies[0]
	fetch := faultproxy.Operation{Method: "GET", Path: "/fetch"}
	produce := faultproxy.Operation{Method: "POST", Path: "/produce"}
	r.printf("\n2. Delay: broker 1 fetches wait 1s; CLI requests time out after 250ms. Healthy partitions continue.\n")
	before := proxy.Snapshot()[fetch]
	if err := proxy.SetRule(faultproxy.Rule{Operation: fetch, Mode: faultproxy.Delay, Delay: time.Second}); err != nil {
		return err
	}
	partial, err := r.cli(ctx, 1, "consume", "orders")
	if err != nil {
		return err
	}
	got, err := rows(partial.stdout)
	if err != nil {
		return err
	}
	if len(got) != 8 || !strings.Contains(partial.stderr, "deadline") {
		return fmt.Errorf("expected eight healthy records and deadline errors: %+v", partial)
	}
	if err = r.send(ctx, 1, "healthy-during-delay"); err != nil {
		return err
	}
	if _, err = r.cli(ctx, 0, "consume", "orders", "--partition", "1"); err != nil {
		return err
	}
	if err = r.await(ctx, "delayed requests canceled", func() bool { return proxy.Snapshot()[fetch].Canceled-before.Canceled == 2 }); err != nil {
		return err
	}
	after := proxy.Snapshot()[fetch]
	if after.Forwarded != before.Forwarded {
		return fmt.Errorf("canceled delayed fetch was forwarded")
	}
	r.printf("ASSERT: delayed fetches matched=%d canceled=%d forwarded=%d.\n", after.Matched-before.Matched, after.Canceled-before.Canceled, after.Forwarded-before.Forwarded)
	proxy.Clear()
	if err = r.assertAll(ctx); err != nil {
		return err
	}
	if err = r.table(ctx, 0); err != nil {
		return err
	}
	r.printf("\n3. Connection failure before forwarding: no broker append; client conservatively reports an unknown outcome.\n")
	before = proxy.Snapshot()[produce]
	previous := len(r.expected[0])
	if err = proxy.SetRule(faultproxy.Rule{Operation: produce, Mode: faultproxy.Disconnect}); err != nil {
		return err
	}
	failed, err := r.cli(ctx, 1, "produce", "orders", "--key", r.keys[0], "--value", "never-forwarded")
	if err != nil {
		return err
	}
	after = proxy.Snapshot()[produce]
	if after.Requests-before.Requests != 1 || after.Forwarded != before.Forwarded || !strings.Contains(failed.stderr, "outcome may be unknown") {
		return fmt.Errorf("pre-forward fault was retried or misreported: %+v", failed)
	}
	r.printf("ASSERT: connection failure requests=%d forwarded=%d; no automatic retry.\n", after.Requests-before.Requests, after.Forwarded-before.Forwarded)
	records, err := r.records(ctx, 0)
	if err != nil {
		return err
	}
	if len(records.Records) != previous {
		return fmt.Errorf("disconnected produce appended")
	}
	if err = r.send(ctx, 2, "healthy-during-disconnect"); err != nil {
		return err
	}
	if _, err = r.cli(ctx, 0, "consume", "orders", "--partition", "2"); err != nil {
		return err
	}
	proxy.Clear()
	if err = r.table(ctx, 0); err != nil {
		return err
	}
	r.printf("\n4. Lost response after append: exactly one stored record despite CLI failure; no automatic retry.\n")
	before = proxy.Snapshot()[produce]
	if err = proxy.SetRule(faultproxy.Rule{Operation: produce, Mode: faultproxy.DropResponse}); err != nil {
		return err
	}
	failed, err = r.cli(ctx, 1, "produce", "orders", "--key", r.keys[0], "--value", "response-lost")
	if err != nil {
		return err
	}
	after = proxy.Snapshot()[produce]
	if after.Requests-before.Requests != 1 || after.Forwarded-before.Forwarded != 1 || after.Dropped-before.Dropped != 1 || !strings.Contains(failed.stderr, "outcome may be unknown") {
		return fmt.Errorf("ambiguous append was retried or not reported: %+v", failed)
	}
	proxy.Clear()
	records, err = r.records(ctx, 0)
	if err != nil {
		return err
	}
	if len(records.Records) != previous+1 || records.Records[previous].Value != "response-lost" {
		return fmt.Errorf("lost-response append not stored exactly once")
	}
	r.printf("ASSERT: lost response requests=%d forwarded=%d dropped=%d; stored once.\n", after.Requests-before.Requests, after.Forwarded-before.Forwarded, after.Dropped-before.Dropped)
	r.expected[0] = append(r.expected[0], payload{r.keys[0], "response-lost"})
	if err = r.assertAll(ctx); err != nil {
		return err
	}
	return r.table(ctx, 0)
}
func (r *runner) restart(ctx context.Context, original broker.ClusterMetadata) error {
	r.printf("\n5. Broker death/restart: metadata has no automatic failover; volatile records disappear.\n")
	if err := r.send(ctx, 0, "checkpoint-before-crash"); err != nil {
		return err
	}
	args := []string{"consume", "orders", "--follow", "--poll-interval", "20ms", "--controller", r.controllerAddr, "--timeout", requestTimeout.String()}
	follower, err := r.start("consumer-follow", args...)
	if err != nil {
		return err
	}
	if err = r.await(ctx, "consumer checkpoint", func() bool { return strings.Contains(follower.stdout.String(), `"checkpoint-before-crash"`) }); err != nil {
		return err
	}
	oldPosition := len(r.expected[0])
	if err = r.brokers[0].stop(syscall.SIGKILL); err != nil {
		return err
	}
	if err = r.await(ctx, "unavailable partition diagnostic", func() bool { return strings.Contains(follower.stderr.String(), "unavailable") }); err != nil {
		return err
	}
	if err = r.send(ctx, 1, "healthy-after-crash"); err != nil {
		return err
	}
	if err = r.await(ctx, "healthy follow delivery", func() bool { return strings.Contains(follower.stdout.String(), `"healthy-after-crash"`) }); err != nil {
		return err
	}
	partial, err := r.cli(ctx, 1, "consume", "orders")
	if err != nil {
		return err
	}
	if !strings.Contains(partial.stdout, `"healthy-after-crash"`) {
		return fmt.Errorf("healthy partitions did not continue after crash")
	}
	if err = r.table(ctx, 1); err != nil {
		return err
	}
	restored, err := r.startBroker(ctx, 0, true)
	if err != nil {
		return err
	}
	r.brokers[0] = restored
	state, err := r.bc.State(ctx, r.targets[0])
	if err != nil {
		return err
	}
	if len(state) != 0 {
		return fmt.Errorf("restarted broker unexpectedly retained assignments")
	}
	if err = r.table(ctx, 1); err != nil {
		return err
	}
	for _, p := range original.Partitions {
		if p.Leader == 1 {
			if err = r.bc.AssignPartition(ctx, r.targets[0], p.TopicPartition); err != nil {
				return err
			}
		}
	}
	state, err = r.bc.State(ctx, r.targets[0])
	if err != nil {
		return err
	}
	if len(state) != 2 {
		return fmt.Errorf("explicit reassignment failed")
	}
	for _, s := range state {
		if s.LogEndOffset != 0 {
			return fmt.Errorf("restart retained records: %+v", s)
		}
	}
	current, err := r.mc.Metadata(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, original) {
		return fmt.Errorf("ownership changed without explicit failover")
	}
	if err = r.await(ctx, "old consumer position rejected", func() bool { return strings.Contains(follower.stderr.String(), "offset_out_of_range") }); err != nil {
		return err
	}
	r.printf("ASSERT: running consumer retained position %d and reported offset_out_of_range before any replacement writes.\n", oldPosition)
	if _, err = r.cli(ctx, 1, "consume", "orders", "--partition", "0", "--from", fmt.Sprint(oldPosition)); err != nil {
		return err
	}
	if err = follower.stop(syscall.SIGTERM); err != nil {
		return err
	}
	r.printf("Follow transcript:\n%s\nFollow diagnostics:\n%s\n", follower.stdout.String(), follower.stderr.String())
	r.expected[0] = nil
	r.expected[3] = nil
	reset, err := r.cli(ctx, 0, "consume", "orders", "--partition", "0", "--from", "0")
	if err != nil {
		return err
	}
	empty, err := rows(reset.stdout)
	if err != nil || len(empty) != 0 {
		return fmt.Errorf("reset should initially read an empty log")
	}
	if err = r.send(ctx, 0, "fresh-after-reset"); err != nil {
		return err
	}
	fresh, err := r.cli(ctx, 0, "consume", "orders", "--partition", "0", "--from", "0")
	if err != nil {
		return err
	}
	got, err := rows(fresh.stdout)
	if err != nil {
		return err
	}
	if len(got) != 1 || got[0].offset != 0 || got[0].value != "fresh-after-reset" {
		return fmt.Errorf("explicit reset did not recover fresh offset zero")
	}
	return r.table(ctx, 0)
}
func (r *runner) clocks(ctx context.Context) error {
	r.printf("\n6. Clock independence: timestamps describe observations, never offset order or cross-partition causality.\n")
	if err := r.send(ctx, 0, "clock-back-1"); err != nil {
		return err
	}
	if err := r.send(ctx, 0, "clock-back-2"); err != nil {
		return err
	}
	result, err := r.records(ctx, 0)
	if err != nil {
		return err
	}
	a, b := result.Records[len(result.Records)-2], result.Records[len(result.Records)-1]
	if b.Offset != a.Offset+1 || b.Timestamp != a.Timestamp-1000 {
		return fmt.Errorf("backward clock did not preserve offset order: %+v %+v", a, b)
	}
	if err = r.send(ctx, 1, "clock-ahead-first"); err != nil {
		return err
	}
	ahead, err := r.records(ctx, 1)
	if err != nil {
		return err
	}
	if err = r.send(ctx, 2, "clock-behind-later"); err != nil {
		return err
	}
	behind, err := r.records(ctx, 2)
	if err != nil {
		return err
	}
	if behind.Records[len(behind.Records)-1].Timestamp >= ahead.Records[len(ahead.Records)-1].Timestamp {
		return fmt.Errorf("cross-broker clock disagreement not visible")
	}
	if _, err = r.cli(ctx, 0, "consume", "orders", "--timestamps"); err != nil {
		return err
	}
	if _, err = r.cli(ctx, 0, "dump", "--broker", "1", "--timestamps"); err != nil {
		return err
	}
	r.printf("ASSERT: same-partition offsets rise while timestamps fall; a later broker operation has an earlier timestamp. No global record order is claimed.\n")
	return r.table(ctx, 0)
}
