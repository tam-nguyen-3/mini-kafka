// minikafka exposes the learning cluster as independent server processes and
// small inspection commands. It is not compatible with Apache Kafka clients.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/client"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

type usageError struct{ error }

type uncertainProduce struct{ error }

func invalid(format string, args ...any) error { return usageError{fmt.Errorf(format, args...)} }

type output struct {
	w          io.Writer
	err        error
	timestamps bool
}

// Server logs can arrive concurrently with readiness/shutdown diagnostics.
// Track failures from flag help and slog as well as ordinary command output.
type checkedWriter struct {
	mu  sync.Mutex
	w   io.Writer
	err error
}

func (w *checkedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.w.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}
func (w *checkedWriter) failure() error { w.mu.Lock(); defer w.mu.Unlock(); return w.err }

func (o *output) printf(format string, args ...any) {
	if o.err == nil {
		_, o.err = fmt.Fprintf(o.w, format, args...)
	}
}
func (o *output) record(tp broker.TopicPartition, id broker.BrokerID, offset int64, key, value string) {
	o.printf("%q\t%d\t%d\t%d\t%q\t%q\n", tp.Topic, tp.Partition, id, offset, key, value)
}
func (o *output) storedRecord(tp broker.TopicPartition, id broker.BrokerID, r broker.Record) {
	if !o.timestamps {
		o.record(tp, id, r.Offset, r.Key, r.Value)
		return
	}
	o.printf("%q\t%d\t%d\t%d\t%q\t%q\t%d\n", tp.Topic, tp.Partition, id, r.Offset, r.Key, r.Value, r.Timestamp)
}
func (o *output) header() {
	o.printf("TOPIC\tPARTITION\tBROKER\tOFFSET\tKEY\tVALUE")
	if o.timestamps {
		o.printf("\tTIMESTAMP_MS")
	}
	o.printf("\n")
}

const usage = `Usage: minikafka COMMAND [flags]
Commands:
  controller --listen :9090
  broker --id ID --listen :9091 --advertise localhost:9091
  topic create NAME --partitions N
  produce TOPIC [--key KEY] [--value VALUE]  (otherwise read stdin lines)
  produce-bulk TOPIC [--count 100] [--keys 10]
  consume TOPIC [--partition N] [--from OFFSET] [--follow]
  dashboard --listen 127.0.0.1:8080
  describe
  dump --broker ID
Network commands accept --timeout 5s; clients and brokers accept --controller localhost:9090.
Use COMMAND --help for flags. Offsets order records only within a partition.
All data and consumer positions are in memory; restarting loses them.
`

func run(ctx context.Context, args []string, in io.Reader, stdout, stderr io.Writer) int {
	outWriter, diagWriter := &checkedWriter{w: stdout}, &checkedWriter{w: stderr}
	out, diag := &output{w: outWriter}, &output{w: diagWriter}
	err := dispatch(ctx, args, in, out, diag)
	if errors.Is(err, flag.ErrHelp) {
		err = nil
	}
	// A write failure must not be mistaken for an orderly cancellation.
	err = errors.Join(err, out.err, diag.err, outWriter.failure(), diagWriter.failure())
	if err == nil {
		return 0
	}
	if ctx.Err() != nil && cancellationOnly(err) && outWriter.failure() == nil && diagWriter.failure() == nil {
		return 0
	}
	diag.printf("error: %v\n", err)
	var u usageError
	if errors.As(err, &u) {
		return 2
	}
	return 1
}

// Cancellation is orderly only if it is the sole failure. In particular, a
// canceled in-flight append still needs its unknown-outcome diagnostic.
func cancellationOnly(err error) bool {
	if err == nil {
		return true
	}
	if _, ok := err.(uncertainProduce); ok {
		return false
	}
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range many.Unwrap() {
			if !cancellationOnly(e) {
				return false
			}
		}
		return true
	}
	if one, ok := err.(interface{ Unwrap() error }); ok {
		return cancellationOnly(one.Unwrap())
	}
	return err == context.Canceled
}

// flag stops at its first positional argument. Collect positional arguments
// separately so the documented topic-first examples work too. Flag values,
// including negative numbers and strings starting with '-', stay with flags.
func parse(fs *flag.FlagSet, args []string, want int) ([]string, error) {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if a == "-" || !strings.HasPrefix(a, "-") {
			positional = append(positional, a)
			continue
		}
		flags = append(flags, a)
		name, _, hasValue := strings.Cut(strings.TrimLeft(a, "-"), "=")
		f := fs.Lookup(name)
		if f == nil {
			if name == "h" || name == "help" {
				err := fs.Parse(flags)
				if err != nil && !errors.Is(err, flag.ErrHelp) {
					return nil, usageError{err}
				}
				return nil, err
			}
			return nil, invalid("unknown flag %s", a)
		}
		boolean := false
		if b, ok := f.Value.(interface{ IsBoolFlag() bool }); ok {
			boolean = b.IsBoolFlag()
		}
		if !hasValue && !boolean {
			if i+1 == len(args) {
				return nil, invalid("flag %s requires a value", a)
			}
			i++
			flags = append(flags, args[i])
		}
	}
	if err := fs.Parse(flags); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, err
		}
		return nil, usageError{err}
	}
	if len(positional) != want {
		return nil, invalid("%s expects %d positional argument(s); use --help", fs.Name(), want)
	}
	for _, p := range positional {
		if strings.TrimSpace(p) == "" {
			return nil, invalid("blank positional argument")
		}
	}
	return positional, nil
}
func visited(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

type options struct {
	controller, listen, advertise, key, value string
	timeout, interval                         time.Duration
	id, partitions, count, keys, partition    int
	from                                      int64
	follow                                    bool
	clockOffset, clockStep                    time.Duration
	timestamps                                bool
}

func dispatch(ctx context.Context, args []string, in io.Reader, out, diag *output) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		out.printf("%s", usage)
		return nil
	}
	command := args[0]
	args = args[1:]
	if command == "topic" {
		if len(args) == 0 {
			return invalid("expected topic create NAME --partitions N")
		}
		if args[0] == "--help" || args[0] == "-h" {
			out.printf("Usage: minikafka topic create NAME --partitions N [--controller ADDR] [--timeout 5s]\n")
			return nil
		}
		if args[0] != "create" {
			return invalid("unknown topic command %q", args[0])
		}
		command = "topic create"
		args = args[1:]
	}
	switch command {
	case "controller", "broker", "topic create", "produce", "produce-bulk", "consume", "describe", "dump", "dashboard":
	default:
		return invalid("unknown command %q", command)
	}
	var o options
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(diag.w)
	fs.Usage = func() {
		out.printf("Usage: minikafka %s [flags]", command)
		switch command {
		case "topic create", "produce", "produce-bulk", "consume":
			out.printf(" TOPIC")
		}
		out.printf("\n")
		fs.PrintDefaults()
		if command == "consume" {
			out.printf("One-shot reads independent startup LEO snapshots; --follow retains local positions.\nOffset errors require an explicit restart with --from. Timestamps imply no global order.\n")
		}
	}
	defaultTimeout := 5 * time.Second
	if command == "dashboard" {
		defaultTimeout = 2 * time.Second
	}
	fs.DurationVar(&o.timeout, "timeout", defaultTimeout, "positive timeout per HTTP request")
	if command != "controller" {
		fs.StringVar(&o.controller, "controller", "localhost:9090", "controller advertised address")
	}
	switch command {
	case "dashboard":
		fs.StringVar(&o.listen, "listen", "127.0.0.1:8080", "dashboard listener address")
	case "controller":
		fs.StringVar(&o.listen, "listen", ":9090", "listener bind address")
	case "broker":
		fs.DurationVar(&o.clockOffset, "clock-offset", 0, "simulation: shift record timestamps only")
		fs.DurationVar(&o.clockStep, "clock-step", 0, "simulation: nonzero fixed step per record, possibly negative")
		fs.IntVar(&o.id, "id", 0, "required broker ID (any integer)")
		fs.StringVar(&o.listen, "listen", "", "required listener bind address")
		fs.StringVar(&o.advertise, "advertise", "", "required reachable advertised destination")
	case "topic create":
		fs.IntVar(&o.partitions, "partitions", 0, "required positive partition count")
	case "produce":
		fs.StringVar(&o.key, "key", "", "record key (empty selects round-robin)")
		fs.StringVar(&o.value, "value", "", "single value; omit to read stdin lines up to 256 KiB")
	case "produce-bulk":
		fs.IntVar(&o.count, "count", 100, "positive record count")
		fs.IntVar(&o.keys, "keys", 10, "positive distinct key count")
	case "consume":
		fs.BoolVar(&o.timestamps, "timestamps", false, "append observational Unix-millisecond timestamps")
		fs.IntVar(&o.partition, "partition", -1, "one partition; omit for all")
		fs.Int64Var(&o.from, "from", 0, "starting offset for each partition")
		fs.BoolVar(&o.follow, "follow", false, "continue polling, including after partition failures")
		fs.DurationVar(&o.interval, "poll-interval", 500*time.Millisecond, "positive follow poll interval")
	case "dump":
		fs.BoolVar(&o.timestamps, "timestamps", false, "append observational Unix-millisecond timestamps")
		fs.IntVar(&o.id, "broker", 0, "required broker ID")
	}
	want := 0
	switch command {
	case "topic create", "produce", "produce-bulk", "consume":
		want = 1
	}
	pos, err := parse(fs, args, want)
	if err != nil {
		return err
	}
	if o.timeout <= 0 {
		return invalid("--timeout must be positive")
	}
	out.timestamps = o.timestamps
	switch command {
	case "controller", "broker", "dashboard":
		if strings.TrimSpace(o.listen) == "" {
			return invalid("--listen is required")
		}
		if command == "broker" && (!visited(fs, "id") || o.advertise == "") {
			return invalid("broker requires --id, --listen, and --advertise")
		}
		return serve(ctx, command, o, diag)
	case "topic create":
		if o.partitions <= 0 {
			return invalid("--partitions must be positive")
		}
	case "produce-bulk":
		if o.count <= 0 || o.keys <= 0 {
			return invalid("--count and --keys must be positive")
		}
	case "consume":
		if (visited(fs, "partition") && o.partition < 0) || o.from < 0 || o.interval <= 0 {
			return invalid("partition/from must be nonnegative and poll-interval positive")
		}
	case "dump":
		if !visited(fs, "broker") {
			return invalid("--broker is required")
		}
	}
	mc, err := transport.NewHTTPControllerClient(o.controller, o.timeout)
	if err != nil {
		return usageError{err}
	}
	bc, err := transport.NewHTTPBrokerClient(o.timeout)
	if err != nil {
		return err
	}
	switch command {
	case "topic create":
		if err := mc.CreateTopic(ctx, pos[0], o.partitions); err != nil {
			return fmt.Errorf("create: %w; partial assignment may remain; explicitly retry the same name/count", err)
		}
		out.printf("created topic %q with %d partitions\n", pos[0], o.partitions)
		return out.err
	case "produce", "produce-bulk":
		return produce(ctx, command, pos[0], o, visited(fs, "value"), in, mc, bc, out)
	case "consume":
		return consume(ctx, pos[0], o, mc, bc, out, diag)
	case "describe":
		return describe(ctx, mc, bc, out, diag)
	case "dump":
		return dump(ctx, broker.BrokerID(o.id), mc, bc, out, diag)
	}
	return nil
}

// observedProducer preserves the actual target used by Producer, including an
// ownership refresh, without exposing or duplicating its routing internals.
type observedProducer struct {
	*transport.HTTPBrokerClient
	target broker.BrokerInfo
}

func (b *observedProducer) Produce(ctx context.Context, target broker.BrokerInfo, tp broker.TopicPartition, key, value string) (int64, error) {
	b.target = target
	return b.HTTPBrokerClient.Produce(ctx, target, tp, key, value)
}

func produce(ctx context.Context, command, topic string, o options, hasValue bool, in io.Reader, mc *transport.HTTPControllerClient, bc *transport.HTTPBrokerClient, out *output) error {
	observed := &observedProducer{HTTPBrokerClient: bc}
	p, err := client.NewProducer(mc, observed)
	if err != nil {
		return err
	}
	out.header()
	if out.err != nil {
		return out.err
	}
	send := func(key, value string) error {
		partition, offset, err := p.Send(ctx, topic, key, value)
		if err != nil {
			// A structured server rejection establishes failure. Other failures after
			// partition selection may conceal an append, including malformed replies.
			var remote *transport.RemoteError
			if partition >= 0 && !errors.As(err, &remote) {
				return uncertainProduce{fmt.Errorf("produce %q partition %d: %w; outcome may be unknown; no automatic retry (an explicit retry may duplicate)", topic, partition, err)}
			}
			return err
		}
		out.record(broker.TopicPartition{Topic: topic, Partition: partition}, observed.target.ID, offset, key, value)
		return out.err
	}
	if command == "produce-bulk" {
		for i := 0; i < o.count; i++ {
			if err := send("key-"+strconv.Itoa(i%o.keys), "value-"+strconv.Itoa(i)); err != nil {
				return err
			}
		}
		return nil
	}
	if hasValue {
		return send(o.key, o.value)
	}
	// Reading arbitrary io.Readers cannot itself be canceled. Keep at most one
	// read pending; cancellation releases the command even if stdin is idle. The
	// process exits on signals, releasing the pending OS stdin read.
	type line struct {
		value string
		err   error
		done  bool
	}
	lines := make(chan line)
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		scanner := bufio.NewScanner(in)
		scanner.Buffer(make([]byte, 4096), 256*1024+2)
		for scanner.Scan() {
			v := scanner.Text()
			if len(v) > 256*1024 {
				select {
				case lines <- line{err: fmt.Errorf("stdin line exceeds 256 KiB"), done: true}:
				case <-readCtx.Done():
				}
				return
			}
			select {
			case lines <- line{value: v}:
			case <-readCtx.Done():
				return
			}
		}
		select {
		case lines <- line{err: scanner.Err(), done: true}:
		case <-readCtx.Done():
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case l := <-lines:
			if l.done {
				return l.err
			}
			if err := send(o.key, l.value); err != nil {
				return err
			}
		}
	}
}
