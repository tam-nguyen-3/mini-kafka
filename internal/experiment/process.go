// Package experiment runs a deterministic teaching scenario against independent
// minikafka OS processes. It never substitutes in-process broker storage.
package experiment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport/faultproxy"
)

const requestTimeout = 250 * time.Millisecond
const commandTimeout = 3 * time.Second

// Config selects the executable, retained logs, and optional live transcript.
// OutputDir must not already contain a transcript; an empty path creates one.
type Config struct {
	Binary, OutputDir string
	Writer            io.Writer
}
type buffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *buffer) Write(p []byte) (int, error) { b.mu.Lock(); defer b.mu.Unlock(); return b.b.Write(p) }
func (b *buffer) String() string              { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }

type child struct {
	cmd            *exec.Cmd
	stdout, stderr buffer
	done           chan struct{}
	err            error // published by closing done
	expected       bool  // runner thread only
}

func (c *child) stop(signal os.Signal) error {
	wasExpected := c.expected
	c.expected = true
	select {
	case <-c.done:
		if !wasExpected {
			return fmt.Errorf("unexpected exit pid=%d: %v", c.cmd.Process.Pid, c.err)
		}
		return nil
	default:
	}
	if err := c.cmd.Process.Signal(signal); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-c.done:
		if signal == syscall.SIGKILL {
			return nil
		}
		return c.err
	case <-timer.C:
		_ = c.cmd.Process.Kill()
		<-c.done
		return fmt.Errorf("process %d required forced cleanup", c.cmd.Process.Pid)
	}
}

type runner struct {
	cfg            Config
	out            io.Writer
	transcript     *os.File
	outputErr      error
	children       []*child
	reserved       []net.Listener
	proxyServers   []*http.Server
	proxies        []*faultproxy.Proxy
	controllerAddr string
	backendAddrs   []string
	targets        []broker.BrokerInfo
	brokers        []*child
	bc             *transport.HTTPBrokerClient
	mc             *transport.HTTPControllerClient
	keys           []string
	expected       map[int][]payload
}

// Run retains logs on success and failure. The caller owns its cancellation
// context; all subprocesses and proxy listeners are released before returning.
func Run(ctx context.Context, cfg Config) (err error) {
	r, err := newRunner(cfg)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, r.close(), r.outputErr)
		if err == nil {
			_, writeErr := fmt.Fprintln(cfgWriter(cfg), "PASS: all six scenarios and cleanup complete.")
			err = errors.Join(err, writeErr)
		}
		_, writeErr := fmt.Fprintf(cfgWriter(cfg), "Logs retained in %s\n", r.cfg.OutputDir)
		err = errors.Join(err, writeErr)
	}()
	if err = r.startCluster(ctx); err != nil {
		return err
	}
	if err = r.scenarios(ctx); err != nil {
		return err
	}
	if err = r.check(); err != nil {
		return err
	}
	r.printf("ASSERT: all six scenarios passed; cleaning up independent processes and proxies.\n")
	return nil
}
func cfgWriter(c Config) io.Writer {
	if c.Writer == nil {
		return io.Discard
	}
	return c.Writer
}
func newRunner(cfg Config) (*runner, error) {
	binary, err := filepath.Abs(cfg.Binary)
	if err != nil || cfg.Binary == "" {
		return nil, fmt.Errorf("--binary is required")
	}
	cfg.Binary = binary
	if cfg.OutputDir == "" {
		cfg.OutputDir, err = os.MkdirTemp("", "minikafka-demo-")
	} else {
		err = os.MkdirAll(cfg.OutputDir, 0755)
	}
	if err != nil {
		return nil, err
	}
	transcript, err := os.OpenFile(filepath.Join(cfg.OutputDir, "transcript.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	r := &runner{cfg: cfg, out: io.MultiWriter(cfgWriter(cfg), transcript), expected: make(map[int][]payload)}
	r.transcript = transcript
	r.bc, _ = transport.NewHTTPBrokerClient(requestTimeout)
	return r, nil
}
func (r *runner) printf(format string, args ...any) {
	if r.outputErr == nil {
		_, r.outputErr = fmt.Fprintf(r.out, format, args...)
	}
}
func (r *runner) check() error {
	if r.outputErr != nil {
		return r.outputErr
	}
	for _, c := range r.children {
		if c.expected {
			continue
		}
		select {
		case <-c.done:
			return fmt.Errorf("unexpected exit pid=%d: %v\n%s", c.cmd.Process.Pid, c.err, c.stderr.String())
		default:
		}
	}
	return nil
}
func (r *runner) await(ctx context.Context, what string, predicate func() bool) error {
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := deadline.Err(); err != nil {
			return fmt.Errorf("waiting for %s: %w", what, err)
		}
		if err := r.check(); err != nil {
			return err
		}
		if predicate() {
			return nil
		}
		select {
		case <-deadline.Done():
		case <-ticker.C:
		}
	}
}
func (r *runner) start(name string, args ...string) (*child, error) {
	logfile, err := os.OpenFile(filepath.Join(r.cfg.OutputDir, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	c := &child{cmd: exec.Command(r.cfg.Binary, args...), done: make(chan struct{})}
	c.cmd.Env = childEnv()
	c.cmd.Stdout = io.MultiWriter(logfile, &c.stdout)
	c.cmd.Stderr = io.MultiWriter(logfile, &c.stderr)
	c.cmd.WaitDelay = time.Second
	if err = c.cmd.Start(); err != nil {
		logfile.Close()
		return nil, err
	}
	r.children = append(r.children, c)
	r.printf("process %s pid=%d args=%q\n", name, c.cmd.Process.Pid, args)
	go func() { c.err = c.cmd.Wait(); _ = logfile.Close(); close(c.done) }()
	return c, nil
}
func (r *runner) reserve() (net.Listener, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err == nil {
		r.reserved = append(r.reserved, l)
	}
	return l, err
}
func (r *runner) startCluster(ctx context.Context) error {
	// Reserved ports are released immediately before their subprocess binds. A
	// collision is a startup failure, never permission to attach to another server.
	ctlSlot, err := r.reserve()
	if err != nil {
		return err
	}
	r.controllerAddr = ctlSlot.Addr().String()
	slots := make([]net.Listener, 3)
	for i := range slots {
		slots[i], err = r.reserve()
		if err != nil {
			return err
		}
		r.backendAddrs = append(r.backendAddrs, slots[i].Addr().String())
	}
	r.mc, err = transport.NewHTTPControllerClient(r.controllerAddr, requestTimeout)
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		proxy, err := faultproxy.New(r.backendAddrs[i], 2*time.Second)
		if err != nil {
			return err
		}
		r.proxies = append(r.proxies, proxy)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		server := &http.Server{Handler: proxy, ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 5 * time.Second}
		r.proxyServers = append(r.proxyServers, server)
		go func() { _ = server.Serve(listener) }()
		r.targets = append(r.targets, broker.BrokerInfo{ID: broker.BrokerID(i + 1), Addr: "http://" + listener.Addr().String()})
	}
	_ = ctlSlot.Close()
	ctl, err := r.start("controller", "controller", "--listen", r.controllerAddr)
	if err != nil {
		return err
	}
	if err = r.await(ctx, "controller readiness", func() bool {
		if !strings.Contains(ctl.stderr.String(), "ready listen=") {
			return false
		}
		_, err := r.mc.Metadata(ctx)
		return err == nil
	}); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		_ = slots[i].Close()
		child, err := r.startBroker(ctx, i, false)
		if err != nil {
			return err
		}
		r.brokers = append(r.brokers, child)
	}
	r.printf("cluster ready: controller=%s; advertised brokers=%v\n", r.controllerAddr, r.targets)
	return nil
}
func (r *runner) startBroker(ctx context.Context, i int, restart bool) (*child, error) {
	name := fmt.Sprintf("broker-%d", i+1)
	if restart {
		name += "-restart"
	}
	args := []string{"broker", "--id", fmt.Sprint(i + 1), "--listen", r.backendAddrs[i], "--advertise", r.targets[i].Addr, "--controller", r.controllerAddr}
	switch i {
	case 0:
		args = append(args, "--clock-step=-1s")
	case 1:
		args = append(args, "--clock-offset=1h")
	case 2:
		args = append(args, "--clock-offset=-1h")
	}
	c, err := r.start(name, args...)
	if err != nil {
		return nil, err
	}
	err = r.await(ctx, name+" readiness", func() bool {
		if !strings.Contains(c.stderr.String(), "ready listen=") {
			return false
		}
		_, err := r.bc.State(ctx, r.targets[i])
		return err == nil
	})
	return c, err
}
func (r *runner) close() error {
	var failures []error
	// Signal and drain children together so cleanup has one five-second
	// grace period rather than one full period per child.
	results := make(chan error, len(r.children))
	for _, c := range r.children {
		go func() { results <- c.stop(syscall.SIGTERM) }()
	}
	for range r.children {
		if err := <-results; err != nil {
			failures = append(failures, err)
		}
	}
	for _, server := range r.proxyServers {
		if err := server.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	for _, proxy := range r.proxies {
		proxy.Close()
	}
	for _, l := range r.reserved {
		_ = l.Close()
	}
	if r.transcript != nil {
		failures = append(failures, r.transcript.Close())
	}
	return errors.Join(failures...)
}

type commandResult struct{ stdout, stderr string }

func (r *runner) cli(ctx context.Context, want int, args ...string) (commandResult, error) {
	if err := ctx.Err(); err != nil {
		return commandResult{}, err
	}
	if err := r.check(); err != nil {
		return commandResult{}, err
	}
	args = append(args, "--controller", r.controllerAddr, "--timeout", requestTimeout.String())
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = "'" + strings.ReplaceAll(a, "'", "'\\''") + "'"
	}
	r.printf("$ minikafka %s\n", strings.Join(quoted, " "))
	commandCtx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(commandCtx, r.cfg.Binary, args...)
	cmd.Env = childEnv()
	cmd.WaitDelay = time.Second
	var out, diag bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &diag
	err := cmd.Run()
	result := commandResult{out.String(), diag.String()}
	r.printf("%s", result.stdout)
	if result.stderr != "" {
		r.printf("stderr: %s", result.stderr)
	}
	if commandCtx.Err() != nil {
		return result, fmt.Errorf("CLI watchdog: %w", commandCtx.Err())
	}
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			return result, err
		}
		code = exit.ExitCode()
	}
	if code != want {
		return result, fmt.Errorf("CLI exit %d, expected %d: %v", code, want, args)
	}
	return result, r.check()
}

// Race-instrumented children still report failures normally, but do not add
// the detector's default exit sleep to every one-shot CLI command.
func childEnv() []string {
	return append(os.Environ(), "GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
}
