package experiment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var buildOnce sync.Once
var binaryPath, buildDir string
var buildErr error

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}
func testBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("subprocess scenarios skipped with -short")
	}
	buildOnce.Do(func() {
		buildDir, buildErr = os.MkdirTemp("", "minikafka-process-binary-")
		if buildErr != nil {
			return
		}
		binaryPath = filepath.Join(buildDir, "minikafka")
		_, source, _, _ := runtime.Caller(0)
		root := filepath.Join(filepath.Dir(source), "../..")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		args := []string{"build"}
		if raceEnabled {
			args = append(args, "-race")
		}
		args = append(args, "-o", binaryPath, "./cmd/minikafka")
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = root
		output, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("build CLI: %w\n%s", err, output)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binaryPath
}
func attachLogs(t *testing.T, r *runner) {
	t.Helper()
	t.Logf("logs retained: %s", r.cfg.OutputDir)
	if !t.Failed() {
		return
	}
	entries, _ := os.ReadDir(r.cfg.OutputDir)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".log") {
			data, _ := os.ReadFile(filepath.Join(r.cfg.OutputDir, entry.Name()))
			t.Logf("%s:\n%s", entry.Name(), data)
		}
	}
}
func assertClosed(t *testing.T, r *runner) {
	t.Helper()
	pids := make(map[int]bool)
	for _, c := range r.children {
		pid := c.cmd.Process.Pid
		if pids[pid] {
			t.Errorf("PID reused within scenario: %d", pid)
		}
		pids[pid] = true
		select {
		case <-c.done:
		default:
			t.Errorf("unreaped child %d", pid)
		}
	}
	addresses := append([]string{r.controllerAddr}, r.backendAddrs...)
	for _, target := range r.targets {
		addresses = append(addresses, strings.TrimPrefix(target.Addr, "http://"))
	}
	for _, addr := range addresses {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Errorf("listener still open: %s", addr)
		}
	}
}
func TestMultiProcessScenarios(t *testing.T) {
	cfg := Config{Binary: testBinary(t), Writer: io.Discard}
	r, err := newRunner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer attachLogs(t, r)
	// Cleanup even when a scenario assertion or startup fails.
	defer func() {
		if err := r.close(); err != nil {
			t.Error(err)
		}
		assertClosed(t, r)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err = r.startCluster(ctx); err != nil {
		t.Fatal(err)
	}
	if len(r.children) != 4 {
		t.Fatalf("expected separate controller and three broker processes: %d", len(r.children))
	}
	if err = r.scenarios(ctx); err != nil {
		t.Fatal(err)
	}
	if err = r.check(); err != nil {
		t.Fatal(err)
	}
}

type cancelOnReady struct {
	buffer
	cancel context.CancelFunc
}

func (w *cancelOnReady) Write(p []byte) (int, error) {
	n, err := w.buffer.Write(p)
	if strings.Contains(string(p), "cluster ready:") {
		w.cancel()
	}
	return n, err
}
func TestCancellationCleansRunningCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	output := &cancelOnReady{cancel: cancel}
	err := Run(ctx, Config{Binary: testBinary(t), Writer: output})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected canceled run, got %v", err)
	}
	text := output.String()
	if strings.Contains(text, "PASS:") {
		t.Error("canceled run reported success")
	}
	pids := regexp.MustCompile(`(?m)^process \S+ pid=(\d+)`).FindAllStringSubmatch(text, -1)
	if len(pids) != 4 {
		t.Fatalf("expected four started children: %s", text)
	}
	for _, match := range pids {
		pid, _ := strconv.Atoi(match[1])
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("child %d still exists: %v", pid, err)
		}
	}
	for _, addr := range regexp.MustCompile(`127\.0\.0\.1:\d+`).FindAllString(text, -1) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Errorf("listener still open: %s", addr)
		}
	}
	dirs := regexp.MustCompile(`(?m)^Logs retained in (.+)$`).FindStringSubmatch(text)
	if len(dirs) == 2 {
		defer attachLogs(t, &runner{cfg: Config{OutputDir: dirs[1]}})
	} else {
		t.Errorf("missing retained log location: %s", text)
	}
}
