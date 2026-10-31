// minikafka-demo owns the local subprocesses and fault proxies used by demo.sh.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tam-nguyen-3/mini-kafka/internal/experiment"
)

func main() {
	var cfg experiment.Config
	flag.StringVar(&cfg.Binary, "binary", "", "path to a built minikafka executable")
	flag.StringVar(&cfg.OutputDir, "output-dir", "", "new log directory (default: generated temporary directory)")
	flag.Parse()
	if cfg.Binary == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	cfg.Writer = os.Stdout
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := experiment.Run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}
