package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/tam-nguyen-3/mini-kafka/internal/broker"
	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport"
	"github.com/tam-nguyen-3/mini-kafka/internal/transport/dashboard"
)

func serve(ctx context.Context, command string, o options, diag *output) error {
	bc, err := transport.NewHTTPBrokerClient(o.timeout)
	if err != nil {
		return err
	}
	var handler http.Handler
	var mc *transport.HTTPControllerClient
	logger := slog.New(slog.NewTextHandler(diag.w, nil))
	if command == "dashboard" {
		c, err := transport.NewHTTPControllerClient(o.controller, o.timeout)
		if err != nil {
			return usageError{err}
		}
		ui, err := dashboard.New(c, bc, o.timeout)
		if err != nil {
			return err
		}
		defer ui.Close()
		handler = ui
	} else if command == "controller" {
		core, err := controller.New(bc)
		if err != nil {
			return err
		}
		handler = transport.NewControllerServer(core)
	} else {
		o.advertise, err = transport.NormalizeAddress(o.advertise)
		if err != nil {
			return usageError{err}
		}
		mc, err = transport.NewHTTPControllerClient(o.controller, o.timeout)
		if err != nil {
			return usageError{err}
		}
		core, err := broker.NewWithClock(broker.BrokerID(o.id), recordClock(o.clockOffset, o.clockStep))
		if err != nil {
			return err
		}
		handler = transport.NewBrokerServerWithLogger(core, logger)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", o.listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	shutdown := func() error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return errors.Join(err, server.Close())
		}
		return nil
	}
	if mc != nil {
		if err := mc.RegisterBroker(ctx, broker.BrokerID(o.id), o.advertise); err != nil {
			closeErr := shutdown()
			serveErr := <-done
			if errors.Is(serveErr, http.ErrServerClosed) {
				serveErr = nil
			}
			return errors.Join(fmt.Errorf("register broker: %w", err), closeErr, serveErr)
		}
		diag.printf("broker %d ready listen=%q advertise=%q; in-memory storage starts empty; restart requires explicit reassignment and loses records\n", o.id, listener.Addr().String(), o.advertise)
	} else if command == "dashboard" {
		diag.printf("dashboard ready http://%s; browser sessions are temporary; cluster=%s\n", listener.Addr().String(), o.controller)
	} else {
		diag.printf("controller ready listen=%q; registry and metadata are in memory\n", listener.Addr().String())
	}
	if diag.err != nil {
		closeErr := shutdown()
		<-done
		return errors.Join(diag.err, closeErr)
	}
	select {
	case err := <-done:
		closeErr := server.Close()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		return errors.Join(err, closeErr)
	case <-ctx.Done():
		closeErr := shutdown()
		serveErr := <-done
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(closeErr, serveErr)
	}
}

// Only record timestamps are simulated; all lifecycle timers use real time.
func recordClock(offset, step time.Duration) func() time.Time {
	if step == 0 {
		return func() time.Time { return time.Now().Add(offset) }
	}
	var mu sync.Mutex
	next := time.Now().Add(offset)
	return func() time.Time { mu.Lock(); defer mu.Unlock(); value := next; next = next.Add(step); return value }
}
