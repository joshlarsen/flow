package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "metric-mcp" {
		if err := serveMetricMCP(os.Stdin, os.Stdout); err != nil {
			log.Fatal(err)
		}
		return
	}
	if (len(os.Args) > 1 && os.Args[1] == "metric") || filepath.Base(os.Args[0]) == "flow-metric" {
		arguments := os.Args[1:]
		if filepath.Base(os.Args[0]) != "flow-metric" {
			arguments = os.Args[2:]
		}
		if len(arguments) == 0 {
			log.Fatal("usage: flow-metric namespace.key=value [namespace.key=value ...]")
		}
		if err := emitMetricCLI(arguments); err != nil {
			log.Fatal(err)
		}
		return
	}
	config, err := loadRuntimeConfig()
	if err != nil {
		log.Fatal(err)
	}
	supervisor := NewSupervisor(config, os.Stdout)
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", config.Port),
		Handler:           supervisor.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	signals, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	serverError := make(chan error, 1)
	go func() { serverError <- server.ListenAndServe() }()

	select {
	case <-signals.Done():
		supervisor.Stop()
		select {
		case <-supervisor.done:
		case <-time.After(time.Duration(config.ShutdownGraceMS)*time.Millisecond + 30*time.Second):
		}
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server failed: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Duration(config.ShutdownGraceMS)*time.Millisecond)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
