package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/worker"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/config"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/logger"
)

func main() {
	var (
		configPath    string
		schedulerAddr string
		workerPort    int
		workerID      string
	)

	flag.StringVar(&configPath, "config", "configs/default.yaml", "path to configuration file")
	flag.StringVar(&schedulerAddr, "scheduler-addr", "localhost:9090", "scheduler RPC address")
	flag.IntVar(&workerPort, "worker-port", 9100, "port for this worker to listen on")
	flag.StringVar(&workerID, "worker-id", "", "unique worker identifier (auto-generated if empty)")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	jsonFormat := cfg.Logging.Format == "json"
	log := logger.New(logger.ParseLevel(cfg.Logging.Level), jsonFormat)

	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	log = log.WithField("component", "worker").WithField("worker_id", workerID)

	listenAddr := fmt.Sprintf(":%d", workerPort)

	log.Info("worker starting")
	log.Info("scheduler=%s listen=%s concurrency=%d",
		schedulerAddr,
		listenAddr,
		cfg.Worker.Concurrency,
	)

	w := worker.NewWorker(workerID, cfg, log, schedulerAddr, listenAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("received signal %s, initiating shutdown", sig)
		cancel()
	}()

	startTime := time.Now()
	if err := w.Start(ctx); err != nil {
		log.Error("worker exited with error: %v", err)
		os.Exit(1)
	}

	log.Info("worker stopped (uptime=%s)", time.Since(startTime))
}
