package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/scheduler"
	"github.com/vladimir120307-droid/distributed-task-scheduler/internal/storage"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/config"
	"github.com/vladimir120307-droid/distributed-task-scheduler/pkg/logger"
)

func main() {
	var (
		configPath string
		listenAddr string
	)

	flag.StringVar(&configPath, "config", "configs/default.yaml", "path to configuration file")
	flag.StringVar(&listenAddr, "listen", "", "override scheduler listen address")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	if listenAddr != "" {
		cfg.Scheduler.ListenAddr = listenAddr
	}

	jsonFormat := cfg.Logging.Format == "json"
	log := logger.New(logger.ParseLevel(cfg.Logging.Level), jsonFormat)
	log = log.WithField("component", "scheduler")

	log.Info("distributed-task-scheduler starting")
	log.Info("listen=%s heartbeat=%s election_timeout=%s",
		cfg.Scheduler.ListenAddr,
		cfg.Scheduler.HeartbeatInterval,
		cfg.Scheduler.ElectionTimeout,
	)

	var store storage.Store
	switch cfg.Storage.Backend {
	case "memory":
		store = storage.NewMemoryStore(cfg.Storage.RetentionPeriod)
		log.Info("using in-memory storage (retention=%s)", cfg.Storage.RetentionPeriod)
	default:
		log.Fatal("unsupported storage backend: %s", cfg.Storage.Backend)
	}

	sched := scheduler.NewScheduler(cfg, log, store)

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
	if err := sched.Start(ctx); err != nil {
		log.Error("scheduler exited with error: %v", err)
		os.Exit(1)
	}

	log.Info("scheduler stopped (uptime=%s)", time.Since(startTime))
}
