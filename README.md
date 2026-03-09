# distributed-task-scheduler

Production-grade distributed task scheduler with gRPC-style communication, consistent hashing, leader election, and fault-tolerant task execution.

## Author

**Cyber_Lord** — Vladimir120307@gmail.com  
GitHub: [vladimir120307-droid](https://github.com/vladimir120307-droid)

## Overview

`distributed-task-scheduler` is a horizontally-scalable task scheduling system written in Go. It coordinates work across multiple worker nodes using consistent hashing for deterministic task placement, a heartbeat-based leader election protocol for coordination, and automatic rebalancing when workers join or leave the cluster.

The scheduler supports DAG-based task dependencies, cron-like recurring schedules, priority-based execution ordering, and retry with exponential backoff + jitter.

## Architecture

```
                           ┌─────────────────────────────┐
                           │       Client / CLI          │
                           └──────────┬──────────────────┘
                                      │ Submit Task (RPC)
                                      ▼
               ┌──────────────────────────────────────────────┐
               │              Scheduler Cluster               │
               │                                              │
               │  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
               │  │ Leader   │  │ Follower │  │ Follower │   │
               │  │ (active) │◄─┤ (standby)│  │ (standby)│   │
               │  └────┬─────┘  └──────────┘  └──────────┘   │
               │       │ Heartbeat protocol                   │
               └───────┼──────────────────────────────────────┘
                       │
          ┌────────────┼────────────────┐
          │            │                │
          ▼            ▼                ▼
   ┌────────────┐┌────────────┐ ┌────────────┐
   │  Worker 1  ││  Worker 2  │ │  Worker N  │
   │            ││            │ │            │
   │ ┌────────┐ ││ ┌────────┐ │ │ ┌────────┐ │
   │ │Executor│ ││ │Executor│ │ │ │Executor│ │
   │ └────────┘ ││ └────────┘ │ │ └────────┘ │
   │ ┌────────┐ ││ ┌────────┐ │ │ ┌────────┐ │
   │ │ Health │ ││ │ Health │ │ │ │ Health │ │
   │ └────────┘ ││ └────────┘ │ │ └────────┘ │
   └────────────┘└────────────┘ └────────────┘
          │            │                │
          └────────────┼────────────────┘
                       ▼
              ┌────────────────┐
              │  Consistent    │
              │  Hashing Ring  │
              └────────────────┘
```

## Features

- **Consistent Hashing** — deterministic task-to-worker mapping with virtual nodes for even distribution
- **Leader Election** — heartbeat-based protocol; automatic failover when leader becomes unreachable
- **Priority Queue** — heap-backed queue with O(log n) insert/extract for task ordering
- **DAG Dependencies** — tasks can declare dependencies; scheduler resolves execution order via topological sort
- **Cron Scheduling** — built-in cron expression parser supporting second-level granularity
- **Retry with Backoff** — exponential backoff with jitter to avoid thundering herd on transient failures
- **Health Checking** — periodic worker liveness probes with configurable thresholds
- **Graceful Shutdown** — SIGINT/SIGTERM handling with in-flight task draining
- **Task State Machine** — well-defined states: `pending → running → completed | failed → retrying → running`

## Getting Started

### Prerequisites

- Go 1.21 or later

### Build

```bash
# Build scheduler
go build -o bin/scheduler ./cmd/scheduler

# Build worker
go build -o bin/worker ./cmd/worker
```

### Run

```bash
# Start the scheduler (becomes leader if first in cluster)
./bin/scheduler --config configs/default.yaml

# Start workers (registers with scheduler automatically)
./bin/worker --scheduler-addr localhost:9090 --worker-port 9100
./bin/worker --scheduler-addr localhost:9090 --worker-port 9101
./bin/worker --scheduler-addr localhost:9090 --worker-port 9102
```

### Configuration

See `configs/default.yaml` for all available options:

```yaml
scheduler:
  listen_addr: ":9090"
  heartbeat_interval: 2s
  election_timeout: 10s
  rebalance_interval: 30s

worker:
  concurrency: 4
  health_check_interval: 5s
  task_timeout: 300s

storage:
  backend: "memory"

hash_ring:
  virtual_nodes: 150
  replication_factor: 2

retry:
  max_retries: 3
  base_delay: 1s
  max_delay: 60s
  multiplier: 2.0
```

### API Usage (RPC)

```go
package main

import (
    "net/rpc"
    "log"

    pb "github.com/vladimir120307-droid/distributed-task-scheduler/internal/proto"
)

func main() {
    client, err := rpc.DialHTTP("tcp", "localhost:9090")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Submit a task
    req := &pb.SubmitTaskRequest{
        Task: &pb.TaskDefinition{
            ID:       "batch-job-001",
            Name:     "data-export",
            Payload:  []byte(`{"table":"users","format":"csv"}`),
            Priority: 10,
            CronExpr: "0 */5 * * * *",
        },
    }

    var resp pb.SubmitTaskResponse
    err = client.Call("SchedulerService.SubmitTask", req, &resp)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("task submitted: %s (status: %s)", resp.TaskID, resp.Status)

    // Query task status
    statusReq := &pb.TaskStatusRequest{TaskID: "batch-job-001"}
    var statusResp pb.TaskStatusResponse
    err = client.Call("SchedulerService.GetTaskStatus", statusReq, &statusResp)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("task %s: state=%s, worker=%s", statusResp.TaskID, statusResp.State, statusResp.AssignedWorker)
}
```

### Submit a DAG (dependent tasks)

```go
dag := &pb.SubmitDAGRequest{
    Tasks: []*pb.TaskDefinition{
        {ID: "extract", Name: "extract-data", Payload: []byte(`{}`)},
        {ID: "transform", Name: "transform-data", Payload: []byte(`{}`), Dependencies: []string{"extract"}},
        {ID: "load", Name: "load-data", Payload: []byte(`{}`), Dependencies: []string{"transform"}},
    },
}

var dagResp pb.SubmitDAGResponse
err = client.Call("SchedulerService.SubmitDAG", dag, &dagResp)
```

## Testing

```bash
go test ./tests/... -v -count=1
go test ./internal/... -v -count=1
go test -race ./...
```

## Project Structure

```
cmd/              — entry points for scheduler and worker binaries
internal/         — core logic (not importable by external projects)
  scheduler/      — task scheduling, priority queue, leader election
  worker/         — task execution, health reporting
  hash/           — consistent hashing ring implementation
  storage/        — pluggable storage backends (in-memory, extensible)
  proto/          — RPC message definitions (structs)
pkg/              — shared utilities importable by external code
  config/         — YAML configuration loader
  logger/         — structured logging
  retry/          — exponential backoff with jitter
configs/          — default YAML configuration files
tests/            — integration tests
```

## License

MIT License — see [LICENSE](LICENSE) for details.

## Contributing

Contributions, issues and feature requests are welcome!

## License

This project is MIT licensed.
