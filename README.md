# Mini-Kafka

A from-scratch Go project for learning distributed messaging. Mini-Kafka splits topics into ordered partitions stored by independent broker processes. A controller manages partition ownership; producers and consumers communicate directly with the owning brokers over HTTP/JSON.

**Current status:** distribution and partition ordering are implemented, with CLI inspection and repeatable failure experiments. Storage is in memory. Replication, automatic failover, disk persistence, and consumer groups are planned. This project uses its own protocol and is not compatible with Apache Kafka clients.

## Requirements

- Go **1.27 or newer**, as specified in [go.mod](go.mod).
- A POSIX shell for the automated demo; the examples below use macOS/Linux shell syntax.
- Permission to start local processes and bind loopback ports.

The application uses the Go standard library. No Docker, Kafka installation, or external Go dependencies are required.

## Quick start: run the distributed demo

From the repository root:

```sh
./scripts/demo.sh
```

The script builds temporary binaries and launches one controller and three broker **OS processes**, each on its own port. A Go experiment runner injects faults through local HTTP proxies, checks the results, and shuts down its child processes on completion or interruption. Ports are selected automatically.

Watch the printed process IDs, partition ownership tables, record offsets, and assertions. Expected failed requests are part of the experiment. A successful run ends with:

```text
PASS: all six scenarios and cleanup complete.
```

The runner prints a directory containing retained logs and a transcript. See the [experiment guide](docs/demo.md) for the full procedure and expected results.

## Run and inspect a cluster manually

Run all commands from the repository root. Build the CLI once:

```sh
go build -o /tmp/minikafka ./cmd/minikafka
```

Use four terminals for the servers. Start the controller first, then wait for each server's `ready` message. Ports 9090-9093 must be available. If they are occupied, choose unused ports and update the corresponding `--listen`, `--advertise`, and `--controller` addresses throughout the examples.

**Terminal 1: controller**

```sh
/tmp/minikafka controller --listen 127.0.0.1:9090
```

**Terminals 2, 3, and 4: one broker per terminal**

```sh
# Terminal 2
/tmp/minikafka broker --id 1 --listen 127.0.0.1:9091 --advertise 127.0.0.1:9091 --controller 127.0.0.1:9090
```

```sh
# Terminal 3
/tmp/minikafka broker --id 2 --listen 127.0.0.1:9092 --advertise 127.0.0.1:9092 --controller 127.0.0.1:9090
```

```sh
# Terminal 4
/tmp/minikafka broker --id 3 --listen 127.0.0.1:9093 --advertise 127.0.0.1:9093 --controller 127.0.0.1:9090
```

In a **fifth terminal**, create a topic after all brokers are ready:

```sh
/tmp/minikafka topic create orders --partitions 6 --controller 127.0.0.1:9090
/tmp/minikafka describe --controller 127.0.0.1:9090
```

Broker 1 should own partitions 0 and 3, broker 2 partitions 1 and 4, and broker 3 partitions 2 and 5. Each initially has a log-end offset (LEO) of 0: the offset of its next append.

Produce two records with the same key and read them back:

```sh
/tmp/minikafka produce orders --key order-42 --value created --controller 127.0.0.1:9090
/tmp/minikafka produce orders --key order-42 --value paid --controller 127.0.0.1:9090
/tmp/minikafka consume orders --controller 127.0.0.1:9090
```

Both records should have the same partition and offsets 0 and 1. Keys select stable partitions while the partition count stays fixed. Reading records does not remove them.

Send more data and inspect broker-local storage:

```sh
/tmp/minikafka produce-bulk orders --count 12 --keys 6 --controller 127.0.0.1:9090
/tmp/minikafka describe --controller 127.0.0.1:9090
/tmp/minikafka dump --broker 1 --controller 127.0.0.1:9090
/tmp/minikafka consume orders --partition 0 --from 0 --controller 127.0.0.1:9090
```

`describe` lists ownership and LEOs; `dump` shows records stored on one broker. Record tables contain `TOPIC`, `PARTITION`, `BROKER`, `OFFSET`, `KEY`, and `VALUE`. Offsets start at zero independently in each partition. Add `--timestamps` to `consume` or `dump` to inspect stored wall-clock timestamps.

To watch new records as they arrive:

```sh
/tmp/minikafka consume orders --follow --from 0 --controller 127.0.0.1:9090
```

Produce another record from a separate client terminal. Stop the consumer and each server with Ctrl-C. **Restarting a broker loses its records and assignments.** Re-registration alone does not restore them; use the automated demo to see explicit reassignment and consumer reset.

Commands accept `--help`; network requests accept a positive `--timeout` (normally `5s`). One-shot consumption reads to each partition's independently captured startup LEO; it is not an atomic cluster-wide snapshot. A partial failure can print healthy records and still exit nonzero.

## Architecture and code map

```text
                 Controller :9090
              registry and ownership
                /      |      \
           Broker 1 Broker 2 Broker 3
             :9091    :9092    :9093
              P0,3     P1,4     P2,5
                ^       ^       ^
                +--- Clients ---+
             direct produce / fetch
          (metadata from the controller)
```

Each broker owns a private in-memory log for each assigned partition. The controller handles metadata and assignment, not record traffic. Core broker, controller, and client logic depends on interfaces; HTTP adapters live in transport.

| Location | Responsibility |
| --- | --- |
| [internal/broker](internal/broker) | Records, append-only logs, partition identity, ownership checks, and state snapshots |
| [internal/controller](internal/controller) | Broker registration, topic creation, partition placement, and metadata |
| [internal/client](internal/client) | Key partitioning, producer routing, consumer positions, and polling |
| [internal/protocol](internal/protocol) | Shared JSON request/response types and error codes |
| [internal/transport](internal/transport) | HTTP servers/clients, fault proxies, and the experimental browser dashboard |
| [cmd/minikafka](cmd/minikafka) | CLI commands, process lifecycle, and inspection output |
| [internal/experiment](internal/experiment) | Multi-process orchestration and failure assertions, launched through [cmd/minikafka-demo](cmd/minikafka-demo) |
| [scripts/demo.sh](scripts/demo.sh) | Builds and launches the automated experiment |

For a first code review, read:

1. [The log](internal/broker/log.go) and [its tests](internal/broker/log_test.go): append order, copied reads, and concurrency.
2. [The broker](internal/broker/broker.go) and [controller](internal/controller/controller.go): local storage versus cluster metadata and assignment.
3. [The HTTP server adapters](internal/transport/server.go): how network requests reach the core types.
4. [The producer](internal/client/producer.go) and [consumer](internal/client/consumer.go): routing, positions, ordering, and partial failures.
5. [The CLI](cmd/minikafka/main.go) and [experiment scenarios](internal/experiment/scenarios.go): how to operate and exercise the whole system.

Tests sit beside implementations as `*_test.go`. The [technical breakdown](breakdown.md) explains the algorithms, interfaces, and design decisions in more detail.

## Failures and current guarantees

The automated demo runs six scenarios:

| Scenario | What to observe |
| --- | --- |
| Healthy distribution | Six partitions on three brokers; ordered offsets and rejection of unowned partitions |
| Network delay | Selected fetches delayed by 1 second exceed the 250 ms client deadline; healthy partitions remain usable |
| Connection failure | A connection closes before forwarding a produce; other brokers continue operating |
| Lost response | An append succeeds but its reply is dropped; the client reports an unknown outcome without retrying |
| Broker crash/restart | A real broker process receives `SIGKILL`; healthy partitions continue, and explicit restart/reassignment exposes lost data |
| Clock disagreement | Simulated record clocks disagree or move backward while partition offsets remain ordered |

The shell script launches the experiments; Go code injects network faults and controls process crashes. Clock simulation changes record timestamps, not the OS clock or timeout timers.

The current system provides **failure isolation and recovery from temporary network faults**. It does not replicate data or automatically move a failed broker's partitions to another broker. Clearing a network fault allows subsequent requests to succeed; restarting an in-memory broker cannot recover its old records. The controller is also a single in-memory process.

Ordering exists only within a partition. There is no global record order, and timestamps do not establish one. Consumer positions are local and volatile; no groups or committed offsets exist yet. A lost response may hide a successful append, so retrying a produce manually can create duplicates. Exactly-once delivery is not promised.

A live learning dashboard is being developed under the `dashboard` command. Its validation is pending in [PLAN.md](PLAN.md); the completed CLI and automated demo are the primary walkthroughs above.

## Build and test

```sh
go build ./...
go test ./...
go test -race ./...
go vet ./...
```

The default tests include real HTTP requests and separate-process failure scenarios. The race suite also builds race-instrumented child binaries. For a shorter run:

```sh
go test -short ./...
```

Short mode skips subprocess scenarios but still needs local sockets for HTTP tests. If an environment blocks the default Go caches, set `GOCACHE` and `GOMODCACHE` to writable temporary directories. Tests and demos require permission to bind loopback ports. Race detection checks exercised memory access; the integration assertions check distributed behavior.

## Further documentation

- [Technical breakdown](breakdown.md): concepts, implementation details, protocol, and source/test reading guide.
- [Experiment guide](docs/demo.md): fault controls, expected observations, and recovery limitations.
- [Project plan](PLAN.md): implementation status, decisions, and the schedule for replication/failover, disk persistence, and consumer groups.
- [Progress report](output/pdf/mini-kafka-progress-report.pdf) and [editable source](docs/progress-report.md): the October 31 checkpoint; the screen-recording link is pending.
- [Diagram sources](docs/diagrams): editable Graphviz DOT diagrams; rendering is optional.
