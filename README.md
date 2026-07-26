# go_zmq_framework

**A Go port of [ruby_zmq_framework](https://github.com/pgdaniel/ruby_zmq_framework)
and its Zig sibling `zig_zmq_framework`: Node-RED without the UI, built on
goroutines, channels, and real interfaces instead of a mixin or comptime
tricks.**

- **Nodes** are independent OS processes. Each one does one job, lives in one
  file, and knows nothing about any other node — not their ports, not their
  names, not their language.
- **Wires** are pub/sub topics carrying JSON, over ZeroMQ.
- **The graph is data**: [`flow.yml`](flow.yml) is the only artifact that
  knows the topology. `flowctl` reads it, computes the wiring, and runs
  everything.
- **The contract is one page**: [`PROTOCOL.md`](PROTOCOL.md) is everything a
  node in any language needs to join — and it's the *same* contract as the
  Ruby and Zig originals, so nodes from any of the three repos can sit in
  one `flow.yml` together without any of them knowing the others exist.

The node contract is a plain Go interface (`Handler`, with one method,
`HandleMessage`), so a node that forgets it fails to compile at the
`Boot()` call site — no reflection, no runtime contract check needed.

## Quick start

You need Go 1.22+, cgo (a C compiler — this uses no external Go modules,
just a direct binding to libzmq), and the ZeroMQ library (`libzmq3-dev` on
Debian/Ubuntu, `brew install zeromq` on macOS), then:

```bash
go build -o bin/ ./cmd/...
bin/flowctl
```

That runs the demo graph from `flow.yml`: a simulated ECU blasting RPM data,
a telemetry node that commands a throttle cut on over-rev, a web dashboard
on <http://localhost:4567>, a state registry caching heartbeats and
telemetry, and a dashboard consumer syncing the registry's snapshot. Output
is streamed with a `[node_name]` prefix; Ctrl-C stops everything.

`bin/flowctl --plan` prints the computed wiring without running anything.
`bin/flowctl --graph` prints the node topology as JSON.

## Writing a node

A Go node is a struct with one method, booted from the environment:

```go
package main

import (
    "encoding/json"
    "fmt"

    "go_zmq_framework/framework"
)

// RpmSmoother publishes: engine_data_smooth. Subscribes: engine_data.
type RpmSmoother struct {
    bus    *framework.Bus
    window []int64
}

func (s *RpmSmoother) HandleMessage(topic string, payload framework.Payload) {
    if topic != "engine_data" {
        return
    }
    n, _ := payload["rpm"].(json.Number)
    rpm, _ := n.Int64()

    s.window = append(s.window, rpm)
    if len(s.window) > 5 {
        s.window = s.window[1:]
    }

    var sum int64
    for _, v := range s.window {
        sum += v
    }
    s.bus.Publish("engine_data_smooth", map[string]any{"rpm": sum / int64(len(s.window))})
}

func main() {
    _, _, err := framework.Boot(func(bus *framework.Bus) *RpmSmoother {
        return &RpmSmoother{bus: bus}
    })
    if err != nil {
        panic(err)
    }
    fmt.Println("online")
    framework.SleepForever()
}
```

Note what's absent: no ports, no peers, no subscribe calls. Wiring comes
from environment variables (`BUS_PORT`, `BUS_PEERS`, `BUS_SUBSCRIBES`,
`NODE_NAME` — see `PROTOCOL.md`), which `flowctl` computes from the node's
entry in the manifest:

```yaml
  rpm_smoother:
    cmd: bin/rpm_smoother
    subscribes: [engine_data]
    publishes: [engine_data_smooth]
```

(add `./cmd/rpm_smoother` to the build command to get it built.)

Run standalone (no environment needed — it binds an ephemeral port) to poke
at a node in isolation: `bin/rpm_smoother`.

Every node automatically heartbeats every 5 seconds. `framework.Boot` is
generic (`Boot[T Handler](newNode func(*Bus) T)`); if a node's constructor
needs more than the bus, close over the extra argument instead of adding a
special case — see `cmd/can_bridge/main.go`.

## Nodes in other languages

The bus is just two-frame ZeroMQ pub/sub — `[topic, json]` — and the whole
contract fits on one page: [`PROTOCOL.md`](PROTOCOL.md), including a
complete minimal Python node and the raw libzmq calls this framework's
`Boot()` makes under the hood. Follow it, add a `cmd` entry to `flow.yml`,
and the language never matters again — including the original
[ruby_zmq_framework](https://github.com/pgdaniel/ruby_zmq_framework) and
its [Zig](https://github.com/pgdaniel/zig_zmq_framework),
[Rust](https://github.com/pgdaniel/rust_zmq_framework), and
[Node](https://github.com/pgdaniel/node_zmq_framework) ports, which all
speak the exact same wire format.
[flow_viewer](https://github.com/pgdaniel/flow_viewer) can view and edit
any of their `flow.yml` files.

## What's in the box

| piece | file | job |
|-------|------|-----|
| `Bus` | `framework/bus.go` | ZeroMQ transport via a direct cgo binding to libzmq; a single actor goroutine owns all subscriber state and handler dispatch, so handlers on one bus never run concurrently — no lock needed, no recursive-mutex trick required |
| `Boot()` / `Handler` | `framework/framework.go` | the node contract as a plain Go interface, generic `Boot[T Handler]`, `NodeHandle.Broadcast`, env parsing, TERM/INT handling |
| `heartbeat.go` | `framework/heartbeat.go` | the 5-second heartbeat goroutine |
| `Flow` | `framework/flow.go` | parses `flow.yml`, computes each node's env wiring and the `--graph` topology — no YAML dependency, just a small hand-rolled parser for the subset flow.yml uses |
| `flowctl` | `cmd/flowctl/main.go` | assigns ports, spawns nodes, prefixes output, tears down on Ctrl-C |
| `StateRegistry` | `framework/state_registry.go` | passive cluster-state cache; replays snapshots on request |
| `CanBridge` | `framework/can_bridge.go` | real SocketCAN frames → `can_frame` topic (classic CAN, via the `syscall` package, no extra dependency) |
| demo nodes | `cmd/*/main.go` | one blackbox process per directory |

Delivery is fire-and-forget (latest-value-wins; slow consumers get
backpressure on the dispatch channel rather than the Ruby/Zig versions'
silent-drop-at-high-water-mark, since Go channels don't drop), and a bad
message or a panicking handler can never kill the dispatch goroutine
(`recover()` catches it, mirroring the originals' per-handler error
isolation). One real behavioral difference from Ruby/Zig: a handler that
calls `Publish` from within `HandleMessage` doesn't nest synchronously —
it enqueues, and runs after the current message finishes and the dispatch
loop cycles back around. Breadth-first instead of depth-first; see the
comment on `Bus` in `framework/bus.go` for why.

> **Note:** ZeroMQ is reached through a direct `cgo` binding to `libzmq`
> (`framework/bus.go`) — no third-party Go module, so this repo has an
> empty `go.mod` require list. The wire format is deliberately plain
> two-frame PUB/SUB, so swapping transports stays a contained change
> behind `Bus`'s interface (`Publish`/`Subscribe`/`Close`).

## CAN hardware

Uncomment the `can_bridge` node in `flow.yml` (set `CAN_IFACE`, e.g.
`vcan0`) to relay real SocketCAN frames onto the bus as `can_frame`
messages. Needs an actual or virtual CAN interface; fails fast if it
doesn't exist.

## Tests

```bash
go test ./...
# or, since this package leans on goroutines and channels:
go test ./... -race
```

Tests live in `framework/*_test.go`, mirroring the Ruby version's Minitest
suite and the Zig port's `test` blocks: bus dispatch, flow wiring/graph
computation, and StateRegistry's heartbeat/telemetry/snapshot behavior.
