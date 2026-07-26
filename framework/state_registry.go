package framework

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// StateRegistry is a passive, in-memory cache of cluster-wide state. It
// listens to heartbeats and telemetry broadcast by other nodes and, on
// request, replays its current snapshot back onto the bus. It never makes
// a blocking call and never crashes when a peer goes quiet — a silent
// node simply stops getting its active-node timestamp updated.
//
// HandleMessage is only ever invoked from the owning Bus's single dispatch
// goroutine, so it needs no locking against itself — but Snapshot is meant
// to be read from other goroutines too (see cmd/state_registry, which
// prints it on a timer), so the maps are still guarded by a mutex.
type StateRegistry struct {
	bus *Bus

	mu          sync.Mutex
	activeNodes map[string]NodeStatus
	telemetry   map[string]Payload
}

type NodeStatus struct {
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

func NewStateRegistry(bus *Bus) *StateRegistry {
	return &StateRegistry{
		bus:         bus,
		activeNodes: make(map[string]NodeStatus),
		telemetry:   make(map[string]Payload),
	}
}

func (r *StateRegistry) HandleMessage(topic string, payload Payload) {
	switch topic {
	case "heartbeat":
		r.handleHeartbeat(payload)
	case "request_global_state":
		r.replaySnapshot()
	default:
		r.handleTelemetry(topic, payload)
	}
}

func (r *StateRegistry) handleHeartbeat(payload Payload) {
	name, _ := payload["node_name"].(string)
	status, _ := payload["status"].(string)
	var ts int64
	if n, ok := payload["timestamp"].(json.Number); ok {
		ts, _ = n.Int64()
	}

	r.mu.Lock()
	r.activeNodes[name] = NodeStatus{Status: status, Timestamp: ts}
	r.mu.Unlock()
}

func (r *StateRegistry) handleTelemetry(topic string, payload Payload) {
	r.mu.Lock()
	r.telemetry[topic] = payload
	r.mu.Unlock()
}

func (r *StateRegistry) replaySnapshot() {
	active, telemetry := r.Snapshot()
	err := r.bus.Publish("global_state_snapshot", map[string]any{
		"active_nodes": active,
		"telemetry":    telemetry,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Framework Error] StateRegistry snapshot broadcast failed: %v\n", err)
	}
}

// Snapshot returns a defensive copy of the current store, safe to read or
// print from any goroutine.
func (r *StateRegistry) Snapshot() (activeNodes map[string]NodeStatus, telemetry map[string]Payload) {
	r.mu.Lock()
	defer r.mu.Unlock()

	activeNodes = make(map[string]NodeStatus, len(r.activeNodes))
	for k, v := range r.activeNodes {
		activeNodes[k] = v
	}
	telemetry = make(map[string]Payload, len(r.telemetry))
	for k, v := range r.telemetry {
		telemetry[k] = v
	}
	return activeNodes, telemetry
}
