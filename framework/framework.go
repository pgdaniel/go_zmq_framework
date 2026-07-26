package framework

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

// NodeHandle is what Boot returns: it owns the bus and the heartbeat
// goroutine. Node scripts call handle.Broadcast(...) the way a Ruby node
// calls self.broadcast(...).
type NodeHandle struct {
	Bus  *Bus
	Name string
	hb   *heartbeat
}

// Broadcast publishes and logs (rather than returns) any error, matching
// the fire-and-forget style the rest of the framework uses for broadcasts
// that aren't the caller's primary concern (e.g. a periodic tick).
func (h *NodeHandle) Broadcast(topic string, payload any) {
	if err := h.Bus.Publish(topic, payload); err != nil {
		fmt.Fprintf(os.Stderr, "[Framework Error] broadcast %s failed: %v\n", topic, err)
	}
}

// StopHeartbeat stops the heartbeat goroutine. Call before closing the bus
// this node broadcasts on.
func (h *NodeHandle) StopHeartbeat() {
	h.hb.stop()
}

// Boot wires a node the flow-runtime way: all bus wiring comes from
// environment variables (set by flowctl, or by hand), so node code never
// contains ports, peer lists, or subscription calls.
//
//	BUS_PORT        port to bind (default 0 = OS-assigned ephemeral)
//	BUS_PEERS       comma-separated peer endpoints ("127.0.0.1:5555,...")
//	BUS_SUBSCRIBES  comma-separated topics routed to node.HandleMessage
//	NODE_NAME       heartbeat identity (defaults to the node's Go type name)
//
// newNode is called with the freshly created bus and must return something
// satisfying Handler — for a node whose constructor needs more than the
// bus (CanBridge takes an interface name), close over the extra argument:
//
//	handle, bridge, err := framework.Boot(func(bus *framework.Bus) *framework.CanBridge {
//	    return framework.NewCanBridge(bus, os.Getenv("CAN_IFACE"), "can_frame")
//	})
//
// With no environment set, the node still boots standalone on an
// ephemeral port — handy for poking at a single node in isolation.
// Installs TERM/INT handlers that exit quietly, matching flowctl's
// supervised-process expectations.
func Boot[T Handler](newNode func(bus *Bus) T) (*NodeHandle, T, error) {
	var zero T

	port := envInt("BUS_PORT", 0)
	peers := envList("BUS_PEERS")
	subscribes := envList("BUS_SUBSCRIBES")

	bus, err := NewBus(port, peers, "127.0.0.1")
	if err != nil {
		return nil, zero, err
	}

	node := newNode(bus)

	name := os.Getenv("NODE_NAME")
	if name == "" {
		name = fmt.Sprintf("%T", node)
	}

	for _, topic := range subscribes {
		bus.Subscribe(topic, node)
	}

	hb := startHeartbeat(bus, name)
	installSignalHandlers()

	return &NodeHandle{Bus: bus, Name: name, hb: hb}, node, nil
}

func envInt(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envList(key string) []string {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// Booted nodes are processes managed by a supervisor (flowctl) or a
// terminal: exit quietly on TERM/INT instead of dumping a stack trace.
func installSignalHandlers() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-c
		os.Exit(0)
	}()
}

// SleepForever parks the calling goroutine while background goroutines
// (heartbeat, bus listener) do the work — the Go-native equivalent of
// Ruby's trailing bare `sleep`.
func SleepForever() {
	select {}
}
