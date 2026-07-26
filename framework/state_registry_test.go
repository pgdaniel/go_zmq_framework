package framework

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func decodePayload(t *testing.T, jsonStr string) Payload {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(jsonStr))
	dec.UseNumber()
	var p Payload
	if err := dec.Decode(&p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return p
}

func TestStateRegistryStartsEmptyAndTracksHeartbeatsAndTelemetry(t *testing.T) {
	bus, err := NewBus(0, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	reg := NewStateRegistry(bus)
	active, telemetry := reg.Snapshot()
	if len(active) != 0 || len(telemetry) != 0 {
		t.Fatalf("expected an empty store, got active=%v telemetry=%v", active, telemetry)
	}

	reg.HandleMessage("heartbeat", decodePayload(t, `{"node_name":"Foo","status":"ok","timestamp":123}`))
	active, _ = reg.Snapshot()
	if active["Foo"].Status != "ok" || active["Foo"].Timestamp != 123 {
		t.Fatalf("expected Foo ok/123, got %+v", active["Foo"])
	}

	reg.HandleMessage("engine_data", decodePayload(t, `{"rpm":4200}`))
	_, telemetry = reg.Snapshot()
	rpm, _ := telemetry["engine_data"]["rpm"].(json.Number)
	if v, _ := rpm.Int64(); v != 4200 {
		t.Fatalf("expected rpm 4200, got %v", telemetry["engine_data"]["rpm"])
	}
}

func TestStateRegistryRequestGlobalStateBroadcastsTheStore(t *testing.T) {
	bus, err := NewBus(0, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	reg := NewStateRegistry(bus)
	reg.HandleMessage("heartbeat", decodePayload(t, `{"node_name":"Foo","status":"ok","timestamp":123}`))

	rec := &recorder{}
	bus.Subscribe("global_state_snapshot", HandlerFunc(func(topic string, payload Payload) {
		activeNodes, _ := payload["active_nodes"].(map[string]any)
		if _, ok := activeNodes["Foo"]; ok {
			rec.mu.Lock()
			rec.seen = true
			rec.mu.Unlock()
		}
	}))

	reg.HandleMessage("request_global_state", decodePayload(t, `{"requester":"Dashboard"}`))

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		seen := rec.seen
		rec.mu.Unlock()
		if seen {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("never saw Foo in the broadcast global_state_snapshot")
}
