package framework

import (
	"strings"
	"testing"
)

const flowSpec = `nodes:
  ecu:
    cmd: ruby nodes/ecu.rb
    publishes: [engine_data]
    subscribes: [throttle_request]

  telemetry:
    cmd: ruby nodes/telemetry.rb
    publishes: [throttle_request]
    subscribes: [engine_data]

  registry:
    cmd: ruby nodes/state_registry.rb
    subscribes: [heartbeat, engine_data]
    env: { VERBOSE: "1" }
`

func envValue(env []EnvPair, key string) (string, bool) {
	for _, p := range env {
		if p.Key == key {
			return p.Value, true
		}
	}
	return "", false
}

func TestWiringPeersAreComputedFromTopicPublishers(t *testing.T) {
	flow, err := ParseFlow(flowSpec)
	if err != nil {
		t.Fatalf("ParseFlow: %v", err)
	}
	wiring, err := flow.Wiring(map[string]int{"ecu": 5001, "telemetry": 5002, "registry": 5003})
	if err != nil {
		t.Fatalf("Wiring: %v", err)
	}

	byName := map[string][]EnvPair{}
	for _, w := range wiring {
		byName[w.NodeName] = w.Env
	}

	if v, _ := envValue(byName["ecu"], "BUS_PEERS"); v != "127.0.0.1:5002" {
		t.Errorf("ecu peers = %q, want 127.0.0.1:5002", v)
	}
	if v, _ := envValue(byName["telemetry"], "BUS_PEERS"); v != "127.0.0.1:5001" {
		t.Errorf("telemetry peers = %q, want 127.0.0.1:5001", v)
	}
}

func TestWiringHeartbeatMakesEveryNodeAPublisherExceptYourself(t *testing.T) {
	flow, err := ParseFlow(flowSpec)
	if err != nil {
		t.Fatalf("ParseFlow: %v", err)
	}
	wiring, err := flow.Wiring(map[string]int{"ecu": 5001, "telemetry": 5002, "registry": 5003})
	if err != nil {
		t.Fatalf("Wiring: %v", err)
	}
	var registryEnv []EnvPair
	for _, w := range wiring {
		if w.NodeName == "registry" {
			registryEnv = w.Env
		}
	}
	peers, _ := envValue(registryEnv, "BUS_PEERS")
	if !strings.Contains(peers, "5001") || !strings.Contains(peers, "5002") {
		t.Errorf("registry peers = %q, want to contain both 5001 and 5002", peers)
	}
}

func TestWiringEachNodeGetsItsOwnPortNameAndSubscriptions(t *testing.T) {
	flow, err := ParseFlow(flowSpec)
	if err != nil {
		t.Fatalf("ParseFlow: %v", err)
	}
	wiring, err := flow.Wiring(map[string]int{"ecu": 5001, "telemetry": 5002, "registry": 5003})
	if err != nil {
		t.Fatalf("Wiring: %v", err)
	}
	byName := map[string][]EnvPair{}
	for _, w := range wiring {
		byName[w.NodeName] = w.Env
	}

	if v, _ := envValue(byName["ecu"], "BUS_PORT"); v != "5001" {
		t.Errorf("ecu BUS_PORT = %q, want 5001", v)
	}
	if v, _ := envValue(byName["ecu"], "NODE_NAME"); v != "ecu" {
		t.Errorf("ecu NODE_NAME = %q, want ecu", v)
	}
	if v, _ := envValue(byName["registry"], "BUS_SUBSCRIBES"); v != "heartbeat,engine_data" {
		t.Errorf("registry BUS_SUBSCRIBES = %q, want heartbeat,engine_data", v)
	}
}

func TestWiringCustomEnvIsMergedIn(t *testing.T) {
	flow, err := ParseFlow(flowSpec)
	if err != nil {
		t.Fatalf("ParseFlow: %v", err)
	}
	wiring, err := flow.Wiring(map[string]int{"ecu": 5001, "telemetry": 5002, "registry": 5003})
	if err != nil {
		t.Fatalf("Wiring: %v", err)
	}
	var registryEnv []EnvPair
	for _, w := range wiring {
		if w.NodeName == "registry" {
			registryEnv = w.Env
		}
	}
	if v, ok := envValue(registryEnv, "VERBOSE"); !ok || v != "1" {
		t.Errorf("registry VERBOSE = %q (ok=%v), want 1", v, ok)
	}
}

func TestANodeWithoutCmdIsRejected(t *testing.T) {
	spec := "nodes:\n  broken:\n    publishes: [x]\n"
	_, err := ParseFlow(spec)
	if err == nil || !strings.Contains(err.Error(), "broken needs a cmd") {
		t.Fatalf("expected a 'broken needs a cmd' error, got %v", err)
	}
}

func TestAManifestWithoutNodesIsRejected(t *testing.T) {
	_, err := ParseFlow("not_nodes: {}\n")
	if err == nil {
		t.Fatal("expected an error for a manifest without a nodes key")
	}
}

func TestGraphHasOneEdgePerPublisherAndIgnoresHeartbeat(t *testing.T) {
	flow, err := ParseFlow(flowSpec)
	if err != nil {
		t.Fatalf("ParseFlow: %v", err)
	}
	g := flow.Graph()

	found := map[string]bool{}
	for _, e := range g.Edges {
		found[e.From+">"+e.To+":"+e.Topic] = true
		if e.Topic == "heartbeat" {
			t.Errorf("heartbeat should never appear as an edge, got %+v", e)
		}
	}
	if !found["ecu>telemetry:engine_data"] {
		t.Errorf("expected an ecu->telemetry engine_data edge, got %+v", g.Edges)
	}
	if !found["telemetry>ecu:throttle_request"] {
		t.Errorf("expected a telemetry->ecu throttle_request edge, got %+v", g.Edges)
	}
}

func TestGraphSurfacesUnpublishedTopicsAsUnresolved(t *testing.T) {
	spec := "nodes:\n  lonely:\n    cmd: \"true\"\n    subscribes: [ghost_topic]\n"
	flow, err := ParseFlow(spec)
	if err != nil {
		t.Fatalf("ParseFlow: %v", err)
	}
	g := flow.Graph()

	if len(g.Edges) != 0 {
		t.Errorf("expected no edges, got %+v", g.Edges)
	}
	if len(g.Unresolved) != 1 || g.Unresolved[0].Topic != "ghost_topic" || g.Unresolved[0].To != "lonely" {
		t.Errorf("expected one unresolved ghost_topic->lonely, got %+v", g.Unresolved)
	}
}
