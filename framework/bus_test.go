package framework

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestBusBindsAnEphemeralPortAndReportsItBack(t *testing.T) {
	bus, err := NewBus(0, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	if bus.Port == 0 {
		t.Fatalf("expected a nonzero bound port")
	}
}

type recorder struct {
	mu   sync.Mutex
	seen bool
	rpm  int64
}

func (r *recorder) HandleMessage(topic string, payload Payload) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = true
	if n, ok := payload["rpm"].(json.Number); ok {
		v, _ := n.Int64()
		r.rpm = v
	}
}

func TestPublishDispatchesLocallyToSubscribers(t *testing.T) {
	bus, err := NewBus(0, nil, "127.0.0.1")
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	defer bus.Close()

	rec := &recorder{}
	bus.Subscribe("engine_data", rec)

	if err := bus.Publish("engine_data", map[string]any{"rpm": 4200}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		seen := rec.seen
		rpm := rec.rpm
		rec.mu.Unlock()
		if seen {
			if rpm != 4200 {
				t.Fatalf("expected rpm 4200, got %d", rpm)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("subscriber never saw the message")
}
