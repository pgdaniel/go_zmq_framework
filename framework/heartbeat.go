package framework

import (
	"fmt"
	"os"
	"time"
)

// HeartbeatInterval is how often every node publishes on topic "heartbeat".
const HeartbeatInterval = 5 * time.Second

// heartbeat is meant to live for the process's lifetime; start() spawns a
// goroutine that holds a reference to bus and name for as long as it runs.
type heartbeat struct {
	stop_ chan struct{}
	done  chan struct{}
}

func startHeartbeat(bus *Bus, name string) *heartbeat {
	hb := &heartbeat{stop_: make(chan struct{}), done: make(chan struct{})}
	go hb.loop(bus, name)
	return hb
}

func (hb *heartbeat) loop(bus *Bus, name string) {
	defer close(hb.done)

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		if err := bus.Publish("heartbeat", map[string]any{
			"node_name": name,
			"status":    "ok",
			"timestamp": time.Now().Unix(),
		}); err != nil {
			fmt.Fprintf(os.Stderr, "[Framework Error] Heartbeat failed for %s: %v\n", name, err)
		}

		select {
		case <-hb.stop_:
			return
		case <-ticker.C:
		}
	}
}

// stop wakes the goroutine out of its interval wait rather than killing
// it, so an in-flight broadcast always completes. Idempotent.
func (hb *heartbeat) stop() {
	select {
	case <-hb.stop_:
	default:
		close(hb.stop_)
	}
	<-hb.done
}
