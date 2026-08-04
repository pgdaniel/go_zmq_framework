// Aggregates both zone readings into a combined reading.
// Publishes: combined_reading. Subscribes: zone_a_reading, zone_b_reading.
package main

import (
	"fmt"
	"sync"

	"go_zmq_framework/framework"
)

type Aggregator struct {
	bus *framework.Bus

	mu    sync.Mutex
	zoneA map[string]any
	zoneB map[string]any
}

func (a *Aggregator) HandleMessage(topic string, payload framework.Payload) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch topic {
	case "zone_a_reading":
		a.zoneA = payload
	case "zone_b_reading":
		a.zoneB = payload
	}

	// Publish combined reading when we have both zones
	if a.zoneA != nil && a.zoneB != nil {
		combined := map[string]any{
			"zone_a": a.zoneA,
			"zone_b": a.zoneB,
		}
		if err := a.bus.Publish("combined_reading", combined); err != nil {
			fmt.Printf("Failed to publish combined reading: %v\n", err)
		}
	}
}

func main() {
	_, _, err := framework.Boot(func(bus *framework.Bus) *Aggregator {
		return &Aggregator{bus: bus}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")
	framework.SleepForever()
}
