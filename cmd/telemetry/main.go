// Watches engine data and commands a throttle cut on over-rev.
// Publishes: throttle_request. Subscribes: engine_data.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"go_zmq_framework/framework"
)

const overRevRPM = 6000

type Telemetry struct {
	bus *framework.Bus
}

func (t *Telemetry) HandleMessage(topic string, payload framework.Payload) {
	if topic != "engine_data" {
		return
	}

	rpmNum, _ := payload["rpm"].(json.Number)
	rpm, _ := rpmNum.Int64()
	fmt.Printf("Processing RPM: %d\n", rpm)
	if rpm <= overRevRPM {
		return
	}

	fmt.Println("OVER-REV DETECTED! Commanding throttle cut...")
	if err := t.bus.Publish("throttle_request", map[string]any{"position": 50}); err != nil {
		fmt.Fprintf(os.Stderr, "[Framework Error] broadcast failed: %v\n", err)
	}
}

func main() {
	_, _, err := framework.Boot(func(bus *framework.Bus) *Telemetry {
		return &Telemetry{bus: bus}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")
	framework.SleepForever()
}
