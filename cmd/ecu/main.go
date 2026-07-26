// Simulated engine unit. Publishes: engine_data. Subscribes: throttle_request.
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"go_zmq_framework/framework"
)

type Ecu struct {
	bus *framework.Bus
}

func (e *Ecu) HandleMessage(topic string, payload framework.Payload) {
	if topic != "throttle_request" {
		return
	}
	if pos, ok := payload["position"].(json.Number); ok {
		fmt.Printf("Received throttle command: %s%%\n", pos.String())
	}
}

func main() {
	handle, _, err := framework.Boot(func(bus *framework.Bus) *Ecu {
		return &Ecu{bus: bus}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")

	time.Sleep(time.Second) // let PUB/SUB connections settle before the first broadcast
	for {
		rpm := 2000 + rand.Intn(5001)
		fmt.Printf("Broadcasting RPM: %d\n", rpm)
		handle.Broadcast("engine_data", map[string]any{"rpm": rpm})
		time.Sleep(time.Second)
	}
}
