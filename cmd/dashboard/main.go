// Consumer side of the async state-sync pattern: request the registry's
// snapshot once on startup, then log whatever comes back.
// Publishes: request_global_state. Subscribes: global_state_snapshot.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"go_zmq_framework/framework"
)

type Dashboard struct{}

func (d *Dashboard) HandleMessage(topic string, payload framework.Payload) {
	if topic != "global_state_snapshot" {
		return
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	fmt.Printf("Synced global state: %s\n", b)
}

func main() {
	handle, _, err := framework.Boot(func(bus *framework.Bus) *Dashboard {
		return &Dashboard{}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")

	time.Sleep(time.Second) // let PUB/SUB connections settle before the fire-and-forget request
	handle.Broadcast("request_global_state", map[string]any{"requester": handle.Name})
	framework.SleepForever()
}
