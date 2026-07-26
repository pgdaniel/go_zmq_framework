// Runs the framework's StateRegistry as a flow node. What it caches is
// decided entirely by the subscribes list in flow.yml — this file knows
// nothing about topics. Prints its snapshot every 5 seconds.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"go_zmq_framework/framework"
)

func main() {
	_, registry, err := framework.Boot(framework.NewStateRegistry)
	if err != nil {
		panic(err)
	}
	fmt.Println("online")

	for {
		time.Sleep(5 * time.Second)
		fmt.Println("---- Global State Snapshot ----")

		active, telemetry := registry.Snapshot()
		for name, status := range active {
			fmt.Printf("  active_nodes[%s] = status=%s timestamp=%d\n", name, status.Status, status.Timestamp)
		}
		for topic, payload := range telemetry {
			b, err := json.Marshal(payload)
			if err != nil {
				continue
			}
			fmt.Printf("  telemetry[%s] = %s\n", topic, b)
		}
	}
}
