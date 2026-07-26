// Relays raw SocketCAN frames onto the bus. Publishes: can_frame.
// Needs a real or virtual CAN interface (set CAN_IFACE, default can0);
// fails fast if it doesn't exist.
package main

import (
	"fmt"
	"os"

	"go_zmq_framework/framework"
)

func main() {
	iface := os.Getenv("CAN_IFACE")
	if iface == "" {
		iface = "can0"
	}

	// CanBridge.New needs an interface name in addition to the bus, so
	// unlike the other nodes this one passes a closure to Boot that
	// captures it — Go's closures make this a non-issue, unlike the Zig
	// port where CanBridge has to wire itself up by hand.
	_, _, err := framework.Boot(func(bus *framework.Bus) *framework.CanBridge {
		return framework.NewCanBridge(bus, iface, "can_frame")
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("online (reading %s, broadcasting :can_frame)\n", iface)
	framework.SleepForever()
}
