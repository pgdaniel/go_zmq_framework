// Climate rules. Publishes: actuator_command. Subscribes: sensor_reading.
//
// Closes the loop the telemetry node closes in the main demo: watch
// readings, command actuators. Vent control uses hysteresis (open above
// 30°C, close below 25°C) and irrigation has a cooldown, so the node
// doesn't spam commands while the physics catch up.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go_zmq_framework/framework"
)

const (
	soilDry          = 30.0 // % moisture
	tempHot          = 30.0 // °C
	tempCoolEnough   = 25.0 // °C
	irrigateCooldown = 8 * time.Second
)

type Controller struct {
	bus *framework.Bus

	// Handlers on one bus never run concurrently, so this state needs no
	// lock — it is only ever touched inside HandleMessage.
	ventOpen     bool
	lastIrrigate time.Time
}

func (c *Controller) command(payload map[string]any) {
	if err := c.bus.Publish("actuator_command", payload); err != nil {
		fmt.Fprintf(os.Stderr, "[Framework Error] broadcast failed: %v\n", err)
	}
}

func (c *Controller) HandleMessage(topic string, payload framework.Payload) {
	if topic != "sensor_reading" {
		return
	}
	num := func(key string) float64 {
		n, _ := payload[key].(json.Number)
		v, _ := n.Float64()
		return v
	}
	temp, soil := num("temperature"), num("soil_moisture")

	if soil < soilDry && time.Since(c.lastIrrigate) >= irrigateCooldown {
		fmt.Printf("Soil dry (%.1f%%) — commanding irrigation\n", soil)
		c.lastIrrigate = time.Now()
		c.command(map[string]any{"command": "irrigate"})
	}

	switch {
	case temp > tempHot && !c.ventOpen:
		fmt.Printf("Too hot (%.1f°C) — opening vent\n", temp)
		c.ventOpen = true
		c.command(map[string]any{"command": "vent", "state": "open"})
	case temp < tempCoolEnough && c.ventOpen:
		fmt.Printf("Cool again (%.1f°C) — closing vent\n", temp)
		c.ventOpen = false
		c.command(map[string]any{"command": "vent", "state": "close"})
	}
}

func main() {
	_, _, err := framework.Boot(func(bus *framework.Bus) *Controller {
		return &Controller{bus: bus}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")
	framework.SleepForever()
}
