// Controls both zones independently. Publishes: zone_a_command, zone_b_command.
// Subscribes: zone_a_reading, zone_b_reading.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go_zmq_framework/framework"
)

const (
	soilDry          = 30.0
	tempHot          = 30.0
	tempCoolEnough   = 25.0
	irrigateCooldown = 8 * time.Second
)

type ZoneController struct {
	bus *framework.Bus

	ventOpenA     bool
	ventOpenB     bool
	lastIrrigateA time.Time
	lastIrrigateB time.Time
}

func (c *ZoneController) command(topic string, payload map[string]any) {
	if err := c.bus.Publish(topic, payload); err != nil {
		fmt.Fprintf(os.Stderr, "[Framework Error] broadcast failed: %v\n", err)
	}
}

func (c *ZoneController) HandleMessage(topic string, payload framework.Payload) {
	num := func(key string) float64 {
		n, _ := payload[key].(json.Number)
		v, _ := n.Float64()
		return v
	}

	switch topic {
	case "zone_a_reading":
		c.controlZone("A", "zone_a_command", payload, num, &c.ventOpenA, &c.lastIrrigateA)
	case "zone_b_reading":
		c.controlZone("B", "zone_b_command", payload, num, &c.ventOpenB, &c.lastIrrigateB)
	}
}

func (c *ZoneController) controlZone(zone, cmdTopic string, payload framework.Payload, num func(string) float64, ventOpen *bool, lastIrrigate *time.Time) {
	temp := num("temperature")
	soil := num("soil_moisture")

	if soil < soilDry && time.Since(*lastIrrigate) >= irrigateCooldown {
		fmt.Printf("[%s] Soil dry (%.1f%%) — commanding irrigation\n", zone, soil)
		*lastIrrigate = time.Now()
		c.command(cmdTopic, map[string]any{"command": "irrigate"})
	}

	switch {
	case temp > tempHot && !*ventOpen:
		fmt.Printf("[%s] Too hot (%.1f°C) — opening vent\n", zone, temp)
		*ventOpen = true
		c.command(cmdTopic, map[string]any{"command": "vent", "state": "open"})
	case temp < tempCoolEnough && *ventOpen:
		fmt.Printf("[%s] Cool again (%.1f°C) — closing vent\n", zone, temp)
		*ventOpen = false
		c.command(cmdTopic, map[string]any{"command": "vent", "state": "close"})
	}
}

func main() {
	_, _, err := framework.Boot(func(bus *framework.Bus) *ZoneController {
		return &ZoneController{bus: bus}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")
	framework.SleepForever()
}
