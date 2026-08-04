// Zone A sensors. Publishes: zone_a_reading. Subscribes: zone_a_command.
package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"go_zmq_framework/framework"
)

type ZoneSensors struct {
	bus *framework.Bus

	mu       sync.Mutex
	temp     float64
	humidity float64
	soil     float64
	ventOpen bool
	zone     string
}

func (z *ZoneSensors) HandleMessage(topic string, payload framework.Payload) {
	expectedCmd := z.zone + "_command"
	if topic != expectedCmd {
		return
	}
	cmd, _ := payload["command"].(string)

	z.mu.Lock()
	defer z.mu.Unlock()
	switch cmd {
	case "irrigate":
		z.soil = math.Min(100, z.soil+15)
		fmt.Printf("[%s] Irrigation received — soil moisture now %.1f%%\n", z.zone, z.soil)
	case "vent":
		state, _ := payload["state"].(string)
		z.ventOpen = state == "open"
		fmt.Printf("[%s] Vent now %s\n", z.zone, state)
	}
}

func (z *ZoneSensors) step() {
	z.temp += (rand.Float64() - 0.5) * 0.6
	if z.ventOpen {
		z.temp += (22 - z.temp) * 0.15
	} else {
		z.temp += 0.08
	}
	z.humidity = clamp(z.humidity+(rand.Float64()-0.5)*0.8, 20, 95)
	z.soil = clamp(z.soil-0.7, 0, 100)
}

func clamp(v, lo, hi float64) float64 {
	return math.Max(lo, math.Min(hi, v))
}

func round1(v float64) float64 {
	return math.Round(v*10) / 10
}

func main() {
	handle, z, err := framework.Boot(func(bus *framework.Bus) *ZoneSensors {
		return &ZoneSensors{bus: bus, temp: 27, humidity: 60, soil: 40, zone: "zone_a"}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")

	time.Sleep(time.Second)
	for {
		z.mu.Lock()
		z.step()
		reading := map[string]any{
			"zone":          "A",
			"temperature":   round1(z.temp),
			"humidity":      round1(z.humidity),
			"soil_moisture": round1(z.soil),
			"vent":          map[bool]string{true: "open", false: "closed"}[z.ventOpen],
		}
		z.mu.Unlock()

		fmt.Printf("[%s] Broadcasting: %.1f°C %.1f%% rh, soil %.1f%%\n",
			z.zone, reading["temperature"], reading["humidity"], reading["soil_moisture"])
		handle.Broadcast("zone_a_reading", reading)
		time.Sleep(2 * time.Second)
	}
}
