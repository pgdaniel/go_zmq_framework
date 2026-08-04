// Simulated greenhouse sensors. Publishes: sensor_reading. Subscribes: actuator_command.
//
// The physics live here: temperature climbs while the sun is out and the
// vent is closed, soil dries steadily, and actuator commands from other
// nodes (irrigate, vent open/close) push the simulation back the other
// way — so the flow is a real feedback loop, not a one-way stream.
package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"go_zmq_framework/framework"
)

type Greenhouse struct {
	bus *framework.Bus

	// Shared between the bus dispatch goroutine (HandleMessage) and the
	// simulation ticker in main — same mutex pattern as the webapp demo.
	mu       sync.Mutex
	temp     float64 // °C
	humidity float64 // %
	soil     float64 // % moisture
	ventOpen bool
}

func (g *Greenhouse) HandleMessage(topic string, payload framework.Payload) {
	if topic != "actuator_command" {
		return
	}
	cmd, _ := payload["command"].(string)

	g.mu.Lock()
	defer g.mu.Unlock()
	switch cmd {
	case "irrigate":
		g.soil = math.Min(100, g.soil+15)
		fmt.Printf("Irrigation received — soil moisture now %.1f%%\n", g.soil)
	case "vent":
		state, _ := payload["state"].(string)
		g.ventOpen = state == "open"
		fmt.Printf("Vent now %s\n", state)
	}
}

// step advances the simulation one tick (call with mu held).
func (g *Greenhouse) step() {
	g.temp += (rand.Float64() - 0.5) * 0.6 // weather noise
	if g.ventOpen {
		g.temp += (22 - g.temp) * 0.15 // vent pulls toward ambient
	} else {
		g.temp += 0.08 // sun warms a closed greenhouse
	}
	g.humidity = clamp(g.humidity+(rand.Float64()-0.5)*0.8, 20, 95)
	g.soil = clamp(g.soil-0.7, 0, 100) // plants drink
}

func clamp(v, lo, hi float64) float64 {
	return math.Max(lo, math.Min(hi, v))
}

func round1(v float64) float64 {
	return math.Round(v*10) / 10
}

func main() {
	handle, g, err := framework.Boot(func(bus *framework.Bus) *Greenhouse {
		return &Greenhouse{bus: bus, temp: 27, humidity: 60, soil: 40}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")

	time.Sleep(time.Second) // let PUB/SUB connections settle before the first broadcast
	for {
		g.mu.Lock()
		g.step()
		reading := map[string]any{
			"zone":          "A",
			"temperature":   round1(g.temp),
			"humidity":      round1(g.humidity),
			"soil_moisture": round1(g.soil),
			"vent":          map[bool]string{true: "open", false: "closed"}[g.ventOpen],
		}
		g.mu.Unlock()

		fmt.Printf("Broadcasting: %.1f°C %.1f%% rh, soil %.1f%%\n",
			reading["temperature"], reading["humidity"], reading["soil_moisture"])
		handle.Broadcast("sensor_reading", reading)
		time.Sleep(2 * time.Second)
	}
}
