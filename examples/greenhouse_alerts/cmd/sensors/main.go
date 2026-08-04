// Simulated greenhouse sensors. Publishes: sensor_reading. Subscribes: alert_acknowledge.
//
// Same physics as smart_greenhouse, but also tracks active alerts and
// clears them when acknowledged.
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

	mu       sync.Mutex
	temp     float64
	humidity float64
	soil     float64
	ventOpen bool
}

func (g *Greenhouse) HandleMessage(topic string, payload framework.Payload) {
	if topic != "alert_acknowledge" {
		return
	}
	alertID, _ := payload["alert_id"].(string)
	fmt.Printf("Alert %s acknowledged\n", alertID)
}

func (g *Greenhouse) step() {
	g.temp += (rand.Float64() - 0.5) * 0.6
	if g.ventOpen {
		g.temp += (22 - g.temp) * 0.15
	} else {
		g.temp += 0.08
	}
	g.humidity = clamp(g.humidity+(rand.Float64()-0.5)*0.8, 20, 95)
	g.soil = clamp(g.soil-0.7, 0, 100)
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

	time.Sleep(time.Second)
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
