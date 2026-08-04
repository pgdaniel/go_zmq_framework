// Event log and running stats. Publishes: nothing.
// Subscribes: sensor_reading, actuator_command.
//
// A pure consumer: it never publishes, so it appears in no other node's
// peer list — the bus equivalent of tapping the wire.
package main

import (
	"encoding/json"
	"fmt"
	"math"

	"go_zmq_framework/framework"
)

type Logger struct {
	readings int
	minT     float64
	maxT     float64
	minS     float64
	maxS     float64
}

func (l *Logger) HandleMessage(topic string, payload framework.Payload) {
	switch topic {
	case "sensor_reading":
		num := func(key string) float64 {
			n, _ := payload[key].(json.Number)
			v, _ := n.Float64()
			return v
		}
		temp, soil := num("temperature"), num("soil_moisture")
		l.readings++
		l.minT, l.maxT = math.Min(l.minT, temp), math.Max(l.maxT, temp)
		l.minS, l.maxS = math.Min(l.minS, soil), math.Max(l.maxS, soil)

		if l.readings%10 == 0 {
			fmt.Printf("--- %d readings: temp %.1f–%.1f°C, soil %.1f–%.1f%% ---\n",
				l.readings, l.minT, l.maxT, l.minS, l.maxS)
		}
	case "actuator_command":
		cmd, _ := payload["command"].(string)
		state, _ := payload["state"].(string)
		if state != "" {
			fmt.Printf("EVENT: %s %s\n", cmd, state)
		} else {
			fmt.Printf("EVENT: %s\n", cmd)
		}
	}
}

func main() {
	_, _, err := framework.Boot(func(bus *framework.Bus) *Logger {
		return &Logger{minT: math.Inf(1), minS: math.Inf(1),
			maxT: math.Inf(-1), maxS: math.Inf(-1)}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")
	framework.SleepForever()
}
