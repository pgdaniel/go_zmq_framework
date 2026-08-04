// Monitors sensor readings and publishes alerts when thresholds are crossed.
// Publishes: alert. Subscribes: sensor_reading, alert_acknowledge.
package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go_zmq_framework/framework"
)

const (
	tempHighThreshold     = 32.0
	tempLowThreshold      = 20.0
	soilDryThreshold      = 25.0
	humidityHighThreshold = 85.0
)

type Monitor struct {
	bus *framework.Bus

	mu           sync.Mutex
	activeAlerts map[string]time.Time
	alertCounter int
}

func (m *Monitor) HandleMessage(topic string, payload framework.Payload) {
	switch topic {
	case "sensor_reading":
		m.checkThresholds(payload)
	case "alert_acknowledge":
		alertID, _ := payload["alert_id"].(string)
		m.mu.Lock()
		delete(m.activeAlerts, alertID)
		m.mu.Unlock()
		fmt.Printf("Cleared alert %s\n", alertID)
	}
}

func (m *Monitor) checkThresholds(payload framework.Payload) {
	num := func(key string) float64 {
		n, _ := payload[key].(json.Number)
		v, _ := n.Float64()
		return v
	}

	temp := num("temperature")
	soil := num("soil_moisture")
	humidity := num("humidity")

	var alerts []map[string]any

	if temp > tempHighThreshold {
		alerts = append(alerts, map[string]any{
			"severity": "warning",
			"message":  fmt.Sprintf("Temperature too high: %.1f°C (threshold: %.1f°C)", temp, tempHighThreshold),
			"metric":   "temperature",
			"value":    temp,
		})
	} else if temp < tempLowThreshold {
		alerts = append(alerts, map[string]any{
			"severity": "warning",
			"message":  fmt.Sprintf("Temperature too low: %.1f°C (threshold: %.1f°C)", temp, tempLowThreshold),
			"metric":   "temperature",
			"value":    temp,
		})
	}

	if soil < soilDryThreshold {
		alerts = append(alerts, map[string]any{
			"severity": "critical",
			"message":  fmt.Sprintf("Soil too dry: %.1f%% (threshold: %.1f%%)", soil, soilDryThreshold),
			"metric":   "soil_moisture",
			"value":    soil,
		})
	}

	if humidity > humidityHighThreshold {
		alerts = append(alerts, map[string]any{
			"severity": "info",
			"message":  fmt.Sprintf("Humidity too high: %.1f%% (threshold: %.1f%%)", humidity, humidityHighThreshold),
			"metric":   "humidity",
			"value":    humidity,
		})
	}

	for _, alert := range alerts {
		m.mu.Lock()
		m.alertCounter++
		alertID := fmt.Sprintf("alert-%d", m.alertCounter)
		m.activeAlerts[alertID] = time.Now()
		m.mu.Unlock()

		alert["alert_id"] = alertID
		alert["timestamp"] = time.Now().Unix()
		fmt.Printf("ALERT [%s]: %s\n", alert["severity"], alert["message"])
		if err := m.bus.Publish("alert", alert); err != nil {
			fmt.Printf("Failed to publish alert: %v\n", err)
		}
	}
}

func main() {
	_, _, err := framework.Boot(func(bus *framework.Bus) *Monitor {
		return &Monitor{
			bus:          bus,
			activeAlerts: make(map[string]time.Time),
		}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")
	framework.SleepForever()
}
