// Web dashboard showing both zones. Publishes: nothing. Subscribes: combined_reading.
package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"sync"

	"go_zmq_framework/framework"
)

var (
	mu     sync.Mutex
	zoneA  map[string]string
	zoneB  map[string]string
	status = "Waiting for data..."
)

type WebDash struct{}

func (w *WebDash) HandleMessage(topic string, payload framework.Payload) {
	if topic != "combined_reading" {
		return
	}

	zoneAData, _ := payload["zone_a"].(map[string]any)
	zoneBData, _ := payload["zone_b"].(map[string]any)

	mu.Lock()
	defer mu.Unlock()

	if zoneAData != nil {
		zoneA = map[string]string{
			"temperature":   formatNum(zoneAData["temperature"]),
			"humidity":      formatNum(zoneAData["humidity"]),
			"soil_moisture": formatNum(zoneAData["soil_moisture"]),
			"vent":          fmt.Sprintf("%v", zoneAData["vent"]),
		}
	}
	if zoneBData != nil {
		zoneB = map[string]string{
			"temperature":   formatNum(zoneBData["temperature"]),
			"humidity":      formatNum(zoneBData["humidity"]),
			"soil_moisture": formatNum(zoneBData["soil_moisture"]),
			"vent":          fmt.Sprintf("%v", zoneBData["vent"]),
		}
	}
	status = "Live"
}

func formatNum(v any) string {
	if n, ok := v.(json.Number); ok {
		return n.String()
	}
	return "-"
}

var pageTmpl = template.Must(template.New("page").Parse(`<!DOCTYPE html>
<html>
<head>
  <title>Multi-Zone Greenhouse</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #0f1a12; color: #eee; padding: 2rem; }
    .zones { display: flex; gap: 2rem; flex-wrap: wrap; }
    .card { background: #1c2b21; padding: 1.5rem; border-radius: 8px; min-width: 300px; }
    h1 { margin-top: 0; color: #4ade80; }
    h2 { color: #60a5fa; margin-top: 0; }
    .reading { font-size: 1.4em; font-weight: bold; }
    .status { color: #888; margin-bottom: 1rem; }
  </style>
  <meta http-equiv="refresh" content="2">
</head>
<body>
  <h1>Multi-Zone Greenhouse Dashboard</h1>
  <p class="status">Status: {{.Status}}</p>
  <div class="zones">
    <div class="card">
      <h2>Zone A</h2>
      {{if .ZoneA}}
        <p>Temperature: <span class="reading">{{index .ZoneA "temperature"}}&deg;C</span></p>
        <p>Humidity: <span class="reading">{{index .ZoneA "humidity"}}%</span></p>
        <p>Soil moisture: <span class="reading">{{index .ZoneA "soil_moisture"}}%</span></p>
        <p>Vent: <span class="reading">{{index .ZoneA "vent"}}</span></p>
      {{else}}
        <p class="reading">No data yet</p>
      {{end}}
    </div>
    <div class="card">
      <h2>Zone B</h2>
      {{if .ZoneB}}
        <p>Temperature: <span class="reading">{{index .ZoneB "temperature"}}&deg;C</span></p>
        <p>Humidity: <span class="reading">{{index .ZoneB "humidity"}}%</span></p>
        <p>Soil moisture: <span class="reading">{{index .ZoneB "soil_moisture"}}%</span></p>
        <p>Vent: <span class="reading">{{index .ZoneB "vent"}}</span></p>
      {{else}}
        <p class="reading">No data yet</p>
      {{end}}
    </div>
  </div>
</body>
</html>
`))

func main() {
	_, _, err := framework.Boot(func(bus *framework.Bus) *WebDash {
		return &WebDash{}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")

	port := os.Getenv("WEB_PORT")
	if port == "" {
		port = "4571"
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		data := struct {
			Status string
			ZoneA  map[string]string
			ZoneB  map[string]string
		}{status, zoneA, zoneB}
		mu.Unlock()

		w.Header().Set("Content-Type", "text/html")
		_ = pageTmpl.Execute(w, data)
	})

	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		panic(err)
	}
}
