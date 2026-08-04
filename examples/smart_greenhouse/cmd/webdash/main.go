// HTTP bridge onto the bus: shows live greenhouse readings and lets a
// human force a manual watering. Publishes: actuator_command.
// Subscribes: sensor_reading.
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

// Shared between the bus dispatch goroutine and HTTP handler goroutines.
var (
	mu     sync.Mutex
	latest = map[string]string{
		"temperature": "-", "humidity": "-", "soil_moisture": "-", "vent": "-",
	}
	status = "Waiting for data..."
)

type WebDash struct{}

func (w *WebDash) HandleMessage(topic string, payload framework.Payload) {
	if topic != "sensor_reading" {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	for _, key := range []string{"temperature", "humidity", "soil_moisture"} {
		if n, ok := payload[key].(json.Number); ok {
			latest[key] = n.String()
		}
	}
	if vent, ok := payload["vent"].(string); ok {
		latest["vent"] = vent
	}
	status = "Live"
}

var pageTmpl = template.Must(template.New("page").Parse(`<!DOCTYPE html>
<html>
<head>
  <title>Greenhouse Dashboard</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #0f1a12; color: #eee; padding: 2rem; }
    .card { background: #1c2b21; padding: 1.5rem; border-radius: 8px; max-width: 420px; margin-bottom: 1rem; }
    h1 { margin-top: 0; color: #4ade80; }
    .reading { font-size: 1.4em; font-weight: bold; }
    button { background: #3b82f6; color: white; border: none; padding: 10px 15px; border-radius: 5px; cursor: pointer; }
    button:hover { background: #2563eb; }
  </style>
  <meta http-equiv="refresh" content="2">
</head>
<body>
  <div class="card">
    <h1>Greenhouse — Zone A</h1>
    <p><strong>Status:</strong> {{.Status}}</p>
    <p>Temperature: <span class="reading">{{index .Latest "temperature"}}&deg;C</span></p>
    <p>Humidity: <span class="reading">{{index .Latest "humidity"}}%</span></p>
    <p>Soil moisture: <span class="reading">{{index .Latest "soil_moisture"}}%</span></p>
    <p>Vent: <span class="reading">{{index .Latest "vent"}}</span></p>
  </div>
  <div class="card">
    <h2>Manual override</h2>
    <form action="/water" method="POST">
      <button type="submit">Irrigate now</button>
    </form>
  </div>
</body>
</html>
`))

func main() {
	handle, _, err := framework.Boot(func(bus *framework.Bus) *WebDash {
		return &WebDash{}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")

	port := os.Getenv("WEB_PORT")
	if port == "" {
		port = "4569"
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		data := struct {
			Status string
			Latest map[string]string
		}{status, latest}
		mu.Unlock()

		w.Header().Set("Content-Type", "text/html")
		_ = pageTmpl.Execute(w, data)
	})

	http.HandleFunc("/water", func(w http.ResponseWriter, r *http.Request) {
		handle.Broadcast("actuator_command", map[string]any{"command": "irrigate"})
		fmt.Println("Broadcasted manual irrigation")
		http.Redirect(w, r, "/", http.StatusFound)
	})

	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		panic(err)
	}
}
