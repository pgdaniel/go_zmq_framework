// HTTP bridge onto the bus: shows live telemetry, sends commands back.
// Publishes: throttle_request. Subscribes: engine_data.
package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"strconv"
	"sync"

	"go_zmq_framework/framework"
)

// Shared between the bus dispatch goroutine and HTTP handler goroutines.
var (
	mu     sync.Mutex
	rpm    int64
	status = "Waiting for data..."
)

type WebBridge struct{}

func (w *WebBridge) HandleMessage(topic string, payload framework.Payload) {
	if topic != "engine_data" {
		return
	}
	n, _ := payload["rpm"].(json.Number)
	v, _ := n.Int64()

	mu.Lock()
	rpm = v
	status = "Live"
	mu.Unlock()
}

var pageTmpl = template.Must(template.New("page").Parse(`<!DOCTYPE html>
<html>
<head>
  <title>ZMQ Telemetry Dashboard</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #111; color: #eee; padding: 2rem; }
    .card { background: #222; padding: 1.5rem; border-radius: 8px; max-width: 400px; margin-bottom: 1rem;}
    h1 { margin-top: 0; color: #4ade80; }
    button { background: #ef4444; color: white; border: none; padding: 10px 15px; border-radius: 5px; cursor: pointer; }
    button:hover { background: #dc2626; }
  </style>
  <meta http-equiv="refresh" content="1">
</head>
<body>
  <div class="card">
    <h1>Telemetry Dashboard</h1>
    <p><strong>Status:</strong> {{.Status}}</p>
    <p><strong>RPM:</strong> <span style="font-size: 1.5em; font-weight: bold;">{{.RPM}}</span></p>
  </div>
  <div class="card">
    <h2>Overrides</h2>
    <form action="/command" method="POST">
      <input type="hidden" name="throttle" value="0">
      <button type="submit">Send Engine Kill (0% Throttle)</button>
    </form>
  </div>
</body>
</html>
`))

func main() {
	handle, _, err := framework.Boot(func(bus *framework.Bus) *WebBridge {
		return &WebBridge{}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")

	port := os.Getenv("WEB_PORT")
	if port == "" {
		port = "4567"
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		data := struct {
			Status string
			RPM    int64
		}{status, rpm}
		mu.Unlock()

		w.Header().Set("Content-Type", "text/html")
		_ = pageTmpl.Execute(w, data)
	})

	http.HandleFunc("/command", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		throttlePos, _ := strconv.Atoi(r.FormValue("throttle"))
		handle.Broadcast("throttle_request", map[string]any{"position": throttlePos})
		fmt.Printf("Broadcasted throttle command: %d%%\n", throttlePos)
		http.Redirect(w, r, "/", http.StatusFound)
	})

	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		panic(err)
	}
}
