// Web dashboard showing active alerts with acknowledge buttons.
// Publishes: alert_acknowledge. Subscribes: alert.
package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"sync"
	"time"

	"go_zmq_framework/framework"
)

type Alert struct {
	ID        string
	Severity  string
	Message   string
	Timestamp time.Time
}

var (
	mu           sync.Mutex
	activeAlerts = make(map[string]Alert)
)

type WebDash struct{}

func (w *WebDash) HandleMessage(topic string, payload framework.Payload) {
	if topic != "alert" {
		return
	}

	alertID, _ := payload["alert_id"].(string)
	severity, _ := payload["severity"].(string)
	message, _ := payload["message"].(string)
	tsNum, _ := payload["timestamp"].(json.Number)
	ts, _ := tsNum.Int64()

	mu.Lock()
	activeAlerts[alertID] = Alert{
		ID:        alertID,
		Severity:  severity,
		Message:   message,
		Timestamp: time.Unix(ts, 0),
	}
	mu.Unlock()
}

var pageTmpl = template.Must(template.New("page").Parse(`<!DOCTYPE html>
<html>
<head>
  <title>Greenhouse Alerts</title>
  <style>
    body { font-family: system-ui, sans-serif; background: #1a1a1a; color: #eee; padding: 2rem; }
    .card { background: #2a2a2a; padding: 1.5rem; border-radius: 8px; max-width: 600px; margin-bottom: 1rem; }
    h1 { margin-top: 0; color: #4ade80; }
    .alert { background: #333; padding: 1rem; border-radius: 6px; margin-bottom: 0.5rem; border-left: 4px solid; }
    .alert.critical { border-color: #ef4444; }
    .alert.warning { border-color: #f59e0b; }
    .alert.info { border-color: #3b82f6; }
    .alert-header { display: flex; justify-content: space-between; align-items: center; }
    .severity { font-weight: bold; text-transform: uppercase; font-size: 0.85em; }
    .severity.critical { color: #ef4444; }
    .severity.warning { color: #f59e0b; }
    .severity.info { color: #3b82f6; }
    button { background: #10b981; color: white; border: none; padding: 6px 12px; border-radius: 4px; cursor: pointer; font-size: 0.9em; }
    button:hover { background: #059669; }
    .timestamp { color: #888; font-size: 0.85em; }
    .empty { color: #666; font-style: italic; }
  </style>
  <meta http-equiv="refresh" content="2">
</head>
<body>
  <div class="card">
    <h1>Active Alerts</h1>
    {{if .Alerts}}
      {{range .Alerts}}
        <div class="alert {{.Severity}}">
          <div class="alert-header">
            <span class="severity {{.Severity}}">{{.Severity}}</span>
            <span class="timestamp">{{.Timestamp.Format "15:04:05"}}</span>
          </div>
          <p>{{.Message}}</p>
          <form action="/acknowledge" method="POST" style="margin-top: 0.5rem;">
            <input type="hidden" name="alert_id" value="{{.ID}}">
            <button type="submit">Acknowledge</button>
          </form>
        </div>
      {{end}}
    {{else}}
      <p class="empty">No active alerts</p>
    {{end}}
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
		port = "4570"
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		alerts := make([]Alert, 0, len(activeAlerts))
		for _, a := range activeAlerts {
			alerts = append(alerts, a)
		}
		mu.Unlock()

		w.Header().Set("Content-Type", "text/html")
		_ = pageTmpl.Execute(w, struct{ Alerts []Alert }{alerts})
	})

	http.HandleFunc("/acknowledge", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		alertID := r.FormValue("alert_id")

		mu.Lock()
		delete(activeAlerts, alertID)
		mu.Unlock()

		handle.Broadcast("alert_acknowledge", map[string]any{"alert_id": alertID})
		fmt.Printf("Acknowledged alert %s\n", alertID)
		http.Redirect(w, r, "/", http.StatusFound)
	})

	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		panic(err)
	}
}
