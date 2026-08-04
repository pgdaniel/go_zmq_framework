// Receives alerts and sends notifications (logs them in this demo).
// Publishes: nothing. Subscribes: alert.
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"go_zmq_framework/framework"
)

type Alerter struct{}

func (a *Alerter) HandleMessage(topic string, payload framework.Payload) {
	if topic != "alert" {
		return
	}

	alertID, _ := payload["alert_id"].(string)
	severity, _ := payload["severity"].(string)
	message, _ := payload["message"].(string)
	tsNum, _ := payload["timestamp"].(json.Number)
	ts, _ := tsNum.Int64()

	timestamp := time.Unix(ts, 0).Format("15:04:05")

	// In a real app, this could send email, SMS, Slack, PagerDuty, etc.
	fmt.Printf("[%s] NOTIFICATION (%s): %s [ID: %s]\n", timestamp, severity, message, alertID)
}

func main() {
	_, _, err := framework.Boot(func(bus *framework.Bus) *Alerter {
		return &Alerter{}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("online")
	framework.SleepForever()
}
