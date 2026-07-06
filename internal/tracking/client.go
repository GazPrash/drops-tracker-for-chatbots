package tracking

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"message-drop-tracker/internal/protocol"
)

type Client struct {
	baseURL string
	http    *http.Client
	queue   chan protocol.TrackingEvent
}

func NewClient(trackerURL string) *Client {
	c := &Client{
		baseURL: trackerURL,
		http: &http.Client{
			Timeout: 5 * time.Second,
		},
		queue: make(chan protocol.TrackingEvent, 2048),
	}
	go c.sender()
	return c
}

func (c *Client) Track(event protocol.TrackingEvent) {
	select {
	case c.queue <- event:
		// queued
	default:
		log.Printf("warning: tracking queue full, dropped event %s for %s", event.EventType, event.MessageID)
	}
}

func (c *Client) sender() {
	url := c.baseURL + "/api/events"
	for event := range c.queue {
		payload, _ := json.Marshal(event)
		resp, err := c.http.Post(url, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("error tracking event: %v", err)
			continue
		}
		resp.Body.Close()
	}
}
