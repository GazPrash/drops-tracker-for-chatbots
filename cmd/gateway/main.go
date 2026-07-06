package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"message-drop-tracker/internal/fault"
	"message-drop-tracker/internal/protocol"
	"message-drop-tracker/internal/tracking"
)

var ctx = context.Background()

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type ClientConn struct {
	SessionID string
	Conn      *websocket.Conn
	Send      chan []byte
}

type ConnectionManager struct {
	conns map[string]*ClientConn
	mu    sync.RWMutex
}

func (cm *ConnectionManager) Add(c *ClientConn) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.conns[c.SessionID] = c
}

func (cm *ConnectionManager) Remove(sessionID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.conns, sessionID)
}

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "")
	trackerURL := flag.String("tracker", "http://localhost:8082", "")
	port := flag.String("port", "8080", "")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	tracker := tracking.NewClient(*trackerURL)
	fi := fault.New()
	connMgr := &ConnectionManager{conns: make(map[string]*ClientConn)}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(w, r, connMgr, rdb, tracker, fi)
	})
	mux.HandleFunc("/", serveClientHTML)
	fi.RegisterRoutes(mux)

	log.Printf("gateway on :%s", *port)
	log.Fatal(http.ListenAndServe(":"+*port, mux))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request, cm *ConnectionManager, rdb *redis.Client, tr *tracking.Client, fi *fault.Injector) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}

	// Wait for register message
	_, msgBytes, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return
	}

	var wsMsg protocol.WSMessage
	if err := json.Unmarshal(msgBytes, &wsMsg); err != nil || wsMsg.Type != "register" {
		conn.Close()
		return
	}

	var regMsg protocol.RegisterMessage
	json.Unmarshal(wsMsg.Payload, &regMsg)
	sessionID := regMsg.SessionID

	client := &ClientConn{
		SessionID: sessionID,
		Conn:      conn,
		Send:      make(chan []byte, 256),
	}
	cm.Add(client)

	go writePump(client)
	go subscribeToResponses(client, rdb, tr, fi)
	readPump(client, rdb, tr, fi, cm)
}

func writePump(client *ClientConn) {
	defer client.Conn.Close()
	for msg := range client.Send {
		if err := client.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			return
		}
	}
}

func readPump(client *ClientConn, rdb *redis.Client, tr *tracking.Client, fi *fault.Injector, cm *ConnectionManager) {
	defer func() {
		cm.Remove(client.SessionID)
		client.Conn.Close()
	}()

	for {
		_, msgBytes, err := client.Conn.ReadMessage()
		if err != nil {
			break
		}

		var wsMsg protocol.WSMessage
		if err := json.Unmarshal(msgBytes, &wsMsg); err != nil {
			continue
		}

		switch wsMsg.Type {
		case "user_message":
			var userMsg protocol.UserMessage
			json.Unmarshal(wsMsg.Payload, &userMsg)

			tr.Track(protocol.NewTrackingEvent(protocol.EventGatewayReceived, userMsg.MessageID, userMsg.CorrelationID, userMsg.SessionID, "gateway"))

			if fi.ShouldDrop("gateway.publish") {
				continue
			}

			payload, _ := json.Marshal(userMsg)
			rdb.Publish(ctx, "user_messages", string(payload))
			tr.Track(protocol.NewTrackingEvent(protocol.EventPubSubPublished, userMsg.MessageID, userMsg.CorrelationID, userMsg.SessionID, "gateway"))

		case "ack":
			var ack protocol.ClientACK
			json.Unmarshal(wsMsg.Payload, &ack)
			tr.Track(protocol.NewTrackingEvent(protocol.EventClientACK, ack.ChunkID, ack.CorrelationID, ack.SessionID, "gateway"))
		}
	}
}

func subscribeToResponses(client *ClientConn, rdb *redis.Client, tr *tracking.Client, fi *fault.Injector) {
	sub := rdb.Subscribe(ctx, "ai_responses:"+client.SessionID)
	defer sub.Close()

	ch := sub.Channel()
	for msg := range ch {
		var chunk protocol.AIResponseChunk
		json.Unmarshal([]byte(msg.Payload), &chunk)

		tr.Track(protocol.NewTrackingEvent(protocol.EventGatewayChunkReceived, chunk.ChunkID, chunk.CorrelationID, chunk.SessionID, "gateway"))

		if fi.ShouldDrop("gateway.deliver") {
			continue
		}

		wsPayload, _ := json.Marshal(protocol.WSMessage{
			Type:    "ai_chunk",
			Payload: []byte(msg.Payload),
		})

		select {
		case client.Send <- wsPayload:
			tr.Track(protocol.NewTrackingEvent(protocol.EventGatewayChunkDelivered, chunk.ChunkID, chunk.CorrelationID, chunk.SessionID, "gateway"))
		default:
			log.Println("client buffer full, dropping chunk")
		}
	}
}

func serveClientHTML(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "frontend/chatbox/index.html")
}
