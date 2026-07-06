package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"message-drop-tracker/internal/protocol"
)

type UserResult struct {
	UserID         int
	MessagesSent   int
	ChunksExpected int
	ChunksReceived int
	ACKsSent       int
	Duration       time.Duration
}

func main() {
	users := flag.Int("users", 50, "concurrent users")
	messages := flag.Int("messages", 10, "messages per user")
	gateway := flag.String("gateway", "ws://localhost:8080/ws", "gateway url")
	delay := flag.Duration("delay", 500*time.Millisecond, "delay between messages")
	timeout := flag.Duration("timeout", 15*time.Second, "max wait time for chunks")
	tracker := flag.String("tracker", "http://localhost:8082", "tracker url")
	flag.Parse()

	log.Printf("starting load test: %d users, %d msgs/user", *users, *messages)

	start := time.Now()
	results := make(chan UserResult, *users)
	var wg sync.WaitGroup

	for i := 0; i < *users; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results <- simulateUser(id, *messages, *gateway, *delay, *timeout)
		}(i)
	}

	wg.Wait()
	close(results)
	duration := time.Since(start)

	var total UserResult
	for r := range results {
		total.MessagesSent += r.MessagesSent
		total.ChunksExpected += r.ChunksExpected
		total.ChunksReceived += r.ChunksReceived
		total.ACKsSent += r.ACKsSent
	}

	fmt.Printf("\n==== LOAD TEST REPORT ====\n")
	fmt.Printf("users:             %d\n", *users)
	fmt.Printf("msgs/user:         %d\n", *messages)
	fmt.Printf("total msgs:        %d\n", total.MessagesSent)
	fmt.Printf("chunks received:   %d\n", total.ChunksReceived)
	fmt.Printf("duration:          %s\n", duration)

	printTrackerSummary(*tracker)
}

func simulateUser(id int, msgCount int, url string, delay time.Duration, waitTimeout time.Duration) UserResult {
	res := UserResult{UserID: id}
	start := time.Now()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return res
	}
	defer conn.Close()

	sessionID := uuid.New().String()
	regMsg, _ := json.Marshal(protocol.WSMessage{
		Type:    "register",
		Payload: mustMarshal(protocol.RegisterMessage{SessionID: sessionID}),
	})
	conn.WriteMessage(websocket.TextMessage, regMsg)

	var mu sync.Mutex
	lastRecv := time.Now()

	go func() {
		for {
			_, msgBytes, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var wsMsg protocol.WSMessage
			if err := json.Unmarshal(msgBytes, &wsMsg); err != nil {
				continue
			}

			if wsMsg.Type == "ai_chunk" {
				var chunk protocol.AIResponseChunk
				json.Unmarshal(wsMsg.Payload, &chunk)

				mu.Lock()
				res.ChunksReceived++
				lastRecv = time.Now()
				mu.Unlock()

				ackPayload := mustMarshal(protocol.ClientACK{
					ChunkID:       chunk.ChunkID,
					CorrelationID: chunk.CorrelationID,
					SessionID:     sessionID,
					Timestamp:     time.Now().UnixMilli(),
				})

				conn.WriteMessage(websocket.TextMessage, mustMarshal(protocol.WSMessage{
					Type:    "ack",
					Payload: ackPayload,
				}))

				mu.Lock()
				res.ACKsSent++
				mu.Unlock()
			}
		}
	}()

	for i := 0; i < msgCount; i++ {
		msgID := uuid.New().String()
		userMsg := protocol.UserMessage{
			MessageID: msgID, CorrelationID: msgID,
			SessionID: sessionID, Content: fmt.Sprintf("test msg %d from user %d", i, id),
			Timestamp: time.Now().UnixMilli(),
		}

		conn.WriteMessage(websocket.TextMessage, mustMarshal(protocol.WSMessage{
			Type:    "user_message",
			Payload: mustMarshal(userMsg),
		}))

		res.MessagesSent++
		time.Sleep(delay)
	}

	for {
		mu.Lock()
		idle := time.Since(lastRecv)
		mu.Unlock()
		if idle > waitTimeout {
			break
		}
		time.Sleep(1 * time.Second)
	}

	res.Duration = time.Since(start)
	return res
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func printTrackerSummary(url string) {
	resp, err := http.Get(url + "/api/drops/summary")
	if err != nil {
		fmt.Printf("failed to get tracker summary: %v\n", err)
		return
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	var summary map[string]any
	json.Unmarshal(b, &summary)

	fmt.Printf("\n==== PER-HOP DROPS (from tracker) ====\n")
	drops := summary["drops_by_hop"].(map[string]any)
	for hop, d := range drops {
		data := d.(map[string]any)
		count := int(data["count"].(float64))
		rate := data["rate"].(float64)
		fmt.Printf("%-18s %4d (%.2f%%)\n", hop+":", count, rate*100)
	}
	fmt.Println("======================================")
}
