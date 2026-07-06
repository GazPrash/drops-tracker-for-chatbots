package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"message-drop-tracker/internal/fault"
	"message-drop-tracker/internal/protocol"
	"message-drop-tracker/internal/tracking"
)

var ctx = context.Background()

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "")
	trackerURL := flag.String("tracker", "http://localhost:8082", "")
	port := flag.String("port", "8083", "")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	tracker := tracking.NewClient(*trackerURL)
	fi := fault.New()

	mux := http.NewServeMux()
	fi.RegisterRoutes(mux)
	go func() {
		log.Printf("backend fault api on :%s", *port)
		http.ListenAndServe(":"+*port, mux)
	}()

	log.Println("backend consumer started")
	sub := rdb.Subscribe(ctx, "user_messages")
	defer sub.Close()

	ch := sub.Channel()
	for msg := range ch {
		go processMessage(msg.Payload, rdb, tracker, fi)
	}
}

func processMessage(payload string, rdb *redis.Client, tr *tracking.Client, fi *fault.Injector) {
	var userMsg protocol.UserMessage
	if err := json.Unmarshal([]byte(payload), &userMsg); err != nil {
		return
	}

	if fi.ShouldDrop("pubsub.deliver") {
		return
	}

	totalChunks := 3 + rand.Intn(3) // 3 to 5 chunks

	tr.Track(protocol.NewTrackingEvent(protocol.EventBackendReceived, userMsg.MessageID, userMsg.CorrelationID, userMsg.SessionID, "backend"))

	processEvent := protocol.NewTrackingEvent(protocol.EventBackendProcessing, userMsg.MessageID, userMsg.CorrelationID, userMsg.SessionID, "backend")
	processEvent.TotalChunks = totalChunks
	tr.Track(processEvent)

	time.Sleep(time.Duration(200+rand.Intn(600)) * time.Millisecond)

	if fi.ShouldDrop("backend.process") {
		return
	}
	responses := []string{
		"let me help you with that.",
		"based on our catalog, here are some options.",
		"i'd recommend checking out our latest collection.",
		"additionally, we have a sale running this week.",
		"is there anything else i can help you with?",
	}

	for i := 0; i < totalChunks; i++ {
		chunkID := uuid.New().String()
		if fi.ShouldDrop("backend.emit") {
			continue
		}

		chunk := protocol.AIResponseChunk{
			ChunkID:       chunkID,
			CorrelationID: userMsg.CorrelationID,
			SessionID:     userMsg.SessionID,
			ChunkIndex:    i + 1,
			TotalChunks:   totalChunks,
			Content:       responses[i],
			Timestamp:     time.Now().UnixMilli(),
		}

		chunkPayload, _ := json.Marshal(chunk)
		rdb.Publish(ctx, "ai_responses:"+userMsg.SessionID, string(chunkPayload))
		tr.Track(protocol.NewTrackingEvent(protocol.EventBackendEmitted, chunk.ChunkID, chunk.CorrelationID, chunk.SessionID, "backend"))

		time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
	}
}
