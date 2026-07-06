package main

import (
	"encoding/json"
	"log"
	"net/http"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"message-drop-tracker/internal/protocol"
)

var db *gorm.DB

type EventRecord struct {
	gorm.Model
	EventID       string `gorm:"uniqueIndex;not null"`
	EventType     string `gorm:"index;not null"`
	MessageID     string `gorm:"index;not null"`
	CorrelationID string `gorm:"index;not null"`
	SessionID     string `gorm:"not null"`
	ChunkIndex    int
	TotalChunks   int
	Service       string `gorm:"not null"`
	Timestamp     int64  `gorm:"index;not null"`
	Metadata      string
}

func main() {
	var err error

	db, err = gorm.Open(sqlite.Open("tracker.db?_pragma=busy_timeout=10000&_pragma=journal_mode=WAL"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatal(err)
	}

	db.AutoMigrate(&EventRecord{})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events", handlePostEvent)
	mux.HandleFunc("/api/events/list", handleGetEvents)
	mux.HandleFunc("/api/drops", handleGetDrops)
	mux.HandleFunc("/api/drops/summary", handleDropsSummary)
	mux.HandleFunc("/api/stats", handleDropsSummary)
	mux.HandleFunc("/", serveDashboard)

	log.Println("tracker on :8082")
	if err := http.ListenAndServe(":8082", mux); err != nil {
		log.Fatal(err)
	}
}

func handlePostEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event protocol.TrackingEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	metadata, _ := json.Marshal(event.Metadata)

	record := EventRecord{
		EventID:       event.EventID,
		EventType:     event.EventType,
		MessageID:     event.MessageID,
		CorrelationID: event.CorrelationID,
		SessionID:     event.SessionID,
		ChunkIndex:    event.ChunkIndex,
		TotalChunks:   event.TotalChunks,
		Service:       event.Service,
		Timestamp:     event.Timestamp,
		Metadata:      string(metadata),
	}

	db.Create(&record)
	w.WriteHeader(http.StatusOK)
}

func handleGetEvents(w http.ResponseWriter, r *http.Request) {
	corrID := r.URL.Query().Get("correlation_id")
	if corrID == "" {
		http.Error(w, "correlation_id required", http.StatusBadRequest)
		return
	}

	var records []EventRecord
	db.Where("correlation_id = ?", corrID).Order("timestamp asc").Find(&records)

	var events []protocol.TrackingEvent
	for _, rec := range records {
		events = append(events, protocol.TrackingEvent{
			EventID:       rec.EventID,
			EventType:     rec.EventType,
			MessageID:     rec.MessageID,
			CorrelationID: rec.CorrelationID,
			SessionID:     rec.SessionID,
			ChunkIndex:    rec.ChunkIndex,
			TotalChunks:   rec.TotalChunks,
			Service:       rec.Service,
			Timestamp:     rec.Timestamp,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(events)
}

func handleGetDrops(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode([]any{})
}

type msgGroup struct {
	ExpectedChunks int
	AckedChunks    int
	EmittedChunks  int
	HasGatewayRecv bool
	HasPubsubPub   bool
	HasBackendRecv bool
	HasGatewayDelv int
}

func handleDropsSummary(w http.ResponseWriter, r *http.Request) {
	var records []EventRecord
	db.Find(&records)

	groups := make(map[string]*msgGroup)
	for _, e := range records {
		grp, ok := groups[e.CorrelationID]
		if !ok {
			grp = &msgGroup{}
			groups[e.CorrelationID] = grp
		}

		switch e.EventType {
		case "gateway.received":
			grp.HasGatewayRecv = true
		case "pubsub.published":
			grp.HasPubsubPub = true
		case "backend.received":
			grp.HasBackendRecv = true
		case "backend.processing":
			if e.TotalChunks > 0 {
				grp.ExpectedChunks = e.TotalChunks
			}
		case "backend.emitted":
			grp.EmittedChunks++
		case "gateway.chunk_delivered":
			grp.HasGatewayDelv++
		case "client.ack":
			grp.AckedChunks++
		}
	}

	var totalMsgs, totalExpected, totalAcked int

	rawCounts := map[string]int{
		"gateway.publish": 0, "pubsub.deliver": 0, "backend.process": 0,
		"backend.emit": 0, "gateway.deliver": 0, "client.ack": 0,
	}

	for _, grp := range groups {
		if grp.HasGatewayRecv {
			totalMsgs++
		}
		totalExpected += grp.ExpectedChunks
		totalAcked += grp.AckedChunks

		if grp.HasGatewayRecv && !grp.HasPubsubPub {
			rawCounts["gateway.publish"]++
		}
		if grp.HasPubsubPub && !grp.HasBackendRecv {
			rawCounts["pubsub.deliver"]++
		}
		if grp.HasBackendRecv && grp.ExpectedChunks == 0 && grp.EmittedChunks == 0 {
			rawCounts["backend.process"]++
		}
		if grp.ExpectedChunks > grp.EmittedChunks {
			rawCounts["backend.emit"] += (grp.ExpectedChunks - grp.EmittedChunks)
		}
		if grp.EmittedChunks > grp.HasGatewayDelv {
			rawCounts["gateway.deliver"] += (grp.EmittedChunks - grp.HasGatewayDelv)
		}
		if grp.HasGatewayDelv > grp.AckedChunks {
			rawCounts["client.ack"] += (grp.HasGatewayDelv - grp.AckedChunks)
		}
	}

	msgsBase := float64(totalMsgs)
	if msgsBase == 0 {
		msgsBase = 1
	}
	chunksBase := float64(totalExpected)
	if chunksBase == 0 {
		chunksBase = 1
	}

	drops := map[string]any{
		"gateway.publish": map[string]any{"count": rawCounts["gateway.publish"], "rate": float64(rawCounts["gateway.publish"]) / msgsBase},
		"pubsub.deliver":  map[string]any{"count": rawCounts["pubsub.deliver"], "rate": float64(rawCounts["pubsub.deliver"]) / msgsBase},
		"backend.process": map[string]any{"count": rawCounts["backend.process"], "rate": float64(rawCounts["backend.process"]) / msgsBase},
		"backend.emit":    map[string]any{"count": rawCounts["backend.emit"], "rate": float64(rawCounts["backend.emit"]) / chunksBase},
		"gateway.deliver": map[string]any{"count": rawCounts["gateway.deliver"], "rate": float64(rawCounts["gateway.deliver"]) / chunksBase},
		"client.ack":      map[string]any{"count": rawCounts["client.ack"], "rate": float64(rawCounts["client.ack"]) / chunksBase},
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]any{
		"total_messages":        totalMsgs,
		"total_chunks_expected": totalExpected,
		"total_chunks_acked":    totalAcked,
		"drops_by_hop":          drops,
	})
}

func serveDashboard(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "frontend/dashboard/index.html")
}
