package protocol

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// client -> gateway
type UserMessage struct {
	MessageID     string `json:"message_id"`
	CorrelationID string `json:"correlation_id"`
	SessionID     string `json:"session_id"`
	Content       string `json:"content"`
	Timestamp     int64  `json:"timestamp"`
}

// backend -> gateway -> client
type AIResponseChunk struct {
	ChunkID       string `json:"chunk_id"`
	CorrelationID string `json:"correlation_id"`
	SessionID     string `json:"session_id"`
	ChunkIndex    int    `json:"chunk_index"`
	TotalChunks   int    `json:"total_chunks"`
	Content       string `json:"content"`
	Timestamp     int64  `json:"timestamp"`
}

// client -> gateway
type ClientACK struct {
	ChunkID       string `json:"chunk_id"`
	CorrelationID string `json:"correlation_id"`
	SessionID     string `json:"session_id"`
	Timestamp     int64  `json:"timestamp"`
}

// websocket envelope
type WSMessage struct {
	Type    string          `json:"type"` // register, user_message, ai_chunk, ack
	Payload json.RawMessage `json:"payload"`
}

// first message after ws connect
type RegisterMessage struct {
	SessionID string `json:"session_id"`
}

// reported to the message ledger at each hop
type TrackingEvent struct {
	EventID       string            `json:"event_id"`
	EventType     string            `json:"event_type"`
	MessageID     string            `json:"message_id"`
	CorrelationID string            `json:"correlation_id"`
	SessionID     string            `json:"session_id"`
	ChunkIndex    int               `json:"chunk_index,omitempty"`
	TotalChunks   int               `json:"total_chunks,omitempty"`
	Service       string            `json:"service"`
	Timestamp     int64             `json:"timestamp"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

const (
	EventGatewayReceived       = "gateway.received"
	EventPubSubPublished       = "pubsub.published"
	EventBackendReceived       = "backend.received"
	EventBackendProcessing     = "backend.processing"
	EventBackendEmitted        = "backend.emitted"
	EventGatewayChunkReceived  = "gateway.chunk_received"
	EventGatewayChunkDelivered = "gateway.chunk_delivered"
	EventClientACK             = "client.ack"
)

func NewMessageID() string {
	return uuid.New().String()
}

func NewTrackingEvent(eventType, messageID, correlationID, sessionID, service string) TrackingEvent {
	return TrackingEvent{
		EventID:       uuid.New().String(),
		EventType:     eventType,
		MessageID:     messageID,
		CorrelationID: correlationID,
		SessionID:     sessionID,
		Service:       service,
		Timestamp:     time.Now().UnixMilli(),
	}
}
