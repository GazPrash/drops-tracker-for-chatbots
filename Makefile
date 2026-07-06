.PHONY: build run-tracker run-gateway run-backend loadtest clean

build:
	go build -o bin/tracker  ./cmd/tracker
	go build -o bin/gateway  ./cmd/gateway
	go build -o bin/backend  ./cmd/backend
	go build -o bin/loadtest ./cmd/loadtest

run-tracker:
	go run ./cmd/tracker

run-gateway:
	go run ./cmd/gateway

run-backend:
	go run ./cmd/backend

loadtest:
	go run ./cmd/loadtest --users=50 --messages=10

clean:
	rm -rf bin/ tracker.db
