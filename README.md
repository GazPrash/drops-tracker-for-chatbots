# Message Drop Tracker

A distributed tracking or observing system for tracking dropped messages across a multi-hop chatbot architecture.

## Overview

we assume the following pipeline:
`Client -> Gateway (WS) -> Pub/Sub (Redis) -> Backend (Mock LLM) -> Redis -> Gateway (WS) -> Client`

this codebase supports:
- real-time drop tracking using a centralized message ledger
- fault injection to simulate drops at 5 different points in the pipeline
- load testing tool to simulate 50+ concurrent clients
- web dashboard to visualize pipeline health

## System Requirements

you need **Go** and **Redis** installed to run this project.

**Option 1: macOS (Homebrew)**
If you are on a Mac, you can install everything in one click using the included `Brewfile`:
```bash
brew bundle
```

**Option 2: Docker**
If you already have Go installed but don't want to install Redis on your system, you can spin it up using the included `docker-compose.yml`:
```bash
docker-compose up -d
```

## How to run
manually:

1. **Start Redis**
   ```bash
   redis-server
   # or 'docker-compose up -d' if using Docker
   ```

2. **Start the Tracker & Dashboard** (Terminal 1)
   ```bash
   make run-tracker
   ```
   Open `http://localhost:8082` for the dashboard.

3. **Start the Gateway** (Terminal 2)
   ```bash
   make run-gateway
   ```
   Open `http://localhost:8080` for the chat client.

4. **Start the Backend** (Terminal 3)
   ```bash
   make run-backend
   ```

u can also use `./run.sh`:

```bash
./run.sh
```
or 
```bash
./run.sh --scratch
```
to reset the tracker.db stats.

## Testing Drops
1. Open the dashboard at `http://localhost:8082`.
2. Move one of the fault injection sliders (e.g. `backend.emit`) to `0.5` (50% drop rate).
3. Send a message from the chat client at `http://localhost:8080`.
4. The dashboard will instantly show the dropped message and where it failed.

## Load Testing
Run the built-in load tester to simulate concurrent users:
```bash
make loadtest
```
This will output a terminal report and the web dashboard will update in real-time.
