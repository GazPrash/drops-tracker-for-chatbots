package fault

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"sync"
)

type Injector struct {
	rates map[string]float64
	mu    sync.RWMutex
}

func New() *Injector {
	return &Injector{
		rates: make(map[string]float64),
	}
}

func (fi *Injector) ShouldDrop(hop string) bool {
	fi.mu.RLock()
	rate := fi.rates[hop]
	fi.mu.RUnlock()

	if rate <= 0 {
		return false
	}
	return rand.Float64() < rate
}

func (fi *Injector) SetRate(hop string, rate float64) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.rates[hop] = rate
}

func (fi *Injector) GetRates() map[string]float64 {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	copy := make(map[string]float64)
	for k, v := range fi.rates {
		copy[k] = v
	}
	return copy
}

func (fi *Injector) ResetAll() {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.rates = make(map[string]float64)
}

func (fi *Injector) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/fault/rates", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == "GET" {
			json.NewEncoder(w).Encode(fi.GetRates())
			return
		}

		if r.Method == "POST" {
			var req struct {
				Hop  string  `json:"hop"`
				Rate float64 `json:"rate"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			fi.SetRate(req.Hop, req.Rate)
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/fault/reset", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == "POST" {
			fi.ResetAll()
			json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})
}
