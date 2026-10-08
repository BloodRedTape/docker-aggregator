// Package api provides the initial HTTP API skeleton.
package api

import (
	"encoding/json"
	"net/http"
)

// NewHandler exposes health checks and placeholders for Docker API endpoints.
// Docker aggregation is not implemented yet: placeholders return 501 rather
// than pretending that there are no containers or engines.
func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /version", notImplemented)
	mux.HandleFunc("GET /containers/json", notImplemented)
	mux.HandleFunc("GET /containers/{id}/json", notImplemented)
	mux.HandleFunc("GET /containers/{id}/stats", notImplemented)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"message": "endpoint not found"})
	})
	return mux
}

func notImplemented(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"message": "Docker aggregation is not implemented yet",
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
