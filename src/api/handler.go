// Package api exposes a read-only subset of Docker's HTTP API.
package api

import (
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"docker-aggregator/src/config"
)

var versionPrefix = regexp.MustCompile(`^/v[0-9]+\.[0-9]+`)

func NewHandler(engines []config.Engine, disable func(string, string, error) error) http.Handler {
	a := newAggregator(engines, disable)
	return logRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"message": "read-only API"})
			return
		}
		path := r.URL.Path
		prefix := versionPrefix.FindString(path)
		path = strings.TrimPrefix(path, prefix)
		if r.Method == http.MethodHead && path != "/_ping" && path != "/healthz" {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch path {
		case "/_ping":
			w.Header().Set("Content-Type", "text/plain")
			if !a.available(r.Context()) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("no Docker engines available"))
				return
			}
			_, _ = w.Write([]byte("OK"))
		case "/healthz":
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		case "/version", "/info":
			a.metadata(w, r, prefix, path)
		case "/containers/json":
			a.list(w, r, prefix)
		default:
			if strings.HasPrefix(path, "/containers/") {
				rest := strings.TrimPrefix(path, "/containers/")
				if index := strings.LastIndex(rest, "/"); index > 0 {
					key, operation := rest[:index], rest[index+1:]
					if operation == "json" || operation == "stats" || operation == "logs" {
						a.container(w, r, prefix, key, operation)
						return
					}
				}
			}
			writeJSON(w, http.StatusNotFound, map[string]string{"message": "endpoint not found"})
		}
	}))
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(body []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(body)
}

func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// logRequests reports failed responses, plus slow or client-canceled ones
// for non-streaming requests, where a disconnect is not the normal ending.
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		took := time.Since(start).Round(time.Millisecond)
		canceled := r.Context().Err() != nil
		if recorder.status < 400 && (streaming(r) || (!canceled && took < slowRequest)) {
			return
		}
		status := "no response"
		if recorder.status != 0 {
			status = strconv.Itoa(recorder.status)
		}
		suffix := ""
		if canceled {
			suffix = " (canceled by client)"
		}
		log.Printf("%s %s -> %s in %s%s", r.Method, r.URL.RequestURI(), status, took, suffix)
	})
}

func streaming(r *http.Request) bool {
	query := r.URL.Query()
	switch {
	case strings.HasSuffix(r.URL.Path, "/logs"):
		return boolValue(query.Get("follow"))
	case strings.HasSuffix(r.URL.Path, "/stats"):
		return query.Get("stream") == "" || boolValue(query.Get("stream"))
	}
	return false
}

// boolValue mirrors Docker's query parameter parsing.
func boolValue(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "0", "no", "false", "none":
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
