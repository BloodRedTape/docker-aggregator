// Package api exposes a read-only subset of Docker's HTTP API.
package api

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"docker-aggregator/src/config"
)

var versionPrefix = regexp.MustCompile(`^/v[0-9]+\.[0-9]+`)

func NewHandler(engines []config.Engine) http.Handler {
	a := newAggregator(engines)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			_, _ = w.Write([]byte("OK"))
		case "/healthz":
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		case "/version":
			writeJSON(w, http.StatusNotImplemented, map[string]string{"message": "API version negotiation is not implemented yet"})
		case "/containers/json":
			a.list(w, r, prefix)
		default:
			if strings.HasPrefix(path, "/containers/") {
				rest := strings.TrimPrefix(path, "/containers/")
				if index := strings.LastIndex(rest, "/"); index > 0 {
					key, operation := rest[:index], rest[index+1:]
					if operation == "json" || operation == "stats" {
						a.container(w, r, prefix, key, operation)
						return
					}
				}
			}
			writeJSON(w, http.StatusNotFound, map[string]string{"message": "endpoint not found"})
		}
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
