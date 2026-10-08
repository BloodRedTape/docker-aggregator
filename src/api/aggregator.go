package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"docker-aggregator/src/config"
)

type upstreamError struct{ status int }

func (e upstreamError) Error() string { return fmt.Sprintf("Docker returned HTTP %d", e.status) }

type engine struct {
	name      string
	client    *http.Client
	transport *http.Transport
}

type route struct {
	engine   *engine
	original string
	name     string
}

type aggregator struct {
	engines []*engine
	mu      sync.RWMutex
	routes  map[string]route
}

func newAggregator(configs []config.Engine) *aggregator {
	a := &aggregator{routes: make(map[string]route)}
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}
		socket := cfg.Socket
		transport := &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "unix", socket)
			},
			ResponseHeaderTimeout: 10 * time.Second,
			IdleConnTimeout:       60 * time.Second,
		}
		a.engines = append(a.engines, &engine{name: cfg.Name, transport: transport, client: &http.Client{
			Transport:     transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
		}})
	}
	return a
}

func virtualID(name, original string) string {
	sum := sha256.Sum256([]byte(name + "\x00" + original))
	return hex.EncodeToString(sum[:])
}

func (e *engine) get(ctx context.Context, path, query string, target any) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker"+path, nil)
	if err != nil {
		return err
	}
	req.URL.RawQuery = query
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return upstreamError{status: resp.StatusCode}
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(target)
}

func stringField(object map[string]json.RawMessage, key string) string {
	var value string
	_ = json.Unmarshal(object[key], &value)
	return value
}

func setField(object map[string]json.RawMessage, key string, value any) {
	object[key], _ = json.Marshal(value)
}

// collect keeps known routes when a daemon is temporarily unavailable.
func (a *aggregator) collect(ctx context.Context, prefix, query string) ([]map[string]json.RawMessage, []string) {
	containers := make([]map[string]json.RawMessage, 0)
	failed := make([]string, 0)
	for _, e := range a.engines {
		var items []map[string]json.RawMessage
		if err := e.get(ctx, prefix+"/containers/json", query, &items); err != nil {
			log.Printf("engine %s: list: %v", e.name, err)
			failed = append(failed, e.name)
			continue
		}
		// A complete refresh replaces routes only for this available engine.
		if query == "all=1" {
			a.mu.Lock()
			for id, rt := range a.routes {
				if rt.engine == e {
					delete(a.routes, id)
				}
			}
			a.mu.Unlock()
		}
		for _, item := range items {
			original := stringField(item, "Id")
			if original == "" {
				continue
			}
			id := virtualID(e.name, original)
			var names []string
			_ = json.Unmarshal(item["Names"], &names)
			for i := range names {
				names[i] = "/" + e.name + "/" + strings.TrimPrefix(names[i], "/")
			}
			name := ""
			if len(names) > 0 {
				name = strings.TrimPrefix(names[0], "/")
			}
			a.mu.Lock()
			a.routes[id] = route{engine: e, original: original, name: name}
			a.mu.Unlock()
			setField(item, "Id", id)
			setField(item, "Names", names)
			containers = append(containers, item)
		}
	}
	return containers, failed
}

func (a *aggregator) list(w http.ResponseWriter, r *http.Request, prefix string) {
	// ID/name filters refer to the virtual namespace, not upstream IDs.
	if r.URL.Query().Get("filters") != "" || r.URL.Query().Get("since") != "" || r.URL.Query().Get("before") != "" || r.URL.Query().Get("limit") != "" {
		writeJSON(w, http.StatusNotImplemented, map[string]string{"message": "filters, since, before and limit are not supported yet"})
		return
	}
	items, failed := a.collect(r.Context(), prefix, r.URL.RawQuery)
	if len(failed) > 0 {
		w.Header().Set("X-Docker-Aggregator-Unavailable", strings.Join(failed, ","))
	}
	if len(a.engines) == 0 || len(failed) == len(a.engines) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "no Docker engines available"})
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (a *aggregator) resolve(key string) (string, route, int) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if rt, ok := a.routes[key]; ok {
		return key, rt, http.StatusOK
	}
	var found route
	id := ""
	isHex := strings.Trim(key, "0123456789abcdef") == ""
	for candidate, rt := range a.routes {
		if rt.name == key || (len(key) >= 12 && isHex && strings.HasPrefix(candidate, key)) {
			if id != "" {
				return "", route{}, http.StatusConflict
			}
			id, found = candidate, rt
		}
	}
	if id == "" {
		return "", route{}, http.StatusNotFound
	}
	return id, found, http.StatusOK
}

func (a *aggregator) container(w http.ResponseWriter, r *http.Request, prefix, key, operation string) {
	id, rt, status := a.resolve(key)
	if status == http.StatusNotFound {
		_, failed := a.collect(r.Context(), prefix, "all=1")
		id, rt, status = a.resolve(key)
		if status == http.StatusNotFound && (len(failed) > 0 || len(a.engines) == 0) {
			status = http.StatusServiceUnavailable
		}
	}
	if status != http.StatusOK {
		writeJSON(w, status, map[string]string{"message": "container not found, ambiguous, or engine unavailable"})
		return
	}
	path := prefix + "/containers/" + url.PathEscape(rt.original) + "/" + operation
	if operation == "json" {
		var object map[string]json.RawMessage
		if err := rt.engine.get(r.Context(), path, r.URL.RawQuery, &object); err != nil {
			status := http.StatusBadGateway
			var upstream upstreamError
			if errors.As(err, &upstream) {
				status = upstream.status
			}
			writeJSON(w, status, map[string]string{"message": "upstream inspect failed"})
			return
		}
		setField(object, "Id", id)
		setField(object, "Name", "/"+rt.engine.name+"/"+strings.TrimPrefix(stringField(object, "Name"), "/"))
		writeJSON(w, http.StatusOK, object)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, "http://docker"+path, nil)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"message": "invalid upstream request"})
		return
	}
	req.URL.RawQuery = r.URL.RawQuery
	resp, err := rt.engine.client.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"message": "upstream stats failed"})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		writeJSON(w, resp.StatusCode, map[string]string{"message": "upstream stats failed"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	decoder := json.NewDecoder(resp.Body)
	encoder := json.NewEncoder(w)
	started := false
	for {
		var frame map[string]json.RawMessage
		if err := decoder.Decode(&frame); err != nil {
			if err != io.EOF && r.Context().Err() == nil {
				log.Printf("engine %s: stats decode: %v", rt.engine.name, err)
				if !started {
					writeJSON(w, http.StatusBadGateway, map[string]string{"message": "invalid upstream stats"})
				}
			}
			return
		}
		if frame == nil {
			return
		}
		setField(frame, "id", id)
		if name := stringField(frame, "name"); name != "" {
			setField(frame, "name", "/"+rt.engine.name+"/"+strings.TrimPrefix(name, "/"))
		}
		if err := encoder.Encode(frame); err != nil {
			return
		}
		started = true
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}
