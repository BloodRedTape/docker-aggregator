package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

var metadataFields = map[string][]string{
	"/info":    {"OperatingSystem", "KernelVersion", "OSType", "Architecture", "NCPU", "MemTotal"},
	"/version": {"Version", "ApiVersion", "MinAPIVersion", "Os", "Arch", "KernelVersion"},
}

type metadataResult struct {
	engine *engine
	body   []byte
	err    error
}

// metadata compares only public platform/version fields, not daemon-specific IDs,
// storage paths, container counts or rootless security settings.
func (a *aggregator) metadata(w http.ResponseWriter, r *http.Request, prefix, endpoint string) {
	var engines []*engine
	for _, e := range a.engines {
		if !e.unavailable() {
			engines = append(engines, e)
		}
	}
	if len(engines) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "no Docker engines available"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
	defer cancel()
	results := make(chan metadataResult, len(engines))
	for _, e := range engines {
		go func(e *engine) {
			var object map[string]json.RawMessage
			err := e.get(ctx, prefix+endpoint, "", &object)
			var body []byte
			if err == nil {
				body, err = projectMetadata(object, metadataFields[endpoint])
			}
			results <- metadataResult{engine: e, body: body, err: err}
		}(e)
	}
	responses := make([]metadataResult, 0, len(engines))
	for range engines {
		result := <-results
		if result.err != nil {
			// Use the downstream context: our own deadline is an engine failure,
			// whereas a disconnected client must not mark engines unavailable.
			a.fail(r.Context(), result.engine, result.err)
			continue
		}
		responses = append(responses, result)
	}
	if r.Context().Err() != nil {
		return
	}
	if len(responses) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": "no Docker engines available"})
		return
	}
	reference := responses[0].body
	mismatch := false
	for _, result := range responses[1:] {
		if !bytes.Equal(reference, result.body) {
			mismatch = true
		}
	}
	if mismatch {
		err := fmt.Errorf("%s metadata mismatch across enabled engines", endpoint)
		for _, result := range responses {
			log.Printf("engine %s: %s compared response: %s", result.engine.name, endpoint, result.body)
			a.disableEngine(result.engine, err)
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"message": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(reference)
}

func projectMetadata(object map[string]json.RawMessage, fields []string) ([]byte, error) {
	projected := make(map[string]any, len(fields))
	for _, key := range fields {
		raw, exists := object[key]
		if !exists {
			return nil, fmt.Errorf("metadata field %s is missing", key)
		}
		if key == "NCPU" || key == "MemTotal" {
			var value uint64
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return nil, fmt.Errorf("metadata field %s is null", key)
			}
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, fmt.Errorf("invalid metadata field %s: %w", key, err)
			}
			projected[key] = value
		} else {
			var value string
			if err := json.Unmarshal(raw, &value); err != nil || value == "" {
				return nil, fmt.Errorf("metadata field %s must be a non-empty string", key)
			}
			projected[key] = value
		}
	}
	return json.Marshal(projected)
}
