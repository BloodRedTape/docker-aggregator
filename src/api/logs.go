package api

import (
	"io"
	"net/http"
)

// logs preserves Docker's multiplexed framing and raw TTY output. Only selected
// response headers are forwarded; downstream credentials never reach Docker.
func (a *aggregator) logs(w http.ResponseWriter, r *http.Request, rt route, path string) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, "http://docker"+path, nil)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"message": "invalid upstream request"})
		return
	}
	req.URL.RawQuery = r.URL.RawQuery
	resp, err := rt.engine.client.Do(req)
	if err != nil {
		a.fail(r.Context(), rt.engine, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"message": "upstream logs failed"})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		a.fail(r.Context(), rt.engine, upstreamError{status: resp.StatusCode})
	}
	for _, key := range []string{"Content-Type", "Content-Length"} {
		if value := resp.Header.Get(key); value != "" {
			w.Header().Set(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	buffer := make([]byte, 32*1024)
	for {
		n, readErr := resp.Body.Read(buffer)
		if n > 0 {
			if _, err := w.Write(buffer[:n]); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				a.fail(r.Context(), rt.engine, readErr)
			}
			return
		}
	}
}
