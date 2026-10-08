// Package server runs an HTTP server on a Unix domain socket (Linux/Unix).
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Run serves until ctx is cancelled. Existing sockets/files are never removed
// at startup: an occupied path must be handled explicitly by the operator.
func Run(ctx context.Context, socketPath string, handler http.Handler) error {
	if socketPath == "" {
		return errors.New("socket path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0750); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}

	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return fmt.Errorf("listen on %s: %w", socketPath, err)
	}
	// Go's Unix listener unlinks its socket when closed, including on errors.
	defer listener.Close()
	if err := os.Chmod(socketPath, 0660); err != nil {
		return fmt.Errorf("set socket permissions: %w", err)
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	result := make(chan error, 1)
	go func() { result <- srv.Serve(listener) }()
	log.Printf("listening on unix://%s", socketPath)

	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	}
}
