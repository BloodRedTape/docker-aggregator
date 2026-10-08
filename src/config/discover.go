package config

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"
)

// Discover probes standard paths only. Nonstandard sockets can be added manually.
func Discover(ctx context.Context) (Config, error) {
	cfg := Config{Server: Server{Socket: DefaultSocket}}
	paths, err := filepath.Glob("/run/user/*/docker.sock")
	if err != nil {
		return Config{}, err
	}
	paths = append([]string{"/var/run/docker.sock"}, paths...)
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return Config{}, err
		}
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err == nil && info.Mode()&os.ModeSocket == 0 {
			log.Printf("discovery: ignoring non-socket %s", path)
			continue
		}
		engine := Engine{Name: engineName(path), Socket: path}
		if err == nil {
			err = ping(ctx, path)
		}
		if err != nil {
			engine.Note = "Discovery failed: " + err.Error()
			log.Printf("discovery: disabled %s: %v", path, err)
		} else {
			engine.Enabled = true
		}
		cfg.Engines = append(cfg.Engines, engine)
	}
	if err := ctx.Err(); err != nil {
		return Config{}, err
	}
	return cfg, cfg.Validate()
}

func engineName(path string) string {
	if path == "/var/run/docker.sock" {
		return "rootful"
	}
	uid := filepath.Base(filepath.Dir(path))
	if account, err := user.LookupId(uid); err == nil {
		return account.Username
	}
	return "user-" + uid
}

func ping(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
		DisableKeepAlives: true,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != "OK" {
		return fmt.Errorf("unexpected Docker ping response: status %d, body %q", resp.StatusCode, body)
	}
	return nil
}
