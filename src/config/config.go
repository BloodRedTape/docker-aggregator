package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const DefaultPath = "/etc/docker-aggregator/config.toml"
const DefaultSocket = "/run/docker-aggregator/docker.sock"

type Config struct {
	Server  Server   `toml:"server"`
	Engines []Engine `toml:"engines"`
}

type Server struct {
	Socket string `toml:"socket"`
}

type Engine struct {
	Name    string `toml:"name"`
	Socket  string `toml:"socket"`
	Enabled bool   `toml:"enabled"`
	Note    string `toml:"note,omitempty"`
}

func Encode(cfg Config) ([]byte, error) {
	body, err := toml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	return append([]byte("# Generated on first start. Edit freely; existing configs are never overwritten.\n"), body...), nil
}

// LoadOrCreate only discovers engines when the config does not exist.
func LoadOrCreate(ctx context.Context, path string) (Config, bool, error) {
	cfg, err := load(path)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return cfg, false, err
	}
	cfg, err = Discover(ctx)
	if err != nil {
		return Config{}, false, err
	}
	body, err := Encode(cfg)
	if err != nil {
		return Config{}, false, err
	}
	if err := create(path, body); err != nil {
		if errors.Is(err, os.ErrExist) {
			// Another instance or the operator created the config during discovery.
			cfg, err = load(path)
			return cfg, false, err
		}
		return Config{}, false, err
	}
	return cfg, true, nil
}

func load(path string) (Config, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	decoder := toml.NewDecoder(bytes.NewReader(body)).DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate %s: %w", path, err)
	}
	return cfg, nil
}

func (cfg Config) Validate() error {
	if !filepath.IsAbs(cfg.Server.Socket) {
		return errors.New("server.socket must be an absolute path")
	}
	names, paths := map[string]string{}, map[string]bool{}
	for _, engine := range cfg.Engines {
		if strings.TrimSpace(engine.Name) == "" {
			return fmt.Errorf("engine at %q: name must not be empty", engine.Socket)
		}
		if strings.Contains(engine.Name, "/") {
			return fmt.Errorf("engine %q: name must not contain '/'", engine.Name)
		}
		if previous, exists := names[engine.Name]; exists {
			return fmt.Errorf("duplicate engine name %q for %q and %q: assign unique names in the config (create it manually if absent)", engine.Name, previous, engine.Socket)
		}
		if !filepath.IsAbs(engine.Socket) {
			return fmt.Errorf("engine %q: socket must be an absolute path", engine.Name)
		}
		path := filepath.Clean(engine.Socket)
		if paths[path] || path == filepath.Clean(cfg.Server.Socket) {
			return fmt.Errorf("engine %q: duplicate socket or server socket used as upstream", engine.Name)
		}
		names[engine.Name], paths[path] = engine.Socket, true
	}
	return nil
}

// create publishes a complete file atomically without replacing an existing one.
func create(path string, body []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".config-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := file.Chmod(0640); err != nil {
		return err
	}
	if _, err := file.Write(body); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Link(file.Name(), path)
}
