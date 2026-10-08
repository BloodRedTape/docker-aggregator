package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Disabler serializes runtime updates and reloads the file to retain user edits.
// TOML is re-encoded; comments and original formatting are not preserved.
func Disabler(path string) func(string, string, error) error {
	var mu sync.Mutex
	return func(name, socket string, reason error) error {
		mu.Lock()
		defer mu.Unlock()
		before, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		cfg, err := load(path)
		if err != nil {
			return err
		}
		found := false
		for i := range cfg.Engines {
			e := &cfg.Engines[i]
			if e.Name == name && e.Socket == socket {
				e.Enabled = false
				e.Note = "Disabled after engine failure: " + reason.Error()
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("engine %q no longer matches config", name)
		}
		body, err := Encode(cfg)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		file, err := os.CreateTemp(filepath.Dir(path), ".config-*")
		if err != nil {
			return err
		}
		defer os.Remove(file.Name())
		defer file.Close()
		if err = file.Chmod(info.Mode().Perm()); err != nil {
			return err
		}
		if _, err = file.Write(body); err != nil {
			return err
		}
		if err = file.Sync(); err != nil {
			return err
		}
		if err = file.Close(); err != nil {
			return err
		}
		current, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(before, current) {
			return fmt.Errorf("config changed during update; refusing to overwrite")
		}
		return os.Rename(file.Name(), path)
	}
}
