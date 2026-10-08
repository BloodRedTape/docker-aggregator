package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"docker-aggregator/src/api"
	"docker-aggregator/src/config"
	"docker-aggregator/src/server"
)

func main() {
	if err := run(); err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", config.DefaultPath, "TOML config path (created if absent)")
	socket := flag.String("socket", "", "Override configured Unix socket path")
	discover := flag.Bool("discover", false, "Print discovered configuration without writing files")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *discover {
		cfg, err := config.Discover(ctx)
		if err != nil {
			return err
		}
		body, err := config.Encode(cfg)
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(body)
		return err
	}

	cfg, created, err := config.LoadOrCreate(ctx, *configPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if created {
		log.Printf("created config: %s", *configPath)
	}
	if *socket != "" {
		cfg.Server.Socket = *socket
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	enabled := 0
	for _, engine := range cfg.Engines {
		if engine.Enabled {
			enabled++
		}
	}
	if enabled == 0 {
		log.Printf("warning: no enabled Docker engines; edit %s", *configPath)
	}
	log.Printf("configured engines: %d enabled, %d total", enabled, len(cfg.Engines))
	return server.Run(ctx, cfg.Server.Socket, api.NewHandler(cfg.Engines))
}
