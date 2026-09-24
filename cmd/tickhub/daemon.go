package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/project"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	configPath := fs.String("config", "examples/config_datalake.yaml", "Path to config.yaml")
	apiKey := fs.String("api-key", "", "Massive.com API key (default: MASSIVE_API_KEY env)")
	shmNameOverride := fs.String("shm-name", "", "Optional SHM segment name override (e.g. tickhub_live)")
	wsEndpoint := fs.String("ws-endpoint", "", "Optional WebSocket endpoint override (default: wss://socket.massive.com/stocks)")
	noUnlink := fs.Bool("no-unlink", false, "Do not unlink SHM segment on exit")
	fs.Parse(args)

	key := *apiKey
	if key == "" {
		key = os.Getenv("MASSIVE_API_KEY")
	}
	if key == "" {
		log.Fatalf("[DAEMON] Error: Massive.com API key required via --api-key or MASSIVE_API_KEY env")
	}

	cfgData, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("[DAEMON] Failed to read config %s: %v", *configPath, err)
	}

	var rawCfg ConfigFile
	if err := yaml.Unmarshal(cfgData, &rawCfg); err != nil {
		log.Fatalf("[DAEMON] Failed to parse config YAML: %v", err)
	}

	shmName := rawCfg.SHM.Name
	if *shmNameOverride != "" {
		shmName = *shmNameOverride
	}

	phaseConfigs := make([]shm.PhaseConfig, len(rawCfg.Phases))
	for i, p := range rawCfg.Phases {
		phaseConfigs[i] = shm.PhaseConfig{
			ID:       p.ID,
			Name:     p.Name,
			OffsetMS: p.OffsetMS,
			Symbols:  p.Symbols,
		}
	}

	shmCfg := shm.Config{
		Name:            shmName,
		MaxFrames:       rawCfg.SHM.MaxFrames,
		CadenceInterval: time.Duration(rawCfg.SHM.CadenceInterval) * time.Nanosecond,
		UniqueSymbols:   rawCfg.UniqueSymbols,
		Features:        rawCfg.Features,
		Phases:          phaseConfigs,
		Permissions:     0666,
		UnlinkOnExit:    !*noUnlink,
		Mode:            shm.ModeLiveStreaming,
	}

	prod, err := shm.CreateProducer(shmCfg)
	if err != nil {
		log.Fatalf("[DAEMON] CreateProducer failed for %s: %v", shmName, err)
	}
	defer prod.Close()
	prod.SetStatus(shm.StatusRunning)

	projector := project.NewProjector(prod, phaseConfigs, rawCfg.UniqueSymbols, shmCfg.CadenceInterval)

	client := feed.NewMassiveWSClient(*wsEndpoint, key, rawCfg.UniqueSymbols, 65536)
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("[DAEMON] Connecting to Massive WS. Ingesting %d symbols into /dev/shm/%s (PID: %d)",
		len(rawCfg.UniqueSymbols), shmName, os.Getpid())

	if err := client.Start(ctx); err != nil {
		log.Fatalf("[DAEMON] Connect failed: %v", err)
	}
	log.Printf("[DAEMON] Authenticated to Massive WS. Subscribed to %d symbols. Streaming to /dev/shm/%s",
		len(rawCfg.UniqueSymbols), shmName)

	var tickCount uint64 = 0
	startTime := time.Now()
	lastReport := time.Now()
	ticks := client.Ticks()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[DAEMON] Shutdown requested. Halting...")
			prod.SetStatus(shm.StatusClosed)
			return
		case tick, ok := <-ticks:
			if !ok {
				log.Printf("[DAEMON] Tick channel closed. Halting...")
				prod.SetStatus(shm.StatusClosed)
				return
			}
			tickCount++

			if err := projector.IngestTick(tick); err != nil {
				log.Printf("[DAEMON] Ingest error: %v", err)
			}

			if time.Since(lastReport) >= 5*time.Second {
				now := time.Now()
				elapsed := now.Sub(startTime).Seconds()
				rate := float64(tickCount) / elapsed
				prod.PublishTelemetry(0, 0, 0, tickCount)
				log.Printf("[DAEMON] Ingested %d ticks (%.1f/sec), committed %d frames",
					tickCount, rate, projector.CommittedFrames())
				lastReport = now
			}
		}
	}
}
