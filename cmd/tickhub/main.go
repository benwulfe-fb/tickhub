package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/project"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
	"gopkg.in/yaml.v3"
)

type ConfigFile struct {
	SHM struct {
		Name            string `yaml:"name"`
		MaxFrames       uint32 `yaml:"max_frames"`
		CadenceInterval int64  `yaml:"cadence_interval"`
	} `yaml:"shm"`
	Features      []string `yaml:"features"`
	UniqueSymbols []string `yaml:"unique_symbols"`
	Phases        []struct {
		ID       uint32   `yaml:"id"`
		Name     string   `yaml:"name"`
		OffsetMS uint32   `yaml:"offset_ms"`
		Symbols  []string `yaml:"symbols"`
	} `yaml:"phases"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case "replay":
		runReplay(os.Args[2:])
	case "worker":
		runWorker(os.Args[2:])
	case "daemon":
		runDaemon(os.Args[2:])
	case "relay-server":
		runRelayServer(os.Args[2:])
	case "relay-client":
		runRelayClient(os.Args[2:])
	case "feed":
		runFeedCtl(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	case "version":
		fmt.Println("TickHub v0.2.0 (Phase 3: Massive WS & TickHub Relay)")
	case "help", "-h", "--help":
		printUsage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: tickhub <command> [options]")
	fmt.Println("\nCommands:")
	fmt.Println("  daemon        Stream live market data from Massive.com WebSocket into SHM")
	fmt.Println("  feed          Manage live market data feed status, enable, or disable")
	fmt.Println("  status        Realtime ANSI terminal status inspection for /dev/shm")
	fmt.Println("  relay-server  Stream SHM frames and snapshots over TCP binary protocol")
	fmt.Println("  relay-client  Receive binary TCP stream and replicate into local SHM")
	fmt.Println("  replay        Stream historical Parquet ticks into SHM with lossless backpressure")
	fmt.Println("  worker        Run persistent DataLoader worker daemon listening on SHM control line")
	fmt.Println("  version       Print TickHub version")
}

func runReplay(args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	configPath := fs.String("config", "examples/config_datalake.yaml", "Path to config.yaml")
	datalakeDir := fs.String("datalake", "/mnt/wc/datalake", "Root datalake directory")
	dateStr := fs.String("date", "2026-05-06", "Date partition (YYYY-MM-DD)")
	shmNameOverride := fs.String("shm-name", "", "Optional SHM segment name override")
	maxTicks := fs.Int64("max-ticks", 0, "Stop after N ticks (0 = process all)")
	noUnlink := fs.Bool("no-unlink", false, "Do not unlink SHM segment on exit (keep resident in /dev/shm)")
	fs.Parse(args)

	cfgData, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("Failed to read config: %v", err)
	}

	var rawCfg ConfigFile
	if err := yaml.Unmarshal(cfgData, &rawCfg); err != nil {
		log.Fatalf("Failed to parse config YAML: %v", err)
	}

	// 1. Locate Parquet trade and quote files for all unique symbols
	dayDir := filepath.Join(*datalakeDir, *dateStr)
	var readers []feed.TickReader

	for _, sym := range rawCfg.UniqueSymbols {
		letter := strings.ToUpper(string(sym[0]))
		tradePath := filepath.Join(dayDir, letter, fmt.Sprintf("%s.trades.parquet", sym))
		quotePath := filepath.Join(dayDir, letter, fmt.Sprintf("%s.quotes.parquet", sym))

		if _, err := os.Stat(tradePath); err == nil {
			tr, err := feed.OpenTradeReader(tradePath, sym, 4096)
			if err != nil {
				log.Fatalf("Failed to open %s: %v", tradePath, err)
			}
			readers = append(readers, tr)
			log.Printf("[REPLAY] Ingesting trades: %s", tradePath)
		} else {
			log.Printf("[REPLAY] Warning: Trade file not found: %s", tradePath)
		}

		if _, err := os.Stat(quotePath); err == nil {
			qr, err := feed.OpenQuoteReader(quotePath, sym, 4096)
			if err != nil {
				log.Fatalf("Failed to open %s: %v", quotePath, err)
			}
			readers = append(readers, qr)
			log.Printf("[REPLAY] Ingesting quotes: %s", quotePath)
		} else {
			log.Printf("[REPLAY] Warning: Quote file not found: %s", quotePath)
		}
	}

	if len(readers) == 0 {
		log.Fatalf("No Parquet files found for symbols %v on date %s", rawCfg.UniqueSymbols, *dateStr)
	}

	// 2. Initialize K-way min-heap merger
	merger := feed.NewMerger(readers)
	defer merger.Close()

	// 3. Create Producer in ModeHistoricalReplay
	phaseConfigs := make([]shm.PhaseConfig, len(rawCfg.Phases))
	for i, p := range rawCfg.Phases {
		phaseConfigs[i] = shm.PhaseConfig{
			ID:       p.ID,
			Name:     p.Name,
			OffsetMS: p.OffsetMS,
			Symbols:  p.Symbols,
		}
	}

	shmName := rawCfg.SHM.Name
	if *shmNameOverride != "" {
		shmName = *shmNameOverride
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
		Mode:            shm.ModeHistoricalReplay,
	}

	prod, err := shm.CreateProducer(shmCfg)
	if err != nil {
		log.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()
	prod.SetStatus(shm.StatusRunning)

	// 4. Initialize 1Hz Projector
	projector := project.NewProjector(prod, phaseConfigs, rawCfg.UniqueSymbols, shmCfg.CadenceInterval)

	log.Printf("[REPLAY] Starting historical replay into /dev/shm/%s (Mode: Replay, Streams: %d)",
		shmCfg.Name, len(readers))

	startTime := time.Now()
	var tickCount int64 = 0
	var lastTS int64 = 0

	for merger.Next() {
		tick := merger.Tick()
		lastTS = tick.SIPTimestampNS
		if err := projector.IngestTick(tick); err != nil {
			log.Fatalf("Projector error @ tick %d (%s): %v", tickCount, tick.Symbol, err)
		}
		tickCount++

		if *maxTicks > 0 && tickCount >= *maxTicks {
			log.Printf("[REPLAY] Reached max ticks (%d), stopping stream.", *maxTicks)
			break
		}
	}

	if err := merger.Err(); err != nil {
		log.Printf("[REPLAY] Merger terminated with error: %v", err)
	}

	// Flush remaining bars
	if lastTS > 0 {
		_ = projector.Flush(lastTS)
	}

	elapsed := time.Since(startTime)
	rate := float64(tickCount) / elapsed.Seconds()
	log.Printf("[REPLAY] Completed replay of %d ticks in %v (%.1f ticks/sec). Setting StatusClosed.",
		tickCount, elapsed, rate)
	prod.SetStatus(shm.StatusClosed)
}
