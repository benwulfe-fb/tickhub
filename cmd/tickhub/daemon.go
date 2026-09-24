package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/metrics"
	"github.com/benwulfe-fb/tickhub/pkg/project"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

// FeedManager manages the dynamic lifecycle of the Massive.com WebSocket ingestion client.
type FeedManager struct {
	mu           sync.Mutex
	enabled      bool
	endpoint     string
	apiKey       string
	symbols      []string
	client       *feed.MassiveWSClient
	rootCtx      context.Context
	clientCtx    context.Context
	clientCancel context.CancelFunc
	tickCh       chan feed.Tick
	tickCount    atomic.Uint64
	wg           sync.WaitGroup
}

func NewFeedManager(rootCtx context.Context, endpoint, apiKey string, symbols []string) *FeedManager {
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	return &FeedManager{
		rootCtx:  rootCtx,
		endpoint: endpoint,
		apiKey:   apiKey,
		symbols:  symbols,
		tickCh:   make(chan feed.Tick, 65536),
	}
}

func (fm *FeedManager) FeedStatus() (enabled bool, ticks uint64) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.enabled, fm.tickCount.Load()
}

func (fm *FeedManager) Ticks() <-chan feed.Tick {
	return fm.tickCh
}

func (fm *FeedManager) EnableFeed(ctx context.Context) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.enabled {
		return nil
	}
	if fm.apiKey == "" {
		return fmt.Errorf("cannot enable feed: Massive.com API key is empty")
	}

	client := feed.NewMassiveWSClient(fm.endpoint, fm.apiKey, fm.symbols, 65536)
	clientCtx, clientCancel := context.WithCancel(fm.rootCtx)
	if err := client.Start(clientCtx); err != nil {
		clientCancel()
		return fmt.Errorf("massive ws connect: %w", err)
	}

	fm.client = client
	fm.clientCtx = clientCtx
	fm.clientCancel = clientCancel
	fm.enabled = true

	fm.wg.Add(1)
	go func(c *feed.MassiveWSClient, cCtx context.Context) {
		defer fm.wg.Done()
		ticks := c.Ticks()
		for {
			select {
			case <-cCtx.Done():
				return
			case tick, ok := <-ticks:
				if !ok {
					return
				}
				fm.tickCount.Add(1)
				select {
				case fm.tickCh <- tick:
				case <-cCtx.Done():
					return
				}
			}
		}
	}(client, clientCtx)

	log.Printf("[FEED] Massive WS feed ENABLED for %d symbols", len(fm.symbols))
	return nil
}

func (fm *FeedManager) DisableFeed(ctx context.Context) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if !fm.enabled {
		return nil
	}
	if fm.clientCancel != nil {
		fm.clientCancel()
		fm.clientCancel = nil
	}
	if fm.client != nil {
		fm.client.Close()
		fm.client = nil
	}
	fm.enabled = false
	fm.wg.Wait()
	log.Printf("[FEED] Massive WS feed DISABLED. Daemon running in standby mode.")
	return nil
}

func (fm *FeedManager) Close() {
	_ = fm.DisableFeed(context.Background())
}

func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	configPath := fs.String("config", "examples/config_datalake.yaml", "Path to config.yaml")
	apiKey := fs.String("api-key", "", "Massive.com API key (default: MASSIVE_API_KEY env)")
	apiKeyFile := fs.String("api-key-file", "", "Optional path to file containing Massive.com API key")
	shmNameOverride := fs.String("shm-name", "", "Optional SHM segment name override (e.g. tickhub_live)")
	wsEndpoint := fs.String("ws-endpoint", "", "Optional WebSocket endpoint override (default: wss://socket.massive.com/stocks)")
	metricsAddr := fs.String("metrics-addr", DefaultMetricsBind, "HTTP server address for /metrics, /healthz, /readyz, /control/feed")
	allowRecovery := fs.Bool("allow-recovery", true, "Attempt warm/resident recovery if SHM segment exists")
	noUnlink := fs.Bool("no-unlink", false, "Do not unlink SHM segment on exit")
	feedEnabled := fs.Bool("feed-enabled", false, "Enable live Massive.com WebSocket feed on boot (default false for safe standby)")
	fs.Parse(args)

	key := *apiKey
	if key == "" && *apiKeyFile != "" {
		if data, err := os.ReadFile(*apiKeyFile); err == nil {
			key = strings.TrimSpace(string(data))
		} else {
			log.Fatalf("[DAEMON] Failed to read API key file %s: %v", *apiKeyFile, err)
		}
	}
	if key == "" {
		key = os.Getenv("MASSIVE_API_KEY")
	}
	if key == "" && *feedEnabled {
		log.Fatalf("[DAEMON] Error: Massive.com API key required via --api-key, --api-key-file, or MASSIVE_API_KEY env")
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
		AllowRecovery:   *allowRecovery,
	}

	prod, recMode, err := shm.CreateProducerWithRecovery(shmCfg)
	if err != nil {
		log.Fatalf("[DAEMON] CreateProducer failed for %s: %v", shmName, err)
	}
	defer prod.Close()

	projector := project.NewProjector(prod, phaseConfigs, rawCfg.UniqueSymbols, shmCfg.CadenceInterval)
	projector.SetStartAnchor(time.Now().UnixNano())

	switch recMode {
	case shm.RecoveryModeWarmSubCadence:
		log.Printf("[RECOVERY] Probed /dev/shm/%s: Eligible for SUB-CADENCE recovery (downtime: %v). Seeding %d bars across %d symbols.",
			shmName, time.Duration(prod.DowntimeNS()), 60, len(rawCfg.UniqueSymbols))
		for pIdx, pCfg := range phaseConfigs {
			for sIdx := range pCfg.Symbols {
				bars := prod.ReadHistoryBars(pIdx, sIdx, 60)
				projector.SeedHistory(pIdx, sIdx, bars)
			}
		}
		projector.SetColdStart(false)
	case shm.RecoveryModeWarmResidentGap:
		log.Printf("[RECOVERY] Probed /dev/shm/%s: Eligible for RESIDENT GAP recovery (downtime: %v). Seeding prevailing snapshots and asserting FlagColdStart for 15s.",
			shmName, time.Duration(prod.DowntimeNS()))
		for symIdx, sym := range rawCfg.UniqueSymbols {
			snap := prod.Snapshot(symIdx)
			if snap != nil && (snap.LastTradePx > 0 || snap.Midprice > 0) {
				px := snap.LastTradePx
				if px <= 0 {
					px = snap.Midprice
				}
				projector.SeedPrevailingPrice(sym, px, snap.BidPx, snap.AskPx)
			}
		}
		projector.SetColdStart(true)
	default:
		log.Printf("[RECOVERY] Starting in COLD mode. FlagColdStart active for next 15 seconds.")
		projector.SetColdStart(true)
	}

	prod.SetStatus(shm.StatusRunning)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	feedMgr := NewFeedManager(ctx, *wsEndpoint, key, rawCfg.UniqueSymbols)
	defer feedMgr.Close()

	// Start metrics server
	metricsSrv := metrics.NewServer(*metricsAddr, prod.Header())
	metricsSrv.SetRecoveryStats(prod.DowntimeNS(), prod.MissedAnchors())
	metricsSrv.SetCommittedFramesFunc(projector.CommittedFrames)
	metricsSrv.SetFeedController(feedMgr)

	if err := metricsSrv.Start(); err != nil {
		log.Printf("[METRICS] Warning: failed to start HTTP server on %s: %v", *metricsAddr, err)
	} else {
		defer func() {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer stopCancel()
			_ = metricsSrv.Stop(stopCtx)
		}()
		log.Printf("[METRICS] Observability server listening on %s (/metrics, /healthz, /readyz, /control/feed)", *metricsAddr)
	}

	if *feedEnabled {
		log.Printf("[DAEMON] Booting with feed enabled (--feed-enabled=true). Ingesting %d symbols into /dev/shm/%s (PID: %d)",
			len(rawCfg.UniqueSymbols), shmName, os.Getpid())
		if err := feedMgr.EnableFeed(ctx); err != nil {
			log.Fatalf("[DAEMON] Initial feed connect failed: %v", err)
		}
	} else {
		log.Printf("[DAEMON] Booting in STANDBY mode (--feed-enabled=false). WebSocket connection held closed. Use 'tickhub feed enable' to activate.")
	}

	var tickCount uint64 = 0
	startTime := time.Now()
	lastReport := time.Now()

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	cadenceNS := shmCfg.CadenceInterval.Nanoseconds()
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}
	var lastFlushedAnchor int64 = 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("[DAEMON] Shutdown requested. Halting...")
			prod.SetStatus(shm.StatusClosed)
			return
		case tick := <-feedMgr.Ticks():
			tickCount++
			if err := projector.IngestTick(tick); err != nil {
				log.Printf("[DAEMON] Ingest error: %v", err)
			}
		case now := <-ticker.C:
			// Single-threaded event loop: zero data races with tick ingestion
			nowNS := now.UnixNano()
			prod.PublishTelemetry(0, 0, 0, tickCount)

			wallAnchor := (nowNS / cadenceNS) * cadenceNS
			if wallAnchor > lastFlushedAnchor {
				_ = projector.Flush(wallAnchor)
				lastFlushedAnchor = wallAnchor
			}

			if time.Since(lastReport) >= 5*time.Second {
				elapsed := now.Sub(startTime).Seconds()
				rate := float64(tickCount) / elapsed
				enabled, _ := feedMgr.FeedStatus()
				feedState := "ENABLED"
				if !enabled {
					feedState = "STANDBY"
				}
				log.Printf("[DAEMON] [%s] Ingested %d ticks (%.1f/sec), committed %d frames",
					feedState, tickCount, rate, projector.CommittedFrames())
				lastReport = now
			}
		}
	}
}
