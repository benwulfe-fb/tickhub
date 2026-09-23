package main

import (
	"flag"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	configPath := flag.String("config", "examples/config.yaml", "Path to YAML configuration")
	maxSeconds := flag.Int("seconds", 0, "Stop after N seconds (0 = run indefinitely)")
	flag.Parse()

	data, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("Failed to read config file %s: %v", *configPath, err)
	}

	var rawCfg ConfigFile
	if err := yaml.Unmarshal(data, &rawCfg); err != nil {
		log.Fatalf("Failed to parse YAML: %v", err)
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

	cfg := shm.Config{
		Name:            rawCfg.SHM.Name,
		MaxFrames:       rawCfg.SHM.MaxFrames,
		CadenceInterval: time.Duration(rawCfg.SHM.CadenceInterval) * time.Nanosecond,
		UniqueSymbols:   rawCfg.UniqueSymbols,
		Features:        rawCfg.Features,
		Phases:          phaseConfigs,
		Permissions:     0666,
		UnlinkOnExit:    true,
	}

	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create SHM producer: %v", err)
	}
	defer prod.Close()

	prod.SetStatus(shm.StatusRunning)
	log.Printf("[PRODUCER] Started TickHub SHM producer on /dev/shm/%s with %d phases, %d unique symbols",
		rawCfg.SHM.Name, len(phaseConfigs), len(rawCfg.UniqueSymbols))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	cadenceNS := int64(time.Second)
	numFeats := len(rawCfg.Features)

	// Simulated price tracking
	prices := map[string]float64{
		"SPY": 500.0, "QQQ": 440.0, "AAPL": 180.0, "MSFT": 420.0, "NVDA": 120.0, "AMZN": 185.0,
	}

	startWall := time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// Track last committed anchor for each phase
	lastCommitted := make(map[uint32]int64)

	stepCount := 0
	for {
		select {
		case <-sigCh:
			log.Println("[PRODUCER] Shutting down...")
			return
		case now := <-ticker.C:
			nowNS := now.UnixNano()

			// 1. Update Top-of-Book Snapshots (SeqLock)
			for i, sym := range rawCfg.UniqueSymbols {
				basePx := prices[sym]
				// Small pseudo-random walk
				drift := math.Sin(float64(nowNS)/1e9+float64(i)) * 0.05
				px := basePx + drift

				prod.WriteSnapshot(i, &shm.SymbolSnapshot{
					SIPTimestampNS:  nowNS - 500_000, // 500us simulated network transit
					RecvTimestampNS: nowNS,
					BidPx:           px - 0.01,
					AskPx:           px + 0.01,
					BidSz:           100.0,
					AskSz:           100.0,
					LastTradePx:     px,
					LastTradeSz:     10.0,
					Midprice:        px,
					Spread:          0.02,
					BidExch:         1,
					AskExch:         2,
					TradeExch:       1,
				})
			}

			// 2. Publish Telemetry
			prod.PublishTelemetry(1_500_000, 10_000_000, 0, uint64(stepCount*100))

			// 3. Check and commit eligible phases
			for pIdx, p := range rawCfg.Phases {
				offsetNS := int64(p.OffsetMS) * 1_000_000
				// Anchor lattice: floor to cadence + offset
				anchorNS := ((nowNS - offsetNS) / cadenceNS) * cadenceNS + offsetNS

				// Watermark delay: 10ms after anchor
				if nowNS >= anchorNS+10_000_000 && lastCommitted[p.ID] < anchorNS {
					// Commit metrics for all symbols in this phase
					for sIdx, sym := range p.Symbols {
						px := prices[sym]
						feats := make([]float64, numFeats)
						// Simulated return features
						feats[0] = math.Sin(float64(anchorNS)/1e9) * 0.001       // ret_1s
						feats[1] = math.Cos(float64(anchorNS)/1e9) * 0.002       // ret_5s
						feats[2] = 0.003                                         // ret_15s
						feats[3] = 1500.0 + float64(sIdx)*100.0                  // vol_1s
						feats[4] = 0.02 / px * 10000.0                           // spread_bps

						_ = prod.CommitSymbolMetrics(pIdx, sIdx, anchorNS, feats)
					}

					prod.CommitFrameFinalize(anchorNS)
					lastCommitted[p.ID] = anchorNS

					if p.ID == 0 {
						log.Printf("[PRODUCER] Committed %s @ anchor=%d (%s)",
							p.Name, anchorNS, time.Unix(0, anchorNS).UTC().Format("15:04:05.000"))
					}
				}
			}

			stepCount++
			if *maxSeconds > 0 && time.Since(startWall) >= time.Duration(*maxSeconds)*time.Second {
				log.Println("[PRODUCER] Reached max seconds, stopping demo producer.")
				return
			}
		}
	}
}
