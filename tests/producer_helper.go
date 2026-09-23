package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func main() {
	shmName := flag.String("shm", "tickhub_test_cross", "SHM segment name")
	frames := flag.Int("frames", 10, "Number of frames to produce")
	intervalMS := flag.Int("interval", 100, "Interval between frames in milliseconds")
	daemonMode := flag.Bool("daemon", false, "Keep running until SIGINT/SIGTERM")
	flag.Parse()

	uniqueSymbols := []string{"SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMZN"}
	features := []string{"ret_1s", "ret_5s", "ret_15s", "vol_1s", "spread_bps"}

	cfg := shm.Config{
		Name:            *shmName,
		MaxFrames:       256,
		CadenceInterval: 1 * time.Second,
		UniqueSymbols:   uniqueSymbols,
		Features:        features,
		Permissions:     0666,
		UnlinkOnExit:    true,
		Phases: []shm.PhaseConfig{
			{
				ID:       0,
				Name:     "phase_0ms",
				OffsetMS: 0,
				Symbols:  []string{"SPY", "QQQ", "AAPL", "MSFT"},
			},
			{
				ID:       1,
				Name:     "phase_500ms",
				OffsetMS: 500,
				Symbols:  []string{"SPY", "QQQ", "NVDA", "AMZN"},
			},
		},
	}

	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create producer: %v\n", err)
		os.Exit(1)
	}
	defer prod.Close()

	prod.SetStatus(shm.StatusRunning)
	prod.PublishTelemetry(1_500_000, 10_000_000, 0, 1000)

	// Write snapshots for all symbols
	for i, sym := range uniqueSymbols {
		basePx := 100.0 + float64(i)*50.0
		prod.WriteSnapshot(i, &shm.SymbolSnapshot{
			SIPTimestampNS:  time.Now().UnixNano(),
			RecvTimestampNS: time.Now().UnixNano(),
			BidPx:           basePx,
			AskPx:           basePx + 0.05,
			BidSz:           10.0,
			AskSz:           15.0,
			LastTradePx:     basePx + 0.02,
			LastTradeSz:     5.0,
			Midprice:        basePx + 0.025,
			Spread:          0.05,
			BidExch:         1,
			AskExch:         2,
			TradeExch:       1,
			Conditions:      0,
		})
		_ = sym
	}

	startAnchor := int64(1700000000_000_000_000)

	// Produce frames
	writeFrame := func(f int) {
		// Phase 0
		anchorP0 := startAnchor + int64(f)*1_000_000_000
		for sIdx, sym := range cfg.Phases[0].Symbols {
			feats := make([]float64, len(features))
			for featIdx := range feats {
				feats[featIdx] = float64(anchorP0) + float64(sIdx)*1000.0 + float64(featIdx)*0.1
			}
			if err := prod.CommitSymbolMetrics(0, sIdx, anchorP0, feats); err != nil {
				fmt.Fprintf(os.Stderr, "commit p0 error: %v\n", err)
			}
			_ = sym
		}
		prod.CommitFrameFinalize(anchorP0)

		// Phase 1 (offset 500ms)
		anchorP1 := anchorP0 + 500_000_000
		for sIdx, sym := range cfg.Phases[1].Symbols {
			feats := make([]float64, len(features))
			for featIdx := range feats {
				feats[featIdx] = float64(anchorP1) + float64(sIdx)*1000.0 + float64(featIdx)*0.1
			}
			if err := prod.CommitSymbolMetrics(1, sIdx, anchorP1, feats); err != nil {
				fmt.Fprintf(os.Stderr, "commit p1 error: %v\n", err)
			}
			_ = sym
		}
		prod.CommitFrameFinalize(anchorP1)
	}

	for f := 0; f < *frames; f++ {
		writeFrame(f)
		if *intervalMS > 0 {
			time.Sleep(time.Duration(*intervalMS) * time.Millisecond)
		}
	}

	fmt.Println("READY")

	if *daemonMode {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
	} else {
		// Wait a bit before exiting so test can read
		time.Sleep(3 * time.Second)
	}
}

func init() {
	_ = math.Pi
}
