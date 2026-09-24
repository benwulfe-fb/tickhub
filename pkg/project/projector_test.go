package project

import (
	"math"
	"testing"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func TestProjectorMathAndMultiPhase(t *testing.T) {
	phases := []shm.PhaseConfig{
		{
			ID:       0,
			Name:     "phase_0ms",
			OffsetMS: 0,
			Symbols:  []string{"SPY", "AAPL"},
		},
		{
			ID:       1,
			Name:     "phase_500ms",
			OffsetMS: 500,
			Symbols:  []string{"SPY"}, // Cross asset in both phases
		},
	}

	uniqueSymbols := []string{"SPY", "AAPL"}
	features := []string{"log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"}

	cfg := shm.Config{
		Name:            "test_proj",
		MaxFrames:       32,
		Phases:          phases,
		UniqueSymbols:   uniqueSymbols,
		Features:        features,
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}

	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	proj := NewProjector(prod, phases, uniqueSymbols, 1*time.Second)

	baseTime := int64(1700000000_000_000_000)

	// Tick 1: SPY quote @ baseTime - 500ms
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: baseTime - 500_000_000,
		Symbol:         "SPY",
		Type:           feed.TickQuote,
		BidPx:          100.0,
		AskPx:          100.10,
	})

	// Tick 2: SPY trade @ baseTime - 100ms
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: baseTime - 100_000_000,
		Symbol:         "SPY",
		Type:           feed.TickTrade,
		Price:          100.05,
		Size:           50.0,
	})

	// Tick 3: crosses Phase 0 anchor (baseTime) and Phase 1 anchor (baseTime + 500ms)
	// SPY trade @ baseTime + 600ms at price 102.00, size 100
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: baseTime + 600_000_000,
		Symbol:         "SPY",
		Type:           feed.TickTrade,
		Price:          102.00,
		Size:           100.0,
	})

	// Phase 0 @ baseTime should be committed
	// Check Phase 0 slot memory for SPY
	slot0 := (baseTime / int64(time.Second)) & 31
	p0Offset := prod.Header().Phases[0].RingOffsetBytes + uint64(slot0)*prod.Header().Phases[0].FrameStrideBytes
	raw := prod.Bytes()
	featsP0 := make([]float64, 5)
	for i := 0; i < 5; i++ {
		featsP0[i] = math.Float64frombits(uint64(raw[p0Offset+64+2*8+uint64(i*8)]) |
			uint64(raw[p0Offset+64+2*8+uint64(i*8)+1])<<8 |
			uint64(raw[p0Offset+64+2*8+uint64(i*8)+2])<<16 |
			uint64(raw[p0Offset+64+2*8+uint64(i*8)+3])<<24 |
			uint64(raw[p0Offset+64+2*8+uint64(i*8)+4])<<32 |
			uint64(raw[p0Offset+64+2*8+uint64(i*8)+5])<<40 |
			uint64(raw[p0Offset+64+2*8+uint64(i*8)+6])<<48 |
			uint64(raw[p0Offset+64+2*8+uint64(i*8)+7])<<56)
	}

	vol1s := featsP0[3]
	if vol1s != 50.0 {
		t.Errorf("expected SPY Phase 0 vol1s=50, got %f", vol1s)
	}
}

func TestProjectorSeedHistoryAndColdStart(t *testing.T) {
	phases := []shm.PhaseConfig{
		{
			ID:       0,
			Name:     "phase_0ms",
			OffsetMS: 0,
			Symbols:  []string{"AAPL"},
		},
	}
	uniqueSymbols := []string{"AAPL"}
	features := []string{"log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"}

	cfg := shm.Config{
		Name:            "test_proj_seed",
		MaxFrames:       64,
		Phases:          phases,
		UniqueSymbols:   uniqueSymbols,
		Features:        features,
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}

	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	proj := NewProjector(prod, phases, uniqueSymbols, 1*time.Second)
	baseTime := int64(1700000000_000_000_000)

	// Produce 20 frames with price = 100.0 + i*1.0
	for i := 0; i <= 20; i++ {
		ts := baseTime + int64(i)*1_000_000_000 - 100_000_000
		px := 100.0 + float64(i)*1.0
		_ = proj.IngestTick(feed.Tick{
			SIPTimestampNS: ts,
			Symbol:         "AAPL",
			Type:           feed.TickTrade,
			Price:          px,
			Size:           10.0,
		})
	}
	// Flush to commit frame 20
	_ = proj.Flush(baseTime + 20*1_000_000_000)

	bars := prod.ReadHistoryBars(0, 0, 20)
	if len(bars) < 20 {
		t.Fatalf("expected 20 history bars, got %d", len(bars))
	}

	// Create second projector and seed
	proj2 := NewProjector(prod, phases, uniqueSymbols, 1*time.Second)
	proj2.SeedHistory(0, 0, bars)

	// Ingest tick 21 into both
	ts21 := baseTime + 21*1_000_000_000 - 100_000_000
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: ts21,
		Symbol:         "AAPL",
		Type:           feed.TickTrade,
		Price:          125.0,
		Size:           10.0,
	})
	_ = proj.Flush(baseTime + 21*1_000_000_000)
	barsProj1 := prod.ReadHistoryBars(0, 0, 1)

	_ = proj2.IngestTick(feed.Tick{
		SIPTimestampNS: ts21,
		Symbol:         "AAPL",
		Type:           feed.TickTrade,
		Price:          125.0,
		Size:           10.0,
	})
	_ = proj2.Flush(baseTime + 21*1_000_000_000)
	barsProj2 := prod.ReadHistoryBars(0, 0, 1)

	ret15s_1 := barsProj1[0].Features[2]
	ret15s_2 := barsProj2[0].Features[2]
	if math.Abs(ret15s_1-ret15s_2) > 1e-9 {
		t.Fatalf("ret15s mismatch between continuous and seeded: %f vs %f", ret15s_1, ret15s_2)
	}

	// Test ColdStart countdown
	proj3 := NewProjector(prod, phases, uniqueSymbols, 1*time.Second)
	proj3.SetColdStart(true)
	if !proj3.IsColdStart() {
		t.Fatalf("expected IsColdStart to be true")
	}

	for i := 1; i <= 15; i++ {
		anchor := baseTime + int64(100+i)*1_000_000_000
		_ = proj3.IngestTick(feed.Tick{
			SIPTimestampNS: anchor - 100_000_000,
			Symbol:         "AAPL",
			Type:           feed.TickTrade,
			Price:          100.0,
			Size:           5.0,
		})
		_ = proj3.Flush(anchor)
	}
	if proj3.IsColdStart() {
		t.Fatalf("expected IsColdStart to be false after 15 bars")
	}
}

