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
