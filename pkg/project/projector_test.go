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

func TestProjectorSnapshotPersistenceAndZeroAlloc(t *testing.T) {
	phases := []shm.PhaseConfig{
		{
			ID:       0,
			Name:     "phase_0ms",
			OffsetMS: 0,
			Symbols:  []string{"AAPL", "MSFT", "SPY"},
		},
	}
	uniqueSymbols := []string{"AAPL", "MSFT", "SPY"}
	features := []string{"log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"}

	cfg := shm.Config{
		Name:            "test_proj_snap",
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

	// 1. Seed AAPL
	proj.SeedState("AAPL", 150.00, 150.10, 149.95)
	snapAAPL := prod.Snapshot(0)
	if snapAAPL.BidPx != 150.00 || snapAAPL.AskPx != 150.10 || snapAAPL.Midprice != 150.05 || snapAAPL.LastTradePx != 149.95 {
		t.Fatalf("SeedState mismatch on AAPL: %+v", snapAAPL)
	}

	// 2. Ingest Quote on AAPL
	quoteTick := feed.Tick{
		SIPTimestampNS: 1700000000_100_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		BidPx:          150.10,
		AskPx:          150.20,
		BidSz:          10,
		AskSz:          20,
	}
	_ = proj.IngestTick(quoteTick)
	snapAAPL = prod.Snapshot(0)
	if snapAAPL.BidPx != 150.10 || snapAAPL.AskPx != 150.20 || math.Abs(snapAAPL.Midprice-150.15) > 1e-9 {
		t.Fatalf("Quote update failed: %+v", snapAAPL)
	}
	if snapAAPL.LastTradePx != 149.95 {
		t.Fatalf("Quote update wiped last trade price: %+v", snapAAPL)
	}

	// 3. Ingest Trade on AAPL
	tradeTick := feed.Tick{
		SIPTimestampNS: 1700000000_200_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickTrade,
		Price:          150.18,
		Size:           100,
	}
	_ = proj.IngestTick(tradeTick)
	snapAAPL = prod.Snapshot(0)
	if snapAAPL.LastTradePx != 150.18 || snapAAPL.LastTradeSz != 100 {
		t.Fatalf("Trade update failed: %+v", snapAAPL)
	}
	if snapAAPL.Conditions != 1 {
		t.Fatalf("Expected Conditions == 1 on trade, got %d", snapAAPL.Conditions)
	}
	// Regression assertion: trade must NOT wipe bid/ask/mid
	if snapAAPL.BidPx != 150.10 || snapAAPL.AskPx != 150.20 || math.Abs(snapAAPL.Midprice-150.15) > 1e-9 {
		t.Fatalf("REGRESSION: Trade tick wiped bid/ask/mid: %+v", snapAAPL)
	}

	// Subsequent quote must NOT increment Conditions and must NOT wipe last trade
	quote2 := feed.Tick{
		SIPTimestampNS: 1700000000_250_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		BidPx:          150.12,
		AskPx:          150.22,
		BidSz:          12,
		AskSz:          22,
	}
	_ = proj.IngestTick(quote2)
	snapAAPL = prod.Snapshot(0)
	if snapAAPL.Conditions != 1 {
		t.Fatalf("Quote must not increment Conditions: got %d", snapAAPL.Conditions)
	}
	if snapAAPL.LastTradePx != 150.18 || snapAAPL.LastTradeSz != 100 {
		t.Fatalf("Quote wiped last trade: %+v", snapAAPL)
	}

	// Second trade must increment Conditions to 2
	tradeTick2 := feed.Tick{
		SIPTimestampNS: 1700000000_260_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickTrade,
		Price:          150.19,
		Size:           50,
	}
	_ = proj.IngestTick(tradeTick2)
	snapAAPL = prod.Snapshot(0)
	if snapAAPL.Conditions != 2 {
		t.Fatalf("Expected Conditions == 2 on second trade, got %d", snapAAPL.Conditions)
	}

	// 4. Ingest on boundary symbol SPY (index len-1 = 2)
	spyQuote := feed.Tick{
		SIPTimestampNS: 1700000000_300_000_000,
		Symbol:         "SPY",
		Type:           feed.TickQuote,
		BidPx:          450.00,
		AskPx:          450.05,
		BidSz:          50,
		AskSz:          60,
	}
	_ = proj.IngestTick(spyQuote)
	snapSPY := prod.Snapshot(2)
	if snapSPY.BidPx != 450.00 || snapSPY.AskPx != 450.05 || math.Abs(snapSPY.Midprice-450.025) > 1e-9 {
		t.Fatalf("Boundary symbol snapshot mismatch: %+v", snapSPY)
	}

	// 5. Zero-allocation verification in hot path
	allocs := testing.AllocsPerRun(1000, func() {
		proj.updateSnapshot(0, quoteTick)
		proj.updateSnapshot(0, tradeTick)
	})
	if allocs != 0 {
		t.Fatalf("Expected 0 allocs in updateSnapshot, got %f", allocs)
	}
}

func TestProjectorBulkCommitAndZeroAlloc(t *testing.T) {
	nSym := 72
	symbols := make([]string, nSym)
	for i := 0; i < nSym; i++ {
		symbols[i] = "SYM" + string(rune('A'+i/26)) + string(rune('A'+i%26))
	}
	phases := []shm.PhaseConfig{
		{ID: 0, Name: "p0", OffsetMS: 0, Symbols: symbols},
	}
	features := []string{"log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"}
	cfg := shm.Config{
		Name:            "test_proj_bulk_alloc",
		MaxFrames:       64,
		Phases:          phases,
		UniqueSymbols:   symbols,
		Features:        features,
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}

	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	proj := NewProjector(prod, phases, symbols, 1*time.Second)
	t0 := int64(1700000000_000_000_000)
	proj.SetStartAnchor(t0)

	// Seed all symbols
	for _, s := range symbols {
		proj.SeedPrevailingPrice(s, 100.0, 100.0, 100.10)
	}

	// First bar: warm up history
	_ = proj.Flush(t0)

	// Second bar: move price on SYM0 (first symbol), leave SYM1 unchanged
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: t0 + 100_000_000,
		Symbol:         symbols[0],
		Type:           feed.TickTrade,
		Price:          105.0,
		Size:           50.0,
	})
	_ = proj.Flush(t0 + 1_000_000_000)

	// Check SHM frame layout directly
	raw := prod.Bytes()
	slot1 := uint32(((t0 + 1_000_000_000) / int64(time.Second)) & 63)
	hdr := prod.Header()
	frameOffset := hdr.Phases[0].RingOffsetBytes + uint64(slot1)*hdr.Phases[0].FrameStrideBytes

	// Symbol 0 return should be ln(105 / 100)
	expectedR1Sym0 := math.Log(105.0 / 100.0)
	feat0Offset := frameOffset + 64 + uint64(nSym)*8
	val0 := math.Float64frombits(uint64(raw[feat0Offset]) |
		uint64(raw[feat0Offset+1])<<8 |
		uint64(raw[feat0Offset+2])<<16 |
		uint64(raw[feat0Offset+3])<<24 |
		uint64(raw[feat0Offset+4])<<32 |
		uint64(raw[feat0Offset+5])<<40 |
		uint64(raw[feat0Offset+6])<<48 |
		uint64(raw[feat0Offset+7])<<56)
	if math.Abs(val0-expectedR1Sym0) > 1e-12 {
		t.Fatalf("expected sym0 return %f, got %f", expectedR1Sym0, val0)
	}

	// Symbol 1 return should be exact 0.0 (fast-path zero return on unchanged price)
	feat1Offset := frameOffset + 64 + uint64(nSym)*8 + 5*8
	val1 := math.Float64frombits(uint64(raw[feat1Offset]) |
		uint64(raw[feat1Offset+1])<<8 |
		uint64(raw[feat1Offset+2])<<16 |
		uint64(raw[feat1Offset+3])<<24 |
		uint64(raw[feat1Offset+4])<<32 |
		uint64(raw[feat1Offset+5])<<40 |
		uint64(raw[feat1Offset+6])<<48 |
		uint64(raw[feat1Offset+7])<<56)
	if val1 != 0.0 {
		t.Fatalf("expected sym1 return 0.0 on unchanged price, got %f", val1)
	}

	// Verify symbol anchors in SHM
	for s := 0; s < nSym; s++ {
		anchorOffset := frameOffset + 64 + uint64(s*8)
		anchorVal := int64(uint64(raw[anchorOffset]) |
			uint64(raw[anchorOffset+1])<<8 |
			uint64(raw[anchorOffset+2])<<16 |
			uint64(raw[anchorOffset+3])<<24 |
			uint64(raw[anchorOffset+4])<<32 |
			uint64(raw[anchorOffset+5])<<40 |
			uint64(raw[anchorOffset+6])<<48 |
			uint64(raw[anchorOffset+7])<<56)
		if anchorVal != t0+1_000_000_000 {
			t.Fatalf("symbol %d anchor mismatch: expected %d, got %d", s, t0+1_000_000_000, anchorVal)
		}
	}

	// Zero-allocation test: closePhase across 72 symbols must allocate 0 heap bytes
	curAnchor := t0 + 2_000_000_000
	allocs := testing.AllocsPerRun(1000, func() {
		_ = proj.closePhase(0, curAnchor)
		curAnchor += 1_000_000_000
	})
	if allocs != 0 {
		t.Fatalf("closePhase allocated on heap: %f allocs/run (expected 0)", allocs)
	}
}

func TestProjectorMicroSnapshot(t *testing.T) {
	phases := []shm.PhaseConfig{
		{ID: 0, Name: "phase_0ms", OffsetMS: 0, Symbols: []string{"AAPL"}},
	}
	cfg := shm.Config{
		Name:            "test_proj_micro",
		MaxFrames:       64,
		Phases:          phases,
		UniqueSymbols:   []string{"AAPL"},
		Features:        []string{"r1", "r5", "r15", "v1", "sp"},
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}
	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	proj := NewProjector(prod, phases, []string{"AAPL"}, 1*time.Second)
	t0 := int64(1700000000_000_000_000)

	// Step through 30 seconds (121 buckets of 250ms), increasing mid by +$0.10 every second
	for b := int64(0); b <= 120; b++ {
		ts := t0 + b*250_000_000
		sec := float64(b / 4)
		bid := 100.0 + sec*0.10
		ask := bid + 0.10
		_ = proj.IngestTick(feed.Tick{
			SIPTimestampNS: ts,
			Symbol:         "AAPL",
			Type:           feed.TickQuote,
			BidPx:          bid,
			AskPx:          ask,
		})
		if b == 116 { // 1s ago (within 3s and 5s windows): buy trade above mid
			_ = proj.IngestTick(feed.Tick{
				SIPTimestampNS: ts + 10_000_000,
				Symbol:         "AAPL",
				Type:           feed.TickTrade,
				Price:          103.10,
				Size:           100.0,
			})
		}
		if b == 104 { // 4s ago (within 5s window, outside 3s window): sell trade below mid
			_ = proj.IngestTick(feed.Tick{
				SIPTimestampNS: ts + 10_000_000,
				Symbol:         "AAPL",
				Type:           feed.TickTrade,
				Price:          bid,
				Size:           50.0,
			})
		}
	}

	micro := prod.MicroSnapshot(0)
	if micro == nil {
		t.Fatalf("MicroSnapshot(0) returned nil")
	}
	// At b=120 (30s): current mid is 103.05, 1s ago (b=116, sec=29) mid is 102.95, 5s ago (b=100, sec=25) mid is 102.55, 30s ago (b=0, sec=0) mid is 100.05
	if math.Abs(micro.Mid1sAgo-102.95) > 1e-6 {
		t.Fatalf("expected Mid1sAgo=102.95, got %f", micro.Mid1sAgo)
	}
	if math.Abs(micro.Mid5sAgo-102.55) > 1e-6 {
		t.Fatalf("expected Mid5sAgo=102.55, got %f", micro.Mid5sAgo)
	}
	if math.Abs(micro.Mid30sAgo-100.05) > 1e-6 {
		t.Fatalf("expected Mid30sAgo=100.05, got %f", micro.Mid30sAgo)
	}
	if math.Abs(micro.Bid5sAgo-102.50) > 1e-6 || math.Abs(micro.Ask5sAgo-102.60) > 1e-6 {
		t.Fatalf("expected Bid5sAgo=102.50 Ask5sAgo=102.60, got %f / %f", micro.Bid5sAgo, micro.Ask5sAgo)
	}
	if micro.QuoteVol5sBps <= 0 {
		t.Fatalf("expected positive QuoteVol5sBps, got %f", micro.QuoteVol5sBps)
	}
	if micro.TradeCount3s != 1 {
		t.Fatalf("expected TradeCount3s=1, got %d", micro.TradeCount3s)
	}
	if math.Abs(micro.TradeVol5s-150.0) > 1e-6 {
		t.Fatalf("expected TradeVol5s=150.0, got %f", micro.TradeVol5s)
	}
	// Imbalance: +100 buy - 50 sell = +50 / 150 = +1/3
	if math.Abs(micro.TradeImbalance5s-(1.0/3.0)) > 1e-6 {
		t.Fatalf("expected TradeImbalance5s=0.333333, got %f", micro.TradeImbalance5s)
	}
}

func TestProjectorCCM44LiveSubSecondPhaseNoClobber(t *testing.T) {
	phases := []shm.PhaseConfig{
		{ID: 0, Name: "phase_0ms", OffsetMS: 0, Symbols: []string{"AAPL"}},
		{ID: 1, Name: "phase_500ms", OffsetMS: 500, Symbols: []string{"AAPL"}},
	}
	feats := make([]string, 44)
	for i := range feats {
		feats[i] = "f"
	}
	cfg := shm.Config{
		Name:            "test_proj_ccm44_live",
		MaxFrames:       64,
		Phases:          phases,
		UniqueSymbols:   []string{"AAPL"},
		Features:        feats,
		CadenceInterval: 1 * time.Second,
		Mode:            shm.ModeLiveStreaming,
		UnlinkOnExit:    true,
	}
	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	proj := NewProjector(prod, phases, []string{"AAPL"}, 1*time.Second)
	proj.SetStartAnchor(99_000_000_000)

	// In [99.0s, 100.0s): quote + buy trade of size 50 @ 100.10
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: 99_500_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		BidPx:          100.0,
		AskPx:          100.10,
		BidSz:          10.0,
		AskSz:          10.0,
	})
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: 99_600_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickTrade,
		Price:          100.10,
		Size:           50.0,
	})

	// In [100.0s, 100.5s]: buy trade of size 200 @ 100.20 (belongs to [100.0s, 101.0s) bar, and terminal state @ 100.5s)
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: 100_200_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickTrade,
		Price:          100.20,
		Size:           200.0,
	})

	// Tick @ 101.2s: flushes 100.0s (phase_0ms) and closes AAPL's [100.0s, 101.0s) bar before phase_500ms (100.5s) commits
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: 101_200_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickTrade,
		Price:          101.10,
		Size:           300.0,
	})

	// Tick @ 101.6s: triggers Flush(100.6s), committing phase_500ms @ 100.5s
	_ = proj.IngestTick(feed.Tick{
		SIPTimestampNS: 101_600_000_000,
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		BidPx:          102.0,
		AskPx:          102.10,
		BidSz:          10.0,
		AskSz:          10.0,
	})

	barsP1 := prod.ReadHistoryBars(1, 0, 1)
	if len(barsP1) != 1 {
		t.Fatalf("expected 1 committed bar on phase_500ms, got %d", len(barsP1))
	}
	if barsP1[0].AnchorNS != 100_500_000_000 {
		t.Fatalf("expected phase_500ms anchor 100.5s, got %d", barsP1[0].AnchorNS)
	}
	// Slot 0 (buy_vol for [99.0s, 100.0s)) must be 50.0, not clobbered by 200.0 from [100.0s, 101.0s)
	if math.Abs(barsP1[0].Features[0]-50.0) > 1e-9 {
		t.Fatalf("expected phase_500ms @ 100.5s buy_vol=50.0 ([99s,100s)), got %f", barsP1[0].Features[0])
	}
	// Slot 35 (bar_trade_idx @ 100.0s) must be 1, Slot 43 (term_trade_idx @ 100.5s) must be 2
	if math.Abs(barsP1[0].Features[35]-1.0) > 1e-9 {
		t.Fatalf("expected bar_trade_idx=1.0 @ 100.0s, got %f", barsP1[0].Features[35])
	}
	if math.Abs(barsP1[0].Features[43]-2.0) > 1e-9 {
		t.Fatalf("expected term_trade_idx=2.0 @ 100.5s, got %f", barsP1[0].Features[43])
	}
	// Slot 40 (term_last_trade_px @ 100.5s) must be 100.20, not 101.10 from 101.2s
	if math.Abs(barsP1[0].Features[40]-100.20) > 1e-9 {
		t.Fatalf("expected term_last_trade_px=100.20 @ 100.5s, got %f", barsP1[0].Features[40])
	}
}



