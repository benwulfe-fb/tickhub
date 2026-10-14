package project

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

// TestLiveReorderOutOfOrderAndReplayParity verifies bit-identity between
// out-of-order live ingestion (watermark 100ms) and sorted historical replay.
func TestLiveReorderOutOfOrderAndReplayParity(t *testing.T) {
	shmNameLive := fmt.Sprintf("test_reorder_live_%d", os.Getpid())
	shmNameReplay := fmt.Sprintf("test_reorder_replay_%d", os.Getpid())

	phases := []shm.PhaseConfig{
		{ID: 0, Name: "phase_0ms", OffsetMS: 0, Symbols: []string{"AAPL"}},
	}
	feats := make([]string, 44)
	for i := range feats {
		feats[i] = fmt.Sprintf("f%d", i)
	}

	cfgLive := shm.Config{
		Name:            shmNameLive,
		MaxFrames:       64,
		Phases:          phases,
		UniqueSymbols:   []string{"AAPL"},
		Features:        feats,
		CadenceInterval: 1 * time.Second,
		Mode:            shm.ModeLiveStreaming,
		UnlinkOnExit:    true,
	}
	prodLive, err := shm.CreateProducer(cfgLive)
	if err != nil {
		t.Fatalf("CreateProducer Live failed: %v", err)
	}
	defer prodLive.Close()

	cfgReplay := shm.Config{
		Name:            shmNameReplay,
		MaxFrames:       64,
		Phases:          phases,
		UniqueSymbols:   []string{"AAPL"},
		Features:        feats,
		CadenceInterval: 1 * time.Second,
		Mode:            shm.ModeHistoricalReplay,
		UnlinkOnExit:    true,
	}
	prodReplay, err := shm.CreateProducer(cfgReplay)
	if err != nil {
		t.Fatalf("CreateProducer Replay failed: %v", err)
	}
	defer prodReplay.Close()

	projLive := NewProjector(prodLive, phases, []string{"AAPL"}, 1*time.Second)
	projLive.SetWatermarkBuffer(100 * time.Millisecond)

	projReplay := NewProjector(prodReplay, phases, []string{"AAPL"}, 1*time.Second)

	baseTS := int64(1_700_000_000_000_000_000)
	projLive.SetStartAnchor(baseTS)
	projReplay.SetStartAnchor(baseTS)

	// Generate 100 ticks across 4 seconds [baseTS, baseTS + 4s)
	// Quotes and trades with varying prices and sizes
	numTicks := 100
	ticks := make([]feed.Tick, numTicks)
	for i := 0; i < numTicks; i++ {
		ts := baseTS + int64(i)*40_000_000 // every 40ms
		if i%3 == 0 {
			ticks[i] = feed.Tick{
				Symbol:         "AAPL",
				Type:           feed.TickTrade,
				SIPTimestampNS: ts,
				Price:          150.0 + float64(i)*0.01,
				Size:           10.0 + float64(i%5)*10.0,
			}
		} else {
			ticks[i] = feed.Tick{
				Symbol:         "AAPL",
				Type:           feed.TickQuote,
				SIPTimestampNS: ts,
				BidPx:          150.0 + float64(i)*0.01 - 0.05,
				AskPx:          150.0 + float64(i)*0.01 + 0.05,
				BidSz:          100 + float64(i%10)*10,
				AskSz:          200 + float64(i%10)*10,
			}
		}
	}

	// Ingest chronologically into Replay
	for _, tick := range ticks {
		if err := projReplay.IngestTick(tick); err != nil {
			t.Fatalf("Replay IngestTick failed: %v", err)
		}
	}

	// Create out-of-order permutation for Live with max jitter <= 80ms (< 100ms watermark)
	scrambled := make([]feed.Tick, numTicks)
	copy(scrambled, ticks)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numTicks-2; i += 2 {
		if rng.Float64() < 0.6 {
			// Swap adjacent ticks (within 40ms, well within 100ms watermark buffer)
			scrambled[i], scrambled[i+1] = scrambled[i+1], scrambled[i]
		}
	}

	for _, tick := range scrambled {
		if err := projLive.IngestTick(tick); err != nil {
			t.Fatalf("Live IngestTick failed: %v", err)
		}
	}

	// Flush both to baseTS + 4s
	endTS := baseTS + 4_000_000_000
	if err := projReplay.Flush(endTS); err != nil {
		t.Fatalf("Replay Flush failed: %v", err)
	}
	if err := projLive.Flush(endTS); err != nil {
		t.Fatalf("Live Flush failed: %v", err)
	}

	// Compare committed frames in SHM
	barsLive := prodLive.ReadHistoryBars(0, 0, 4)
	barsReplay := prodReplay.ReadHistoryBars(0, 0, 4)

	if len(barsLive) != len(barsReplay) {
		t.Fatalf("Frame count mismatch: live=%d, replay=%d", len(barsLive), len(barsReplay))
	}
	if len(barsLive) == 0 {
		t.Fatalf("Expected committed bars > 0, got 0")
	}

	for bIdx := 0; bIdx < len(barsLive); bIdx++ {
		bLive := barsLive[bIdx]
		bReplay := barsReplay[bIdx]
		if bLive.AnchorNS != bReplay.AnchorNS {
			t.Fatalf("Bar %d anchor mismatch: live=%d, replay=%d", bIdx, bLive.AnchorNS, bReplay.AnchorNS)
		}
		for fIdx := 0; fIdx < len(bLive.Features); fIdx++ {
			vLive := bLive.Features[fIdx]
			vReplay := bReplay.Features[fIdx]
			if math.IsNaN(vLive) && math.IsNaN(vReplay) {
				continue
			}
			if math.Float64bits(vLive) != math.Float64bits(vReplay) {
				t.Fatalf("Bar %d feature %d mismatch: live=%v, replay=%v", bIdx, fIdx, vLive, vReplay)
			}
		}
	}
}

// TestLiveReorderZeroAllocPushPop confirms PushItem and PopItem do not allocate on pre-warmed heap.
func TestLiveReorderZeroAllocPushPop(t *testing.T) {
	heap := tickMinHeap{
		items: make([]liveTickItem, 0, 1024),
	}

	sampleTick := feed.Tick{
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		SIPTimestampNS: 100_000_000,
		BidPx:          150.0,
		AskPx:          150.05,
	}

	// Warm up
	for i := int64(0); i < 100; i++ {
		heap.PushItem(liveTickItem{tick: sampleTick, seq: uint64(i)})
	}
	for heap.Len() > 0 {
		_ = heap.PopItem()
	}

	allocs := testing.AllocsPerRun(1000, func() {
		heap.PushItem(liveTickItem{tick: sampleTick, seq: 1})
		_ = heap.PopItem()
	})

	if allocs > 0 {
		t.Fatalf("expected 0 allocations in PushItem/PopItem, got %f", allocs)
	}
}

// TestLiveVirtualStreamIdleDrain verifies that VirtualStreamNow advances 1:1 with wall-clock
// and drains the watermark buffer during quiet / halted periods.
func TestLiveVirtualStreamIdleDrain(t *testing.T) {
	shmName := fmt.Sprintf("test_vstream_idle_%d", os.Getpid())
	phases := []shm.PhaseConfig{
		{ID: 0, Name: "phase_0ms", OffsetMS: 0, Symbols: []string{"AAPL"}},
	}
	feats := make([]string, 44)
	for i := range feats {
		feats[i] = fmt.Sprintf("f%d", i)
	}

	cfg := shm.Config{
		Name:            shmName,
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
	proj.SetWatermarkBuffer(100 * time.Millisecond) // 100ms

	t0 := int64(1_000_000_000)
	proj.SetStartAnchor(t0)

	// Ingest tick at t0 + 50ms
	_ = proj.IngestTick(feed.Tick{
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		SIPTimestampNS: t0 + 50_000_000,
		BidPx:          100.0,
		AskPx:          100.05,
	})

	// Heap must hold the tick because maxSIP - watermark = 50ms - 100ms < 0
	if proj.liveHeap.Len() != 1 {
		t.Fatalf("expected 1 item in heap before drain, got %d", proj.liveHeap.Len())
	}

	// Advance virtual time by simulating wall time +150ms
	wallAdvance := 150 * time.Millisecond
	vNow := proj.VirtualStreamNow(proj.maxSIPWallTime.Add(wallAdvance))
	expectedVNow := proj.maxSIPTimestampNS + wallAdvance.Nanoseconds()
	if vNow != expectedVNow {
		t.Fatalf("expected VirtualStreamNow=%d, got %d", expectedVNow, vNow)
	}

	// Flush at target = vNow - watermarkBuffer (effective target = t0 + 50ms + 150ms - 100ms = t0 + 100ms)
	flushTarget := vNow - proj.watermarkBufferNS
	if err := proj.Flush(flushTarget); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Heap must now be drained completely
	if proj.liveHeap.Len() != 0 {
		t.Fatalf("expected 0 items in heap after idle virtual stream flush, got %d", proj.liveHeap.Len())
	}
}

// TestLiveLateTailTickRouting verifies that ticks arriving after their whole-second bar has closed
// are routed to IngestPreWindowCCM* and do NOT corrupt current bar accumulators.
func TestLiveLateTailTickRouting(t *testing.T) {
	shmName := fmt.Sprintf("test_late_tail_%d", os.Getpid())
	phases := []shm.PhaseConfig{
		{ID: 0, Name: "phase_0ms", OffsetMS: 0, Symbols: []string{"AAPL"}},
	}
	feats := make([]string, 44)
	for i := range feats {
		feats[i] = fmt.Sprintf("f%d", i)
	}

	cfg := shm.Config{
		Name:            shmName,
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
	proj.SetWatermarkBuffer(0) // drain immediately for direct inspection

	t0 := int64(1_000_000_000)
	proj.SetStartAnchor(t0)

	// Tick 1 at 1.1s (closes [0, 1.0s) and establishes [1.0s, 2.0s))
	_ = proj.IngestTick(feed.Tick{
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		SIPTimestampNS: 1_100_000_000,
		BidPx:          100.0,
		AskPx:          100.05,
		BidSz:          10,
		AskSz:          10,
	})

	// Advance past 2.0s boundary by tick at 2.1s (closes [1.0s, 2.0s) bar)
	_ = proj.IngestTick(feed.Tick{
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		SIPTimestampNS: 2_100_000_000,
		BidPx:          100.10,
		AskPx:          100.15,
		BidSz:          20,
		AskSz:          20,
	})

	hist := proj.uniqueHistories[0]
	// Current bar is [2.0s, 3.0s). Next second boundary is 3.0s.
	// closed bar boundary is symNextSecondNS - 1s = 2.0s.
	if proj.LateDroppedTicks() != 0 {
		t.Fatalf("expected 0 late dropped ticks initially, got %d", proj.LateDroppedTicks())
	}

	// Ingest a late trade with ts = 1.95s (< 2.0s closed boundary)
	lateTrade := feed.Tick{
		Symbol:         "AAPL",
		Type:           feed.TickTrade,
		SIPTimestampNS: 1_950_000_000,
		Price:          100.12,
		Size:           500.0,
	}
	_ = proj.IngestTick(lateTrade)

	if proj.LateDroppedTicks() != 1 {
		t.Fatalf("expected LateDroppedTicks=1, got %d", proj.LateDroppedTicks())
	}

	// Current bar [2.0s, 3.0s) trade volume must be 0, not corrupted by 500.0 from late trade
	if hist.ccmBuyVol != 0.0 || hist.ccmSellVol != 0.0 {
		t.Fatalf("expected current bar buy/sell vol to be 0, got buy=%f sell=%f", hist.ccmBuyVol, hist.ccmSellVol)
	}

	// Prevailing state (last trade price) in hist must be updated by IngestPreWindowCCMTrade
	if hist.ccmLastTradePx != 100.12 {
		t.Fatalf("expected ccmLastTradePx=100.12, got %f", hist.ccmLastTradePx)
	}
}
