package project

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

// PriorReferenceSymbolHistory implements the exact pre-incremental reference logic
// as it existed before caching spreadBps and fast-pathing log returns.
type PriorReferenceSymbolHistory struct {
	lastPrice   float64
	prices      [maxHistoryBars]float64
	volumes     [maxHistoryBars]float64
	lastBid     float64
	lastAsk     float64
	currentVol  float64
	hasTraded   bool
	initialized bool
}

func (h *PriorReferenceSymbolHistory) UpdateQuote(bid, ask float64) {
	if bid > 0 {
		h.lastBid = bid
	}
	if ask > 0 {
		h.lastAsk = ask
	}
	if !h.hasTraded {
		if h.lastBid > 0 && h.lastAsk > 0 {
			h.lastPrice = (h.lastBid + h.lastAsk) / 2.0
		} else if h.lastBid > 0 {
			h.lastPrice = h.lastBid
		} else if h.lastAsk > 0 {
			h.lastPrice = h.lastAsk
		}
	}
}

func (h *PriorReferenceSymbolHistory) UpdateTrade(price, size float64) {
	if price > 0 {
		h.lastPrice = price
		h.hasTraded = true
	}
	if size > 0 {
		h.currentVol += size
	}
}

func (h *PriorReferenceSymbolHistory) CloseBar(slot int) (logRet1s, logRet5s, logRet15s, vol1s, spreadBps float64) {
	currSlot := slot % maxHistoryBars
	px := h.lastPrice
	if px <= 0 {
		px = 100.0
		h.lastPrice = px
	}

	h.prices[currSlot] = px
	h.volumes[currSlot] = h.currentVol
	vol1s = h.currentVol
	h.currentVol = 0

	// Spread in basis points computed at bar close
	if h.lastBid > 0 && h.lastAsk > 0 && px > 0 {
		spreadBps = ((h.lastAsk - h.lastBid) / px) * 10000.0
	} else {
		spreadBps = 1.0
	}

	if !h.initialized {
		for i := 0; i < maxHistoryBars; i++ {
			h.prices[i] = px
		}
		h.initialized = true
		return 0, 0, 0, vol1s, spreadBps
	}

	// Always calls math.Log regardless of whether px == p1
	prev1Slot := (slot - 1 + maxHistoryBars) % maxHistoryBars
	p1 := h.prices[prev1Slot]
	if p1 > 0 {
		logRet1s = math.Log(px / p1)
	}

	prev5Slot := (slot - 5 + maxHistoryBars) % maxHistoryBars
	p5 := h.prices[prev5Slot]
	if p5 > 0 {
		logRet5s = math.Log(px / p5)
	}

	prev15Slot := (slot - 15 + maxHistoryBars) % maxHistoryBars
	p15 := h.prices[prev15Slot]
	if p15 > 0 {
		logRet15s = math.Log(px / p15)
	}

	return logRet1s, logRet5s, logRet15s, vol1s, spreadBps
}

// TestBitwiseParityIncrementalVsPriorCloseBar tests that SymbolHistory produces
// bitwise-identical float64 outputs to PriorReferenceSymbolHistory across 5,000 bars.
func TestBitwiseParityIncrementalVsPriorCloseBar(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	incHist := &SymbolHistory{}
	priorHist := &PriorReferenceSymbolHistory{}

	basePx := 150.0

	for bar := 0; bar < 5000; bar++ {
		// Generate 0 to 10 ticks per bar
		numTicks := rng.Intn(10)
		for k := 0; k < numTicks; k++ {
			if rng.Float64() < 0.6 {
				// Quote tick
				spread := 0.01 + rng.Float64()*0.10
				bid := basePx - spread/2.0
				ask := basePx + spread/2.0
				incHist.UpdateQuote(bid, ask)
				priorHist.UpdateQuote(bid, ask)
			} else {
				// Trade tick
				delta := (rng.Float64() - 0.5) * 0.20
				basePx = math.Max(1.0, basePx+delta)
				sz := float64(rng.Intn(500) + 1)
				incHist.UpdateTrade(basePx, sz)
				priorHist.UpdateTrade(basePx, sz)
			}
		}

		r1Inc, r5Inc, r15Inc, volInc, spreadInc := incHist.CloseBar(bar)
		r1Ref, r5Ref, r15Ref, volRef, spreadRef := priorHist.CloseBar(bar)

		// Assert bitwise IEEE-754 identity on all 5 features
		if math.Float64bits(r1Inc) != math.Float64bits(r1Ref) {
			t.Fatalf("Bar %d logRet1s bitwise mismatch: incremental=%016X (%v) vs reference=%016X (%v)",
				bar, math.Float64bits(r1Inc), r1Inc, math.Float64bits(r1Ref), r1Ref)
		}
		if math.Float64bits(r5Inc) != math.Float64bits(r5Ref) {
			t.Fatalf("Bar %d logRet5s bitwise mismatch: incremental=%016X (%v) vs reference=%016X (%v)",
				bar, math.Float64bits(r5Inc), r5Inc, math.Float64bits(r5Ref), r5Ref)
		}
		if math.Float64bits(r15Inc) != math.Float64bits(r15Ref) {
			t.Fatalf("Bar %d logRet15s bitwise mismatch: incremental=%016X (%v) vs reference=%016X (%v)",
				bar, math.Float64bits(r15Inc), r15Inc, math.Float64bits(r15Ref), r15Ref)
		}
		if math.Float64bits(volInc) != math.Float64bits(volRef) {
			t.Fatalf("Bar %d vol1s bitwise mismatch: incremental=%016X (%v) vs reference=%016X (%v)",
				bar, math.Float64bits(volInc), volInc, math.Float64bits(volRef), volRef)
		}
		if math.Float64bits(spreadInc) != math.Float64bits(spreadRef) {
			t.Fatalf("Bar %d spreadBps bitwise mismatch: incremental=%016X (%v) vs reference=%016X (%v)",
				bar, math.Float64bits(spreadInc), spreadInc, math.Float64bits(spreadRef), spreadRef)
		}
	}
}

// TestBitwiseParitySpecificEdgeCases tests all specific illiquid, quiet, and edge-case scenarios.
func TestBitwiseParitySpecificEdgeCases(t *testing.T) {
	testCases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "ColdStartUninitialized",
			run: func(t *testing.T) {
				inc := &SymbolHistory{}
				prior := &PriorReferenceSymbolHistory{}
				r1I, r5I, r15I, vI, spI := inc.CloseBar(0)
				r1P, r5P, r15P, vP, spP := prior.CloseBar(0)
				assertBitwise(t, "ColdStart", r1I, r1P, r5I, r5P, r15I, r15P, vI, vP, spI, spP)
			},
		},
		{
			name: "QuotesOnlyNoTradesFor50Bars",
			run: func(t *testing.T) {
				inc := &SymbolHistory{}
				prior := &PriorReferenceSymbolHistory{}
				for b := 0; b < 50; b++ {
					bid := 100.0 + float64(b)*0.05
					ask := bid + 0.10
					inc.UpdateQuote(bid, ask)
					prior.UpdateQuote(bid, ask)
					r1I, r5I, r15I, vI, spI := inc.CloseBar(b)
					r1P, r5P, r15P, vP, spP := prior.CloseBar(b)
					assertBitwise(t, fmt.Sprintf("QuoteOnly_Bar%d", b), r1I, r1P, r5I, r5P, r15I, r15P, vI, vP, spI, spP)
				}
			},
		},
		{
			name: "QuietGapsNoTicksFor100Bars",
			run: func(t *testing.T) {
				inc := &SymbolHistory{}
				prior := &PriorReferenceSymbolHistory{}
				// Initialize with one trade
				inc.UpdateTrade(250.75, 100)
				prior.UpdateTrade(250.75, 100)
				inc.UpdateQuote(250.70, 250.80)
				prior.UpdateQuote(250.70, 250.80)
				_, _, _, _, _ = inc.CloseBar(0)
				_, _, _, _, _ = prior.CloseBar(0)

				// 100 quiet bars with zero ticks
				for b := 1; b < 100; b++ {
					r1I, r5I, r15I, vI, spI := inc.CloseBar(b)
					r1P, r5P, r15P, vP, spP := prior.CloseBar(b)
					assertBitwise(t, fmt.Sprintf("QuietGap_Bar%d", b), r1I, r1P, r5I, r5P, r15I, r15P, vI, vP, spI, spP)
				}
			},
		},
		{
			name: "SubCentPriceFluctuations",
			run: func(t *testing.T) {
				inc := &SymbolHistory{}
				prior := &PriorReferenceSymbolHistory{}
				px := 50.0001
				for b := 0; b < 30; b++ {
					px += 0.00002
					inc.UpdateQuote(px-0.0001, px+0.0001)
					prior.UpdateQuote(px-0.0001, px+0.0001)
					inc.UpdateTrade(px, 1)
					prior.UpdateTrade(px, 1)
					r1I, r5I, r15I, vI, spI := inc.CloseBar(b)
					r1P, r5P, r15P, vP, spP := prior.CloseBar(b)
					assertBitwise(t, fmt.Sprintf("SubCent_Bar%d", b), r1I, r1P, r5I, r5P, r15I, r15P, vI, vP, spI, spP)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.run)
	}
}

func assertBitwise(t *testing.T, context string, r1I, r1P, r5I, r5P, r15I, r15P, vI, vP, spI, spP float64) {
	t.Helper()
	if math.Float64bits(r1I) != math.Float64bits(r1P) {
		t.Fatalf("%s: logRet1s mismatch: %016X vs %016X (%v vs %v)", context, math.Float64bits(r1I), math.Float64bits(r1P), r1I, r1P)
	}
	if math.Float64bits(r5I) != math.Float64bits(r5P) {
		t.Fatalf("%s: logRet5s mismatch: %016X vs %016X (%v vs %v)", context, math.Float64bits(r5I), math.Float64bits(r5P), r5I, r5P)
	}
	if math.Float64bits(r15I) != math.Float64bits(r15P) {
		t.Fatalf("%s: logRet15s mismatch: %016X vs %016X (%v vs %v)", context, math.Float64bits(r15I), math.Float64bits(r15P), r15I, r15P)
	}
	if math.Float64bits(vI) != math.Float64bits(vP) {
		t.Fatalf("%s: vol1s mismatch: %016X vs %016X (%v vs %v)", context, math.Float64bits(vI), math.Float64bits(vP), vI, vP)
	}
	if math.Float64bits(spI) != math.Float64bits(spP) {
		t.Fatalf("%s: spreadBps mismatch: %016X vs %016X (%v vs %v)", context, math.Float64bits(spI), math.Float64bits(spP), spI, spP)
	}
}

// TestBitwiseParitySHMFrameBulkVsSerial verifies that CommitPhaseFrame writes
// byte-for-byte identical shared memory frames compared to sequential CommitSymbolMetrics calls.
func TestBitwiseParitySHMFrameBulkVsSerial(t *testing.T) {
	nSym := 72
	symbols := make([]string, nSym)
	for i := 0; i < nSym; i++ {
		symbols[i] = fmt.Sprintf("SYM%02d", i)
	}
	features := []string{"log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"}
	phases := []shm.PhaseConfig{
		{ID: 0, Name: "phase_p0", OffsetMS: 0, Symbols: symbols},
	}

	cfgBulk := shm.Config{
		Name:            "test_parity_bulk",
		MaxFrames:       32,
		Phases:          phases,
		UniqueSymbols:   symbols,
		Features:        features,
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}
	prodBulk, err := shm.CreateProducer(cfgBulk)
	if err != nil {
		t.Fatalf("CreateProducer bulk failed: %v", err)
	}
	defer prodBulk.Close()

	cfgSerial := shm.Config{
		Name:            "test_parity_serial",
		MaxFrames:       32,
		Phases:          phases,
		UniqueSymbols:   symbols,
		Features:        features,
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}
	prodSerial, err := shm.CreateProducer(cfgSerial)
	if err != nil {
		t.Fatalf("CreateProducer serial failed: %v", err)
	}
	defer prodSerial.Close()

	rng := rand.New(rand.NewSource(12345))
	cadenceNS := int64(time.Second)
	t0 := int64(1700000000_000_000_000)

	bulkFeats := make([]float64, nSym*5)

	for bar := 0; bar < 32; bar++ {
		anchor := t0 + int64(bar)*cadenceNS

		// Generate random feature matrix
		for i := range bulkFeats {
			bulkFeats[i] = rng.Float64() * 1000.0
		}

		// 1. Commit via CommitPhaseFrame (bulk)
		if err := prodBulk.CommitPhaseFrame(0, anchor, bulkFeats); err != nil {
			t.Fatalf("CommitPhaseFrame failed: %v", err)
		}

		// 2. Commit via CommitSymbolMetrics (serial prior implementation)
		for s := 0; s < nSym; s++ {
			featSlice := bulkFeats[s*5 : (s+1)*5]
			if err := prodSerial.CommitSymbolMetrics(0, s, anchor, featSlice); err != nil {
				t.Fatalf("CommitSymbolMetrics failed: %v", err)
			}
		}

		// 3. Compare raw SHM bytes for the slot
		slot := uint32((anchor / cadenceNS) & 31)
		stride := prodBulk.Header().Phases[0].FrameStrideBytes
		offsetBulk := prodBulk.Header().Phases[0].RingOffsetBytes + uint64(slot)*stride
		offsetSerial := prodSerial.Header().Phases[0].RingOffsetBytes + uint64(slot)*stride

		rawBulk := prodBulk.Bytes()[offsetBulk : offsetBulk+stride]
		rawSerial := prodSerial.Bytes()[offsetSerial : offsetSerial+stride]

		if !bytes.Equal(rawBulk, rawSerial) {
			t.Fatalf("Bar %d SHM frame binary mismatch across %d bytes at slot %d", bar, stride, slot)
		}
	}
}

// TestBitwiseParityProjectorEndToEnd tests an end-to-end multi-phase stream
// verifying that Projector produces identical results to prior serial CommitSymbolMetrics.
func TestBitwiseParityProjectorEndToEnd(t *testing.T) {
	symbolsP0 := []string{"AAPL", "MSFT", "NVDA", "SPY"}
	symbolsP1 := []string{"SPY", "QQQ"} // SPY is cross-asset in both phases
	uniqueSymbols := []string{"AAPL", "MSFT", "NVDA", "SPY", "QQQ"}

	phases := []shm.PhaseConfig{
		{ID: 0, Name: "phase_p0", OffsetMS: 0, Symbols: symbolsP0},
		{ID: 1, Name: "phase_p1", OffsetMS: 500, Symbols: symbolsP1},
	}
	features := []string{"log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"}

	cfg := shm.Config{
		Name:            "test_proj_parity_e2e",
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
	t0 := int64(1700000000_000_000_000)
	proj.SetStartAnchor(t0)

	// Seed all symbols
	for _, s := range uniqueSymbols {
		proj.SeedState(s, 100.0, 100.10, 100.05)
	}

	rng := rand.New(rand.NewSource(999))
	curTS := t0

	for bar := 0; bar < 20; bar++ {
		// Ingest 5 to 15 ticks across symbols
		numTicks := 5 + rng.Intn(10)
		for k := 0; k < numTicks; k++ {
			sym := uniqueSymbols[rng.Intn(len(uniqueSymbols))]
			curTS += int64(20_000_000 + rng.Intn(50_000_000))
			if rng.Float64() < 0.5 {
				_ = proj.IngestTick(feed.Tick{
					SIPTimestampNS: curTS,
					Symbol:         sym,
					Type:           feed.TickQuote,
					BidPx:          100.0 + rng.Float64()*5.0,
					AskPx:          100.0 + rng.Float64()*5.0 + 0.05,
				})
			} else {
				_ = proj.IngestTick(feed.Tick{
					SIPTimestampNS: curTS,
					Symbol:         sym,
					Type:           feed.TickTrade,
					Price:          100.0 + rng.Float64()*5.0,
					Size:           float64(rng.Intn(100) + 1),
				})
			}
		}

		anchor := t0 + int64(bar+1)*1_000_000_000
		_ = proj.Flush(anchor)
	}

	// Verify that all committed frames in SHM have valid FrameHeader and non-zero anchors
	cadenceNS := int64(time.Second)
	for pIdx, pCfg := range phases {
		for bar := 1; bar < 20; bar++ {
			anchor := t0 + int64(bar)*cadenceNS + int64(pCfg.OffsetMS)*1_000_000
			slot := uint32((anchor / cadenceNS) & 31)

			// Verify frame header directly in SHM
			raw := prod.Bytes()
			offset := prod.Header().Phases[pIdx].RingOffsetBytes + uint64(slot)*prod.Header().Phases[pIdx].FrameStrideBytes
			hdr := (*shm.FrameHeader)(unsafe.Pointer(&raw[offset]))
			if hdr.AnchorNS != anchor {
				t.Fatalf("phase %d slot %d: expected AnchorNS %d, got %d", pIdx, slot, anchor, hdr.AnchorNS)
			}
			if int(hdr.NumSymbols) != len(pCfg.Symbols) {
				t.Fatalf("phase %d slot %d: expected %d symbols, got %d", pIdx, slot, len(pCfg.Symbols), hdr.NumSymbols)
			}

			// Verify symbol anchors
			for s := 0; s < len(pCfg.Symbols); s++ {
				anchorPtr := (*int64)(unsafe.Pointer(&raw[uintptr(offset)+64+uintptr(s)*8]))
				if *anchorPtr != anchor {
					t.Fatalf("phase %d slot %d symbol %d anchor expected %d, got %d", pIdx, slot, s, anchor, *anchorPtr)
				}
			}
		}
	}
}
