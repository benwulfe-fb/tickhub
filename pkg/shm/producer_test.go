package shm

import (
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestProducerLifecycleAndCommit(t *testing.T) {
	cfg := Config{
		Name:      "test_unit",
		MaxFrames: 16,
		Phases: []PhaseConfig{
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
				Symbols:  []string{"SPY", "MSFT"},
			},
		},
		UniqueSymbols:   []string{"SPY", "AAPL", "MSFT"},
		Features:        []string{"feat_a", "feat_b"},
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}

	prod, err := CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	prod.SetStatus(StatusRunning)
	if prod.header.Status != StatusRunning {
		t.Fatalf("expected StatusRunning, got %d", prod.header.Status)
	}

	// 1. Test Snapshot SeqLock Write
	snapIn := &SymbolSnapshot{
		SIPTimestampNS: 1700000000000000000,
		BidPx:          450.50,
		AskPx:          450.55,
		BidSz:          500,
		AskSz:          300,
		Midprice:       450.525,
		Spread:         0.05,
	}
	prod.WriteSnapshot(0, snapIn) // 0 = SPY

	spySnap := prod.snapshots[0]
	if spySnap.SeqLockSeq%2 != 0 {
		t.Fatalf("expected even SeqLock sequence, got %d", spySnap.SeqLockSeq)
	}
	if spySnap.BidPx != 450.50 || spySnap.AskPx != 450.55 {
		t.Fatalf("snapshot mismatch: bid=%f ask=%f", spySnap.BidPx, spySnap.AskPx)
	}

	// 2. Test Phase 0 Metrics Commit (SPY at 10:00:00.000)
	anchor0 := int64(1700000000000000000)
	feats0 := []float64{100.0, 200.0}
	if err := prod.CommitSymbolMetrics(0, 0, anchor0, feats0); err != nil {
		t.Fatalf("CommitSymbolMetrics phase 0 failed: %v", err)
	}

	// 3. Test Phase 1 Metrics Commit (SPY at 10:00:00.500)
	anchor1 := int64(1700000000500000000)
	feats1 := []float64{999.0, 888.0}
	if err := prod.CommitSymbolMetrics(1, 0, anchor1, feats1); err != nil {
		t.Fatalf("CommitSymbolMetrics phase 1 failed: %v", err)
	}

	// 4. Verify Phase 0 slot memory
	slot0 := (anchor0 / int64(time.Second)) & 15
	frame0Off := prod.phaseOffsets[0] + uintptr(slot0)*prod.phaseStrides[0]
	raw := prod.segment.Bytes()

	anchorPtr0 := (*int64)(unsafe.Pointer(&raw[frame0Off+64])) // symbol 0 anchor
	if got := atomic.LoadInt64(anchorPtr0); got != anchor0 {
		t.Fatalf("phase 0 anchor mismatch: expected %d, got %d", anchor0, got)
	}

	featOff0 := frame0Off + 64 + 2*8 // 2 symbols * 8B
	f0 := *(*float64)(unsafe.Pointer(&raw[featOff0]))
	f1 := *(*float64)(unsafe.Pointer(&raw[featOff0+8]))
	if f0 != 100.0 || f1 != 200.0 {
		t.Fatalf("phase 0 features mismatch: got %f, %f", f0, f1)
	}

	// 5. Verify Phase 1 slot memory
	slot1 := (anchor1 / int64(time.Second)) & 15
	frame1Off := prod.phaseOffsets[1] + uintptr(slot1)*prod.phaseStrides[1]
	anchorPtr1 := (*int64)(unsafe.Pointer(&raw[frame1Off+64]))
	if got := atomic.LoadInt64(anchorPtr1); got != anchor1 {
		t.Fatalf("phase 1 anchor mismatch: expected %d, got %d", anchor1, got)
	}

	featOff1 := frame1Off + 64 + 2*8
	f1_0 := *(*float64)(unsafe.Pointer(&raw[featOff1]))
	f1_1 := *(*float64)(unsafe.Pointer(&raw[featOff1+8]))
	if f1_0 != 999.0 || f1_1 != 888.0 {
		t.Fatalf("phase 1 features mismatch: got %f, %f", f1_0, f1_1)
	}

	// 6. Telemetry update
	prod.PublishTelemetry(42*1000*1000, 50*1000*1000, 3, 1000)
	if prod.header.AnchorPublishLatencyNS != 42*1000*1000 {
		t.Fatalf("telemetry latency mismatch")
	}
	if prod.header.DroppedTickCount != 3 {
		t.Fatalf("telemetry dropped ticks mismatch")
	}
}

func TestReplayFlowControlBackpressure(t *testing.T) {
	cfg := Config{
		Name:      "test_replay_backpressure",
		MaxFrames: 8,
		Mode:      ModeHistoricalReplay,
		Phases: []PhaseConfig{
			{
				ID:       0,
				Name:     "phase_0ms",
				OffsetMS: 0,
				Symbols:  []string{"SPY"},
			},
		},
		UniqueSymbols:   []string{"SPY"},
		Features:        []string{"feat_a"},
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}

	prod, err := CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	startAnchor := int64(1700000000_000_000_000)

	// Fill buffer up to maxFrames - 2 (which is 6 frames: f=0..5)
	for f := 0; f < 6; f++ {
		anchor := startAnchor + int64(f)*1_000_000_000
		if err := prod.CommitSymbolMetrics(0, 0, anchor, []float64{float64(f)}); err != nil {
			t.Fatalf("frame %d commit failed: %v", f, err)
		}
		prod.CommitFrameFinalize(anchor)
	}

	// Now buffer is full (anchor 6 will pause).
	// In background, advance consumer LastReadAnchorNS after 50ms.
	go func() {
		time.Sleep(50 * time.Millisecond)
		// Advance consumer to frame 3
		atomic.StoreInt64(&prod.header.LastReadAnchorNS, startAnchor+3*1_000_000_000)
	}()

	anchor6 := startAnchor + 6*1_000_000_000
	t0 := time.Now()
	if err := prod.CommitSymbolMetrics(0, 0, anchor6, []float64{6.0}); err != nil {
		t.Fatalf("frame 6 commit should succeed after consumer advance, got: %v", err)
	}
	elapsed := time.Since(t0)

	if elapsed < 30*time.Millisecond {
		t.Fatalf("expected producer to wait for consumer advance, but elapsed was only %v", elapsed)
	}

	if atomic.LoadUint64(&prod.header.DroppedTickCount) != 0 {
		t.Fatalf("expected 0 dropped/overrun frames, got %d", prod.header.DroppedTickCount)
	}
}

