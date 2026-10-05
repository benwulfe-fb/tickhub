package playback

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/benwulfe-fb/tickhub/pkg/recorder"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
	"github.com/parquet-go/parquet-go"
)

func TestPlaybackE2E(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tickhub_pb_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	pqPath := filepath.Join(tmpDir, "sample_1hz.parquet")
	f, err := os.OpenFile(pqPath, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	w := parquet.NewGenericWriter[recorder.MetricRow](f, parquet.Compression(&parquet.Snappy))
	rawYAML := `shm:
  name: test_pb_shm
  max_frames: 16
  cadence_interval: 1000000000
unique_symbols:
  - SPY
  - QQQ
features:
  - ret1
  - ret5
  - ret15
  - vol1
  - spread_bps
phases:
  - id: 0
    name: core
    offset_ms: 0
    symbols:
      - SPY
      - QQQ
`
	w.SetKeyValueMetadata("tickhub_config_yaml", rawYAML)

	anchor1 := int64(1700000010000000000)
	anchor2 := int64(1700000011000000000)

	rows := []recorder.MetricRow{
		{
			TimestampNS: anchor1,
			RecvTSNS:    anchor1 + 1000,
			SeqLockSeq:  2,
			Symbol:      "SPY",
			PhaseID:     0,
			PhaseSlot:   0,
			BidPX:       500.10,
			AskPX:       500.20,
			BidSZ:       50,
			AskSZ:       60,
			MidPX:       500.15,
			SpreadPX:    0.10,
			SpreadBPS:   2.0,
			LastTradePX: 500.12,
			LastTradeSZ: 10,
			Features:    []float64{0.0001, 0.0002, 0.0003, 10.0, 2.0},
		},
		{
			TimestampNS: anchor1,
			RecvTSNS:    anchor1 + 2000,
			SeqLockSeq:  4,
			Symbol:      "QQQ",
			PhaseID:     0,
			PhaseSlot:   0,
			BidPX:       400.10,
			AskPX:       400.30,
			BidSZ:       30,
			AskSZ:       40,
			MidPX:       400.20,
			SpreadPX:    0.20,
			SpreadBPS:   5.0,
			LastTradePX: 400.15,
			LastTradeSZ: 5,
			Features:    []float64{0.0005, 0.0010, 0.0015, 5.0, 5.0},
		},
		{
			TimestampNS: anchor2,
			RecvTSNS:    anchor2 + 1000,
			SeqLockSeq:  6,
			Symbol:      "SPY",
			PhaseID:     0,
			PhaseSlot:   1,
			BidPX:       500.25,
			AskPX:       500.35,
			BidSZ:       70,
			AskSZ:       80,
			MidPX:       500.30,
			SpreadPX:    0.10,
			SpreadBPS:   2.0,
			LastTradePX: 500.32,
			LastTradeSZ: 20,
			Features:    []float64{0.0002, 0.0004, 0.0006, 20.0, 2.0},
		},
	}

	if _, err := w.Write(rows); err != nil {
		t.Fatalf("Write rows failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}
	_ = f.Close()

	// Launch playback engine into test SHM segment
	shmName := "test_playback_shm"
	pbCfg := Config{
		SHMName:            shmName,
		SourcePath:         pqPath,
		Speed:              0, // unthrottled
		Loop:               false,
		UnlinkOnExit:       false,
		FlowControlTimeout: 5 * time.Second,
	}

	eng, err := NewEngine(pbCfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := eng.Run(ctx); err != nil {
		t.Fatalf("Engine Run failed: %v", err)
	}

	stats := eng.Stats()
	if stats.AnchorsReplayed != 2 {
		t.Fatalf("Expected 2 anchors replayed, got %d", stats.AnchorsReplayed)
	}
	if stats.TotalRows != 3 {
		t.Fatalf("Expected 3 total rows replayed, got %d", stats.TotalRows)
	}

	// Verify SHM segment state by attaching as a consumer
	seg, err := shm.AttachSegment(shmName, true)
	if err != nil {
		t.Fatalf("Attach to playback SHM failed: %v", err)
	}
	defer seg.Close(true)

	hdr := seg.Header()
	if atomic.LoadInt64(&hdr.LastWrittenAnchorNS) != anchor2 {
		t.Fatalf("Expected last anchor %d, got %d", anchor2, atomic.LoadInt64(&hdr.LastWrittenAnchorNS))
	}

	raw := seg.Bytes()
	snapSPY := (*shm.SymbolSnapshot)(unsafe.Pointer(&raw[shm.SnapshotOffset]))
	if snapSPY.BidPx != 500.25 || snapSPY.AskPx != 500.35 {
		t.Fatalf("Expected SPY bid/ask 500.25/500.35, got %.2f/%.2f", snapSPY.BidPx, snapSPY.AskPx)
	}
	if snapSPY.LastTradePx != 500.32 {
		t.Fatalf("Expected SPY last trade 500.32, got %.2f", snapSPY.LastTradePx)
	}

	// Verify phase frame features in ring buffer for anchor2
	slot2 := uint32((anchor2 / int64(time.Second)) & int64(hdr.MaxFrames-1))
	pInfo := hdr.Phases[0]
	frameOffset2 := uintptr(pInfo.RingOffsetBytes) + uintptr(slot2)*uintptr(pInfo.FrameStrideBytes)
	nFeat := int(pInfo.NumFeatures)
	featOffsetSPY := frameOffset2 + 64 + uintptr(pInfo.NumSymbols)*8 + uintptr(0)*uintptr(nFeat)*8
	replayedFeatsSPY := unsafe.Slice((*float64)(unsafe.Pointer(&raw[featOffsetSPY])), nFeat)
	if replayedFeatsSPY[0] != 0.0002 || replayedFeatsSPY[3] != 20.0 {
		t.Fatalf("Expected replayed SPY features [0.0002 ... 20.0], got %v", replayedFeatsSPY)
	}
}

func TestFullRoundTripE2E(t *testing.T) {
	liveSHM := "test_rt_live"
	playbackSHM := "test_rt_playback"
	symbols := []string{"AAPL", "NVDA"}

	rawYAML := `shm:
  name: test_rt_live
  max_frames: 16
  cadence_interval: 1000000000
unique_symbols:
  - AAPL
  - NVDA
features:
  - ret1
  - ret5
  - ret15
  - vol1
  - spread_bps
phases:
  - id: 0
    name: core
    offset_ms: 0
    symbols:
      - AAPL
      - NVDA
`

	liveCfg := shm.Config{
		Name:            liveSHM,
		MaxFrames:       16,
		CadenceInterval: time.Second,
		UniqueSymbols:   symbols,
		Features:        []string{"ret1", "ret5", "ret15", "vol1", "spread_bps"},
		Phases: []shm.PhaseConfig{
			{
				ID:       0,
				Name:     "core",
				OffsetMS: 0,
				Symbols:  symbols,
			},
		},
		Permissions:  0660,
		UnlinkOnExit: true,
		Mode:         shm.ModeLiveStreaming,
		RawYAML:      []byte(rawYAML),
	}

	liveProd, err := shm.CreateProducer(liveCfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer liveProd.Close()

	// Setup recorder
	tmpDir, err := os.MkdirTemp("", "tickhub_roundtrip_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	pqPath := filepath.Join(tmpDir, "rt.parquet")
	rec, err := recorder.NewRecorder(recorder.Config{
		SHMName:       liveSHM,
		OutFile:       pqPath,
		FlushInterval: 100 * time.Millisecond,
		BatchSize:     10,
		PollInterval:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}

	// Produce Frame 1
	t1 := int64(1700000020000000000)
	liveProd.WriteSnapshot(0, &shm.SymbolSnapshot{
		SeqLockSeq:      2,
		SIPTimestampNS:  t1,
		RecvTimestampNS: t1 + 100,
		BidPx:           180.50,
		AskPx:           180.60,
		BidSz:           100,
		AskSz:           200,
		Midprice:        180.55,
		Spread:          0.10,
		LastTradePx:     180.52,
	})
	liveProd.WriteSnapshot(1, &shm.SymbolSnapshot{
		SeqLockSeq:      4,
		SIPTimestampNS:  t1,
		RecvTimestampNS: t1 + 200,
		BidPx:           120.10,
		AskPx:           120.30,
		BidSz:           300,
		AskSz:           150,
		Midprice:        120.20,
		Spread:          0.20,
		LastTradePx:     120.15,
	})
	feats1 := []float64{
		0.0010, 0.0020, 0.0030, 100.0, 5.5,
		0.0015, 0.0025, 0.0035, 200.0, 16.6,
	}
	if err := liveProd.CommitPhaseFrame(0, t1, feats1); err != nil {
		t.Fatalf("CommitPhaseFrame t1: %v", err)
	}
	liveProd.CommitFrameFinalize(t1)

	// Recorder poll
	if err := rec.PollOnce(); err != nil {
		t.Fatalf("PollOnce t1: %v", err)
	}

	// Produce Frame 2
	t2 := int64(1700000021000000000)
	liveProd.WriteSnapshot(0, &shm.SymbolSnapshot{
		SeqLockSeq:      6,
		SIPTimestampNS:  t2,
		RecvTimestampNS: t2 + 100,
		BidPx:           180.70,
		AskPx:           180.80,
		BidSz:           120,
		AskSz:           220,
		Midprice:        180.75,
		Spread:          0.10,
		LastTradePx:     180.72,
	})
	feats2 := []float64{
		0.0012, 0.0022, 0.0032, 110.0, 5.5,
		0.0018, 0.0028, 0.0038, 210.0, 16.6,
	}
	if err := liveProd.CommitPhaseFrame(0, t2, feats2); err != nil {
		t.Fatalf("CommitPhaseFrame t2: %v", err)
	}
	liveProd.CommitFrameFinalize(t2)

	if err := rec.PollOnce(); err != nil {
		t.Fatalf("PollOnce t2: %v", err)
	}

	if err := rec.Close(); err != nil {
		t.Fatalf("rec.Close failed: %v", err)
	}

	// Now playback into playbackSHM
	pbEng, err := NewEngine(Config{
		SHMName:            playbackSHM,
		SourcePath:         pqPath,
		Speed:              0,
		Loop:               false,
		UnlinkOnExit:       false,
		FlowControlTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pbEng.Run(ctx); err != nil {
		t.Fatalf("pbEng.Run failed: %v", err)
	}

	// Now attach to playbackSHM and verify both snapshots AND phase ring buffer features!
	pbSeg, err := shm.AttachSegment(playbackSHM, true)
	if err != nil {
		t.Fatalf("Attach to playback segment failed: %v", err)
	}
	defer pbSeg.Close(true)

	pbHdr := pbSeg.Header()
	if atomic.LoadInt64(&pbHdr.LastWrittenAnchorNS) != t2 {
		t.Fatalf("Expected replayed last anchor %d, got %d", t2, atomic.LoadInt64(&pbHdr.LastWrittenAnchorNS))
	}

	raw := pbSeg.Bytes()
	snapAAPL := (*shm.SymbolSnapshot)(unsafe.Pointer(&raw[shm.SnapshotOffset]))
	if snapAAPL.BidPx != 180.70 || snapAAPL.AskPx != 180.80 {
		t.Fatalf("Expected AAPL bid/ask 180.70/180.80, got %.2f/%.2f", snapAAPL.BidPx, snapAAPL.AskPx)
	}
	if snapAAPL.LastTradePx != 180.72 {
		t.Fatalf("Expected AAPL last trade 180.72, got %.2f", snapAAPL.LastTradePx)
	}

	// Verify ring buffer features for Frame 2
	slot2 := uint32((t2 / int64(time.Second)) & int64(pbHdr.MaxFrames-1))
	pInfo := pbHdr.Phases[0]
	frameOffset2 := uintptr(pInfo.RingOffsetBytes) + uintptr(slot2)*uintptr(pInfo.FrameStrideBytes)
	frameHdr2 := (*shm.FrameHeader)(unsafe.Pointer(&raw[frameOffset2]))
	if atomic.LoadInt64(&frameHdr2.AnchorNS) != t2 {
		t.Fatalf("Expected frame anchor %d, got %d", t2, atomic.LoadInt64(&frameHdr2.AnchorNS))
	}

	// AAPL features at slot2
	nFeat := int(pInfo.NumFeatures)
	featOffsetAAPL := frameOffset2 + 64 + uintptr(pInfo.NumSymbols)*8 + uintptr(0)*uintptr(nFeat)*8
	replayedFeatsAAPL := unsafe.Slice((*float64)(unsafe.Pointer(&raw[featOffsetAAPL])), nFeat)
	if replayedFeatsAAPL[0] != 0.0012 || replayedFeatsAAPL[3] != 110.0 {
		t.Fatalf("Replayed AAPL features mismatch: got %v, expected %v", replayedFeatsAAPL, feats2[:5])
	}

	// NVDA features at slot2
	featOffsetNVDA := frameOffset2 + 64 + uintptr(pInfo.NumSymbols)*8 + uintptr(1)*uintptr(nFeat)*8
	replayedFeatsNVDA := unsafe.Slice((*float64)(unsafe.Pointer(&raw[featOffsetNVDA])), nFeat)
	if replayedFeatsNVDA[0] != 0.0018 || replayedFeatsNVDA[3] != 210.0 {
		t.Fatalf("Replayed NVDA features mismatch: got %v, expected %v", replayedFeatsNVDA, feats2[5:])
	}
}
