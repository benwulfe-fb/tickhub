package recorder

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
	"github.com/parquet-go/parquet-go"
)

func TestRecorderE2E(t *testing.T) {
	shmName := "test_recorder_shm"
	symbols := []string{"AAPL", "NVDA"}

	shmCfg := shm.Config{
		Name:            shmName,
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
		RawYAML:      []byte("shm:\n  name: test_recorder_shm\n"),
	}

	prod, err := shm.CreateProducer(shmCfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	// Populate initial snapshot for AAPL and NVDA
	prod.WriteSnapshot(0, &shm.SymbolSnapshot{
		SeqLockSeq:      2,
		SIPTimestampNS:  1700000000000000000,
		RecvTimestampNS: 1700000000000100000,
		BidPx:           150.10,
		AskPx:           150.20,
		BidSz:           100,
		AskSz:           200,
		Midprice:        150.15,
		Spread:          0.10,
		LastTradePx:     150.18,
		LastTradeSz:     50,
	})

	prod.WriteSnapshot(1, &shm.SymbolSnapshot{
		SeqLockSeq:      4,
		SIPTimestampNS:  1700000000000000000,
		RecvTimestampNS: 1700000000000200000,
		BidPx:           450.50,
		AskPx:           450.70,
		BidSz:           300,
		AskSz:           100,
		Midprice:        450.60,
		Spread:          0.20,
		LastTradePx:     450.65,
		LastTradeSz:     25,
	})

	// Commit a phase frame at anchor
	anchorNS := int64(1700000001000000000)
	feats := []float64{
		0.0001, 0.0005, 0.0010, 50.0, 6.66, // AAPL
		0.0002, 0.0008, 0.0015, 25.0, 4.44, // NVDA
	}
	if err := prod.CommitPhaseFrame(0, anchorNS, feats); err != nil {
		t.Fatalf("CommitPhaseFrame failed: %v", err)
	}
	prod.CommitFrameFinalize(anchorNS)

	tmpDir, err := os.MkdirTemp("", "tickhub_rec_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	outFile := filepath.Join(tmpDir, "test_1hz.parquet")

	recCfg := Config{
		SHMName:       shmName,
		OutFile:       outFile,
		FlushInterval: 100 * time.Millisecond,
		BatchSize:     10,
		PollInterval:  10 * time.Millisecond,
	}

	rec, err := NewRecorder(recCfg)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}

	// Poll once to capture the committed frame
	if err := rec.PollOnce(); err != nil {
		t.Fatalf("PollOnce failed: %v", err)
	}

	// Force flush to file
	if err := rec.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file was written
	if _, err := os.Stat(outFile); err != nil {
		t.Fatalf("Parquet file %s was not written: %v", outFile, err)
	}

	// Read Parquet file back and assert fields
	f, err := os.Open(outFile)
	if err != nil {
		t.Fatalf("Open parquet failed: %v", err)
	}
	defer f.Close()

	reader := parquet.NewGenericReader[MetricRow](f)
	defer reader.Close()

	rows := make([]MetricRow, 10)
	n, err := reader.Read(rows)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("Read parquet rows failed: %v", err)
	}

	if n != 2 {
		t.Fatalf("Expected 2 rows recorded, got %d", n)
	}

	aaplRow := rows[0]
	if aaplRow.Symbol != "AAPL" {
		t.Fatalf("Expected symbol AAPL, got %s", aaplRow.Symbol)
	}
	if aaplRow.TimestampNS != anchorNS {
		t.Fatalf("Expected timestamp %d, got %d", anchorNS, aaplRow.TimestampNS)
	}
	if aaplRow.BidPX != 150.10 || aaplRow.AskPX != 150.20 {
		t.Fatalf("Unexpected AAPL bid/ask: %.2f / %.2f", aaplRow.BidPX, aaplRow.AskPX)
	}
	if len(aaplRow.Features) != 5 {
		t.Fatalf("Expected 5 features for AAPL, got %d", len(aaplRow.Features))
	}
	if aaplRow.Features[0] != 0.0001 || aaplRow.Features[3] != 50.0 {
		t.Fatalf("Unexpected AAPL features: %v", aaplRow.Features)
	}

	if aaplRow.Vol1S != 50.0 || aaplRow.LogRet1S != 0.0001 {
		t.Fatalf("Expected Vol1S 50.0 and LogRet1S 0.0001, got %.1f and %.4f", aaplRow.Vol1S, aaplRow.LogRet1S)
	}

	nvdaRow := rows[1]
	if nvdaRow.Symbol != "NVDA" {
		t.Fatalf("Expected symbol NVDA, got %s", nvdaRow.Symbol)
	}
	if nvdaRow.BidPX != 450.50 || nvdaRow.AskPX != 450.70 {
		t.Fatalf("Unexpected NVDA bid/ask: %.2f / %.2f", nvdaRow.BidPX, nvdaRow.AskPX)
	}
	if len(nvdaRow.Features) != 5 || nvdaRow.Features[3] != 25.0 {
		t.Fatalf("Unexpected NVDA features: %v", nvdaRow.Features)
	}
}

func TestRecorderRTHFiltering(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation America/New_York failed: %v", err)
	}

	shmName := "test_rth_recorder_shm"
	symbols := []string{"SPY"}

	shmCfg := shm.Config{
		Name:            shmName,
		MaxFrames:       16,
		CadenceInterval: time.Second,
		UniqueSymbols:   symbols,
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
	}

	prod, err := shm.CreateProducer(shmCfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	// Initial snapshot for SPY
	prod.WriteSnapshot(0, &shm.SymbolSnapshot{
		SeqLockSeq:      2,
		SIPTimestampNS:  1700000000000000000,
		RecvTimestampNS: 1700000000000100000,
		BidPx:           500.10,
		AskPx:           500.20,
		Midprice:        500.15,
	})

	tmpDir, err := os.MkdirTemp("", "tickhub_rth_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	outFile := filepath.Join(tmpDir, "rth_test.parquet")

	recCfg := Config{
		SHMName:       shmName,
		OutFile:       outFile,
		FlushInterval: 1 * time.Second,
		BatchSize:     100,
		PollInterval:  10 * time.Millisecond,
		RTHOnly:       true,
		Timezone:      "America/New_York",
		RTHStart:      "09:30:00",
		RTHEnd:        "16:00:00",
	}

	rec, err := NewRecorder(recCfg)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}
	defer rec.Close()

	// Test 1: Direct IsRTH checks across all boundaries
	// Saturday Oct 3, 2026 12:00:00 EDT
	satNS := time.Date(2026, 10, 3, 12, 0, 0, 0, loc).UnixNano()
	if rec.IsRTH(satNS) {
		t.Errorf("Expected Saturday %v to be outside RTH", satNS)
	}

	// Sunday Oct 4, 2026 12:00:00 EDT
	sunNS := time.Date(2026, 10, 4, 12, 0, 0, 0, loc).UnixNano()
	if rec.IsRTH(sunNS) {
		t.Errorf("Expected Sunday %v to be outside RTH", sunNS)
	}

	// Monday Oct 5, 2026 09:29:59 EDT (pre-market)
	preNS := time.Date(2026, 10, 5, 9, 29, 59, 0, loc).UnixNano()
	if rec.IsRTH(preNS) {
		t.Errorf("Expected pre-market %v to be outside RTH", preNS)
	}

	// Monday Oct 5, 2026 09:30:00 EDT (market open)
	openNS := time.Date(2026, 10, 5, 9, 30, 0, 0, loc).UnixNano()
	if !rec.IsRTH(openNS) {
		t.Errorf("Expected market open %v to be inside RTH", openNS)
	}

	// Monday Oct 5, 2026 12:00:00 EDT (midday)
	midNS := time.Date(2026, 10, 5, 12, 0, 0, 0, loc).UnixNano()
	if !rec.IsRTH(midNS) {
		t.Errorf("Expected midday %v to be inside RTH", midNS)
	}

	// Monday Oct 5, 2026 16:00:00 EDT (market close)
	closeNS := time.Date(2026, 10, 5, 16, 0, 0, 0, loc).UnixNano()
	if !rec.IsRTH(closeNS) {
		t.Errorf("Expected market close %v to be inside RTH", closeNS)
	}

	// Monday Oct 5, 2026 16:00:01 EDT (post-market)
	postNS := time.Date(2026, 10, 5, 16, 0, 1, 0, loc).UnixNano()
	if rec.IsRTH(postNS) {
		t.Errorf("Expected post-market %v to be outside RTH", postNS)
	}

	// Test 2: PollOnce behavior with anchor advances
	// 2a. Pre-market anchor should be skipped
	prod.CommitFrameFinalize(preNS)
	if err := rec.PollOnce(); err != nil {
		t.Fatalf("PollOnce(preNS) failed: %v", err)
	}
	if len(rec.buffer) != 0 {
		t.Fatalf("Expected 0 buffered rows for pre-market anchor, got %d", len(rec.buffer))
	}

	// 2b. RTH open anchor should be captured
	prod.CommitFrameFinalize(openNS)
	if err := rec.PollOnce(); err != nil {
		t.Fatalf("PollOnce(openNS) failed: %v", err)
	}
	if len(rec.buffer) != 1 {
		t.Fatalf("Expected 1 buffered row for RTH anchor, got %d", len(rec.buffer))
	}
	if rec.buffer[0].TimestampNS != openNS {
		t.Fatalf("Expected row timestamp %d, got %d", openNS, rec.buffer[0].TimestampNS)
	}

	// 2c. Post-market anchor should flush existing buffer and NOT buffer the post-market frame
	prod.CommitFrameFinalize(postNS)
	if err := rec.PollOnce(); err != nil {
		t.Fatalf("PollOnce(postNS) failed: %v", err)
	}
	if len(rec.buffer) != 0 {
		t.Fatalf("Expected buffer to be flushed on crossing close, got %d rows", len(rec.buffer))
	}

	// 2d. Weekend anchor should be skipped
	prod.CommitFrameFinalize(satNS)
	if err := rec.PollOnce(); err != nil {
		t.Fatalf("PollOnce(satNS) failed: %v", err)
	}
	if len(rec.buffer) != 0 {
		t.Fatalf("Expected 0 buffered rows for weekend anchor, got %d", len(rec.buffer))
	}

	// Close recorder to ensure all handles flushed
	if err := rec.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read Parquet file and verify ONLY the RTH anchor row exists
	f, err := os.Open(outFile)
	if err != nil {
		t.Fatalf("Open parquet failed: %v", err)
	}
	defer f.Close()

	reader := parquet.NewGenericReader[MetricRow](f)
	defer reader.Close()

	rows := make([]MetricRow, 10)
	n, err := reader.Read(rows)
	if err != nil && err.Error() != "EOF" {
		t.Fatalf("Read parquet failed: %v", err)
	}

	if n != 1 {
		t.Fatalf("Expected exactly 1 row in parquet (RTH only), got %d", n)
	}
	if rows[0].TimestampNS != openNS {
		t.Fatalf("Expected recorded row to be RTH anchor %d, got %d", openNS, rows[0].TimestampNS)
	}
	if rows[0].Symbol != "SPY" {
		t.Fatalf("Expected symbol SPY, got %s", rows[0].Symbol)
	}
}
