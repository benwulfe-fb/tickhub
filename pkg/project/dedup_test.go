package project

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

func TestAcceptQuoteDeduplication(t *testing.T) {
	h := &SymbolHistory{}
	h.InitCCM44()

	// 1. Initial invalid quotes (px <= 0 or NaN) are rejected without setting baseline
	if h.AcceptQuote(0.0, 100.0, 100, 100) {
		t.Fatalf("expected 0 bid price to be rejected")
	}
	if h.AcceptQuote(100.0, -1.0, 100, 100) {
		t.Fatalf("expected negative ask price to be rejected")
	}
	if h.AcceptQuote(math.NaN(), 100.0, 100, 100) {
		t.Fatalf("expected NaN bid price to be rejected")
	}
	if h.DedupDroppedQuotes() != 0 {
		t.Fatalf("invalid quotes must not increment dedupDroppedQuotes, got %d", h.DedupDroppedQuotes())
	}
	if h.hasDedupBaseline {
		t.Fatalf("invalid quotes must not set baseline")
	}

	// 2. First valid quote B is accepted and establishes baseline
	if !h.AcceptQuote(100.0, 101.0, 10, 20) {
		t.Fatalf("expected first valid quote to be accepted")
	}
	if h.DedupDroppedQuotes() != 0 {
		t.Fatalf("expected 0 dropped quotes, got %d", h.DedupDroppedQuotes())
	}

	// 3. Consecutive identical quote B is dropped
	if h.AcceptQuote(100.0, 101.0, 10, 20) {
		t.Fatalf("expected consecutive identical quote to be dropped")
	}
	if h.DedupDroppedQuotes() != 1 {
		t.Fatalf("expected 1 dropped quote, got %d", h.DedupDroppedQuotes())
	}

	// 4. Invalid quote Z between two Bs does NOT act as separator ([B, Z, B])
	if h.AcceptQuote(0.0, 101.0, 10, 20) {
		t.Fatalf("expected invalid quote to be rejected")
	}
	if h.DedupDroppedQuotes() != 1 {
		t.Fatalf("invalid quote must not increment dropped quotes, got %d", h.DedupDroppedQuotes())
	}
	// Subsequent identical quote B must still be dropped
	if h.AcceptQuote(100.0, 101.0, 10, 20) {
		t.Fatalf("expected second B in [B, Z, B] to be dropped")
	}
	if h.DedupDroppedQuotes() != 2 {
		t.Fatalf("expected 2 dropped quotes, got %d", h.DedupDroppedQuotes())
	}

	// 5. Price change is accepted and establishes new baseline
	if !h.AcceptQuote(100.5, 101.0, 10, 20) {
		t.Fatalf("expected bid price change to be accepted")
	}
	if h.DedupDroppedQuotes() != 2 {
		t.Fatalf("expected 2 dropped quotes, got %d", h.DedupDroppedQuotes())
	}

	// 6. Size change is accepted and establishes new baseline
	if !h.AcceptQuote(100.5, 101.0, 15, 20) {
		t.Fatalf("expected bid size change to be accepted")
	}
	if !h.AcceptQuote(100.5, 101.0, 15, 25) {
		t.Fatalf("expected ask size change to be accepted")
	}

	// 7. NaN sizes never compare equal (NaN == NaN is false) and are always accepted
	if !h.AcceptQuote(100.5, 101.0, math.NaN(), 25) {
		t.Fatalf("expected NaN bid size quote to be accepted")
	}
	if !h.AcceptQuote(100.5, 101.0, math.NaN(), 25) {
		t.Fatalf("expected subsequent NaN bid size quote to also be accepted due to NaN != NaN")
	}
	if h.DedupDroppedQuotes() != 2 {
		t.Fatalf("expected dropped quotes to remain 2, got %d", h.DedupDroppedQuotes())
	}
}

func TestProjectorIngestTickDuplicateSuppression(t *testing.T) {
	shmName := fmt.Sprintf("test_dedup_ingest_%d", os.Getpid())
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
		UnlinkOnExit:    true,
	}
	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	proj := NewProjector(prod, phases, []string{"AAPL"}, time.Second)
	proj.SetWatermarkBuffer(0)
	baseTS := int64(1_000_000_000)

	// Ingest quote 1
	q1 := feed.Tick{
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		SIPTimestampNS: baseTS,
		BidPx:          150.00,
		AskPx:          150.05,
		BidSz:          100,
		AskSz:          200,
	}
	if err := proj.IngestTick(q1); err != nil {
		t.Fatalf("IngestTick q1 failed: %v", err)
	}

	hist := proj.uniqueHistories[0]
	if hist.ccmBidQuoteCnt != 1.0 || hist.ccmAskQuoteCnt != 1.0 {
		t.Fatalf("expected quote count 1, got bid=%.1f ask=%.1f", hist.ccmBidQuoteCnt, hist.ccmAskQuoteCnt)
	}

	// Ingest duplicate quote 2 at ts + 10ms with identical NBBO
	q2 := feed.Tick{
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		SIPTimestampNS: baseTS + 10_000_000,
		BidPx:          150.00,
		AskPx:          150.05,
		BidSz:          100,
		AskSz:          200,
	}
	if err := proj.IngestTick(q2); err != nil {
		t.Fatalf("IngestTick q2 failed: %v", err)
	}

	if hist.DedupDroppedQuotes() != 1 {
		t.Fatalf("expected 1 dropped duplicate quote, got %d", hist.DedupDroppedQuotes())
	}
	if hist.ccmBidQuoteCnt != 1.0 || hist.ccmAskQuoteCnt != 1.0 {
		t.Fatalf("expected quote count to remain 1 after duplicate, got bid=%.1f ask=%.1f",
			hist.ccmBidQuoteCnt, hist.ccmAskQuoteCnt)
	}

	// Ingest quote 3 with updated ask price
	q3 := feed.Tick{
		Symbol:         "AAPL",
		Type:           feed.TickQuote,
		SIPTimestampNS: baseTS + 20_000_000,
		BidPx:          150.00,
		AskPx:          150.06,
		BidSz:          100,
		AskSz:          200,
	}
	if err := proj.IngestTick(q3); err != nil {
		t.Fatalf("IngestTick q3 failed: %v", err)
	}

	if hist.DedupDroppedQuotes() != 1 {
		t.Fatalf("expected dropped quotes to remain 1, got %d", hist.DedupDroppedQuotes())
	}
	if hist.ccmBidQuoteCnt != 2.0 || hist.ccmAskQuoteCnt != 2.0 {
		t.Fatalf("expected quote count 2 after valid update, got bid=%.1f ask=%.1f",
			hist.ccmBidQuoteCnt, hist.ccmAskQuoteCnt)
	}
}

func TestProjectorIngestPreWindowTickDuplicateSuppression(t *testing.T) {
	shmName := fmt.Sprintf("test_dedup_pre_%d", os.Getpid())
	phases := []shm.PhaseConfig{
		{ID: 0, Name: "phase_0ms", OffsetMS: 0, Symbols: []string{"NVDA"}},
	}
	feats := make([]string, 44)
	for i := range feats {
		feats[i] = fmt.Sprintf("f%d", i)
	}
	cfg := shm.Config{
		Name:            shmName,
		MaxFrames:       64,
		Phases:          phases,
		UniqueSymbols:   []string{"NVDA"},
		Features:        feats,
		CadenceInterval: 1 * time.Second,
		UnlinkOnExit:    true,
	}
	prod, err := shm.CreateProducer(cfg)
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}
	defer prod.Close()

	proj := NewProjector(prod, phases, []string{"NVDA"}, time.Second)

	q1 := feed.Tick{
		Symbol:         "NVDA",
		Type:           feed.TickQuote,
		SIPTimestampNS: 500_000_000,
		BidPx:          120.00,
		AskPx:          120.10,
		BidSz:          50,
		AskSz:          80,
	}
	proj.IngestPreWindowTick(q1)

	hist := proj.uniqueHistories[0]
	if hist.ccmCumQuotes != 1 {
		t.Fatalf("expected ccmCumQuotes 1, got %d", hist.ccmCumQuotes)
	}

	// Duplicate quote
	q2 := feed.Tick{
		Symbol:         "NVDA",
		Type:           feed.TickQuote,
		SIPTimestampNS: 600_000_000,
		BidPx:          120.00,
		AskPx:          120.10,
		BidSz:          50,
		AskSz:          80,
	}
	proj.IngestPreWindowTick(q2)

	if hist.DedupDroppedQuotes() != 1 {
		t.Fatalf("expected 1 dropped quote, got %d", hist.DedupDroppedQuotes())
	}
	if hist.ccmCumQuotes != 1 {
		t.Fatalf("expected ccmCumQuotes to remain 1, got %d", hist.ccmCumQuotes)
	}
}
