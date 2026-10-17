package project

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

type liveTickItem struct {
	tick feed.Tick
	seq  uint64
}

type tickMinHeap struct {
	items []liveTickItem
}

func (h *tickMinHeap) Len() int { return len(h.items) }

func (h *tickMinHeap) less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if a.tick.SIPTimestampNS != b.tick.SIPTimestampNS {
		return a.tick.SIPTimestampNS < b.tick.SIPTimestampNS
	}
	if a.tick.Type != b.tick.Type {
		return a.tick.Type == feed.TickQuote
	}
	if a.tick.Symbol != b.tick.Symbol {
		return a.tick.Symbol < b.tick.Symbol
	}
	return a.seq < b.seq
}

func (h *tickMinHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *tickMinHeap) PushItem(item liveTickItem) {
	h.items = append(h.items, item)
	h.siftUp(len(h.items) - 1)
}

func (h *tickMinHeap) PopItem() liveTickItem {
	n := len(h.items)
	item := h.items[0]
	h.items[0] = h.items[n-1]
	h.items = h.items[:n-1]
	if n > 1 {
		h.siftDown(0)
	}
	return item
}

func (h *tickMinHeap) PeekItem() liveTickItem {
	return h.items[0]
}

func (h *tickMinHeap) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !h.less(i, p) {
			break
		}
		h.swap(i, p)
		i = p
	}
}

func (h *tickMinHeap) siftDown(i int) {
	n := len(h.items)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		smallest := left
		if right := left + 1; right < n && h.less(right, left) {
			smallest = right
		}
		if !h.less(smallest, i) {
			break
		}
		h.swap(i, smallest)
		i = smallest
	}
}

// Projector aggregates ticks and commits 1Hz multi-phase features into POSIX SHM.
type Projector struct {
	producer      *shm.Producer
	cadenceNS     int64
	phases        []shm.PhaseConfig
	uniqueSymbols []string
	symToUnique   map[string]int

	// Per-phase symbol histories: [phaseIdx][symbolPhaseIdx] (5-feature legacy mode)
	histories [][]*SymbolHistory

	// Mapping: symbol -> []PhaseRef{phaseIdx, symbolPhaseIdx}
	symbolPhaseRefs map[string][]phaseRef

	// CCM 44-slot mode state (one SymbolHistory per unique symbol)
	isCCM44            bool
	isLiveMode         bool
	nextSecondAnchorNS int64
	symNextSecondNS    []int64
	uniqueHistories    []*SymbolHistory
	uniqueLastCCMBar   [][44]float64
	symBarRing         [][4][44]float64
	symBarAnchor       [][4]int64
	phaseAsOf          [][][8]float64
	phaseAsOfCaptured  [][]bool

	// Current target anchor per phase
	phaseNextAnchor []int64
	phaseSlots      []int

	featBuffer       []float64
	phaseFeatBuffers [][]float64

	snapshots      []shm.SymbolSnapshot
	microTrackers  []MicroTracker
	microSnapshots []shm.SymbolMicroSnapshot
	trackers250ms  []Tracker250ms

	firstCommittedAnchor   int64
	lastCommittedAnchor    int64
	totalCommittedFrames   int
	coldStartBarsRemaining int
	// Watermark reorder buffer and stream-time state (CCM 44 live mode)
	watermarkBufferNS int64
	maxSIPTimestampNS int64
	maxSIPWallTime    time.Time
	liveSeq           uint64
	lateDroppedTicks  uint64
	warnedLateDrop    bool
	liveHeap          tickMinHeap
	targetSymbol      string
}

type phaseRef struct {
	phaseIdx       int
	symbolPhaseIdx int
}

// NewProjector creates and initializes the 1Hz projection engine.
func NewProjector(producer *shm.Producer, phases []shm.PhaseConfig, uniqueSymbols []string, cadence time.Duration) *Projector {
	cadenceNS := cadence.Nanoseconds()
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}

	symToUnique := make(map[string]int, len(uniqueSymbols))
	for i, s := range uniqueSymbols {
		symToUnique[s] = i
	}

	histories := make([][]*SymbolHistory, len(phases))
	symbolPhaseRefs := make(map[string][]phaseRef)

	isCCM44 := false
	isLiveMode := false
	if producer != nil && producer.Header() != nil {
		if producer.Header().Mode == shm.ModeLiveStreaming {
			isLiveMode = true
		}
		if len(producer.Header().Phases) > 0 && int(producer.Header().Phases[0].NumFeatures) == 44 {
			isCCM44 = true
		}
	}

	phaseFeatBuffers := make([][]float64, len(phases))
	for pIdx, p := range phases {
		histories[pIdx] = make([]*SymbolHistory, len(p.Symbols))
		nFeat := 5
		if producer != nil && pIdx < len(producer.Header().Phases) {
			hdrFeat := int(producer.Header().Phases[pIdx].NumFeatures)
			if hdrFeat > 0 {
				nFeat = hdrFeat
			}
		}
		phaseFeatBuffers[pIdx] = make([]float64, len(p.Symbols)*nFeat)
		for sIdx, s := range p.Symbols {
			histories[pIdx][sIdx] = &SymbolHistory{}
			symbolPhaseRefs[s] = append(symbolPhaseRefs[s], phaseRef{
				phaseIdx:       pIdx,
				symbolPhaseIdx: sIdx,
			})
		}
	}

	var uniqueHistories []*SymbolHistory
	var uniqueLastCCMBar [][44]float64
	var symBarRing [][4][44]float64
	var symBarAnchor [][4]int64
	var symNextSecondNS []int64
	var phaseAsOf [][][8]float64
	var phaseAsOfCaptured [][]bool

	if isCCM44 {
		nan := math.NaN()
		uniqueHistories = make([]*SymbolHistory, len(uniqueSymbols))
		uniqueLastCCMBar = make([][44]float64, len(uniqueSymbols))
		symBarRing = make([][4][44]float64, len(uniqueSymbols))
		symBarAnchor = make([][4]int64, len(uniqueSymbols))
		symNextSecondNS = make([]int64, len(uniqueSymbols))
		for i := range uniqueSymbols {
			h := &SymbolHistory{}
			h.InitCCM44()
			uniqueHistories[i] = h
			for k := 14; k <= 29; k++ {
				uniqueLastCCMBar[i][k] = nan
			}
			for k := 30; k <= 33; k++ {
				uniqueLastCCMBar[i][k] = -1.0
			}
			for k := 36; k <= 41; k++ {
				uniqueLastCCMBar[i][k] = nan
			}
		}
		phaseAsOf = make([][][8]float64, len(phases))
		phaseAsOfCaptured = make([][]bool, len(phases))
		for pIdx := range phases {
			phaseAsOf[pIdx] = make([][8]float64, len(uniqueSymbols))
			phaseAsOfCaptured[pIdx] = make([]bool, len(uniqueSymbols))
		}
	}

	var liveHeap tickMinHeap
	var watermarkBufferNS int64 = 0
	if isLiveMode && isCCM44 {
		watermarkBufferNS = 100_000_000 // 100ms default
		liveHeap = tickMinHeap{items: make([]liveTickItem, 0, 16384)}
	}

	trackers250ms := make([]Tracker250ms, len(uniqueSymbols))
	for i := range trackers250ms {
		var ring *shm.SymbolRing250ms
		if producer != nil {
			ring = producer.Ring250ms(i)
		}
		trackers250ms[i].Init(ring)
	}

	return &Projector{
		producer:          producer,
		cadenceNS:         cadenceNS,
		phases:            phases,
		uniqueSymbols:     uniqueSymbols,
		symToUnique:       symToUnique,
		histories:         histories,
		symbolPhaseRefs:   symbolPhaseRefs,
		isCCM44:           isCCM44,
		isLiveMode:        isLiveMode,
		watermarkBufferNS: watermarkBufferNS,
		liveHeap:          liveHeap,
		symNextSecondNS:   symNextSecondNS,
		uniqueHistories:   uniqueHistories,
		uniqueLastCCMBar:  uniqueLastCCMBar,
		symBarRing:        symBarRing,
		symBarAnchor:      symBarAnchor,
		phaseAsOf:         phaseAsOf,
		phaseAsOfCaptured: phaseAsOfCaptured,
		phaseNextAnchor:   make([]int64, len(phases)),
		phaseSlots:        make([]int, len(phases)),
		featBuffer:        make([]float64, 5),
		phaseFeatBuffers:  phaseFeatBuffers,
		snapshots:         make([]shm.SymbolSnapshot, len(uniqueSymbols)),
		microTrackers:     make([]MicroTracker, len(uniqueSymbols)),
		microSnapshots:    make([]shm.SymbolMicroSnapshot, len(uniqueSymbols)),
		trackers250ms:     trackers250ms,
	}
}

// IsCCM44 returns true when the projector is configured with the 44-slot CCM 1Hz schema.
func (p *Projector) IsCCM44() bool {
	return p.isCCM44
}

// SetWatermarkBuffer overrides the watermark latency buffer.
func (p *Projector) SetWatermarkBuffer(d time.Duration) {
	p.watermarkBufferNS = d.Nanoseconds()
	if p.watermarkBufferNS > 0 && p.isCCM44 {
		p.isLiveMode = true
		if p.liveHeap.items == nil {
			p.liveHeap = tickMinHeap{items: make([]liveTickItem, 0, 16384)}
		}
	}
}

// WatermarkBuffer returns the configured watermark latency buffer duration.
func (p *Projector) WatermarkBuffer() time.Duration {
	return time.Duration(p.watermarkBufferNS) * time.Nanosecond
}

// MaxSIPTimestampNS returns the highest SIP timestamp observed across all ingested ticks.
func (p *Projector) MaxSIPTimestampNS() int64 {
	return p.maxSIPTimestampNS
}

// LateDroppedTicks returns the count of ticks dropped due to arriving after their bar closed.
func (p *Projector) LateDroppedTicks() uint64 {
	return p.lateDroppedTicks
}

// VirtualStreamNow returns the current virtual SIP stream time.
// Anchored to maxSIPTimestampNS and advances 1:1 with wall-clock time during quiet periods.
func (p *Projector) VirtualStreamNow(now time.Time) int64 {
	if p.maxSIPTimestampNS == 0 {
		return 0
	}
	elapsed := now.Sub(p.maxSIPWallTime).Nanoseconds()
	if elapsed < 0 {
		elapsed = 0
	}
	return p.maxSIPTimestampNS + elapsed
}

// SetTargetSymbol restricts projection and frame commits to a single symbol (e.g. on-demand worker chunk replay).
func (p *Projector) SetTargetSymbol(sym string) {
	p.targetSymbol = sym
}

// SetStartAnchor sets the initial window start anchor across all phases.
func (p *Projector) SetStartAnchor(startNS int64) {
	for pIdx, pCfg := range p.phases {
		offsetNS := int64(pCfg.OffsetMS) * 1_000_000
		firstAnchor := ((startNS-offsetNS)/p.cadenceNS)*p.cadenceNS + offsetNS
		if firstAnchor <= startNS {
			firstAnchor += p.cadenceNS
		}
		p.phaseNextAnchor[pIdx] = firstAnchor
	}
	if p.isCCM44 {
		secAnchor := (startNS / p.cadenceNS) * p.cadenceNS
		if secAnchor <= startNS {
			secAnchor += p.cadenceNS
		}
		p.nextSecondAnchorNS = secAnchor
		for uIdx := range p.symNextSecondNS {
			p.symNextSecondNS[uIdx] = secAnchor
		}
	}
}

// SeedState initializes the prevailing bid, ask, and trade price for a symbol prior to window start.
func (p *Projector) SeedState(symbol string, lastBid, lastAsk, lastPrice float64) {
	refs := p.symbolPhaseRefs[symbol]
	for _, ref := range refs {
		hist := p.histories[ref.phaseIdx][ref.symbolPhaseIdx]
		if lastBid > 0 || lastAsk > 0 {
			hist.UpdateQuote(lastBid, lastAsk)
		}
		if lastPrice > 0 {
			hist.UpdateTrade(lastPrice, 0)
		}
	}
	if uIdx, ok := p.symToUnique[symbol]; ok && uIdx >= 0 && uIdx < len(p.snapshots) {
		snap := &p.snapshots[uIdx]
		if lastBid > 0 && lastAsk > 0 {
			snap.BidPx = lastBid
			snap.AskPx = lastAsk
			snap.Midprice = (lastBid + lastAsk) / 2.0
			snap.Spread = lastAsk - lastBid
		} else if lastBid > 0 {
			snap.BidPx = lastBid
		} else if lastAsk > 0 {
			snap.AskPx = lastAsk
		}
		if lastPrice > 0 {
			snap.LastTradePx = lastPrice
		}
		p.producer.WriteSnapshot(uIdx, snap)
		mt := &p.microTrackers[uIdx]
		mt.SeedQuote(lastBid, lastAsk)
		t250 := &p.trackers250ms[uIdx]
		t250.OnQuote(0, lastBid, lastAsk, p.producer.Ring250ms(uIdx))
		micro := &p.microSnapshots[uIdx]
		mt.ComputeSnapshot(0, micro)
		micro.LastCol250ms = t250.LastClosedCol()
		micro.Trailing15mCnt = t250.Trailing15mCnt()
		micro.Trailing15mQ = t250.Trailing15mQ()
		p.producer.WriteMicroSnapshot(uIdx, micro)
	}
}

// SetColdStart configures whether rolling return queues are in warmup.
// When cold is true, exactly 15 bars will be committed with FlagColdStart.
func (p *Projector) SetColdStart(cold bool) {
	if cold {
		p.coldStartBarsRemaining = 15
	} else {
		p.coldStartBarsRemaining = 0
	}
}

// IsColdStart returns true if rolling return queues are currently in warmup.
func (p *Projector) IsColdStart() bool {
	return p.coldStartBarsRemaining > 0
}

// SeedHistory populates rolling return history for phaseIdx and symbolPhaseIdx from pre-existing frame bars.
func (p *Projector) SeedHistory(phaseIdx int, symbolPhaseIdx int, bars []shm.FrameBar) {
	if phaseIdx < 0 || phaseIdx >= len(p.phases) || symbolPhaseIdx < 0 || len(bars) == 0 {
		return
	}
	pCfg := p.phases[phaseIdx]
	if symbolPhaseIdx >= len(pCfg.Symbols) {
		return
	}
	sym := pCfg.Symbols[symbolPhaseIdx]
	hist := p.histories[phaseIdx][symbolPhaseIdx]

	uIdx, ok := p.symToUnique[sym]
	var lastPx, lastBid, lastAsk float64
	if ok && p.producer != nil {
		snap := p.producer.Snapshot(uIdx)
		if snap != nil {
			if snap.LastTradePx > 0 {
				lastPx = snap.LastTradePx
			} else if snap.Midprice > 0 {
				lastPx = snap.Midprice
			}
			lastBid = snap.BidPx
			lastAsk = snap.AskPx
		}
	}

	hist.SeedFromBars(lastPx, lastBid, lastAsk, bars, p.cadenceNS)

	lastAnchor := bars[len(bars)-1].AnchorNS
	nextAnchor := lastAnchor + p.cadenceNS
	if nextAnchor > p.phaseNextAnchor[phaseIdx] {
		p.phaseNextAnchor[phaseIdx] = nextAnchor
		p.phaseSlots[phaseIdx] = int(nextAnchor / p.cadenceNS)
	}
	if p.firstCommittedAnchor == 0 || bars[0].AnchorNS < p.firstCommittedAnchor {
		p.firstCommittedAnchor = bars[0].AnchorNS
	}
	if lastAnchor > p.lastCommittedAnchor {
		p.lastCommittedAnchor = lastAnchor
	}
}

// SeedPrevailingPrice initializes baseline price and quote state across all phases containing this symbol.
func (p *Projector) SeedPrevailingPrice(symbol string, lastPx, lastBid, lastAsk float64) {
	refs := p.symbolPhaseRefs[symbol]
	for _, ref := range refs {
		hist := p.histories[ref.phaseIdx][ref.symbolPhaseIdx]
		hist.SeedPrevailingPrice(lastPx, lastBid, lastAsk)
	}
}

// IngestPreWindowTick advances cumulative Lee-Ready, venue quote, and top-of-book state for ticks prior to startNS.
func (p *Projector) IngestPreWindowTick(tick feed.Tick) {
	uIdx, ok := p.symToUnique[tick.Symbol]
	if !ok {
		return
	}
	t250 := &p.trackers250ms[uIdx]
	ring250 := p.producer.Ring250ms(uIdx)
	if p.isCCM44 {
		hist := p.uniqueHistories[uIdx]
		if tick.Type == feed.TickTrade {
			p.updateSnapshot(uIdx, tick)
			hist.IngestPreWindowCCMTrade(tick.SIPTimestampNS, tick.ParticipantTimestampNS, tick.Price, tick.Size, tick.TradeExch)
			t250.OnTrade(tick.SIPTimestampNS, tick.Price, tick.Size, tick.IsDark, ring250)
		} else {
			if !hist.AcceptQuote(tick.BidPx, tick.AskPx, tick.BidSz, tick.AskSz) {
				return
			}
			p.updateSnapshot(uIdx, tick)
			hist.IngestPreWindowCCMQuote(tick.SIPTimestampNS, tick.ParticipantTimestampNS, tick.BidPx, tick.AskPx, tick.BidSz, tick.AskSz, tick.BidExch, tick.AskExch)
			t250.OnQuote(tick.SIPTimestampNS, tick.BidPx, tick.AskPx, ring250)
		}
	} else {
		p.updateSnapshot(uIdx, tick)
		if tick.Type == feed.TickTrade {
			t250.OnTrade(tick.SIPTimestampNS, tick.Price, tick.Size, tick.IsDark, ring250)
		} else {
			t250.OnQuote(tick.SIPTimestampNS, tick.BidPx, tick.AskPx, ring250)
		}
	}
}

func (p *Projector) closeSymbolSecondBar(uIdx int, secAnchorNS int64) {
	hist := p.uniqueHistories[uIdx]
	hist.CloseCCMBar(secAnchorNS, &p.uniqueLastCCMBar[uIdx])
	slot := int((secAnchorNS / p.cadenceNS) & 3)
	p.symBarRing[uIdx][slot] = p.uniqueLastCCMBar[uIdx]
	p.symBarAnchor[uIdx][slot] = secAnchorNS
	p.symNextSecondNS[uIdx] = secAnchorNS + p.cadenceNS
}

func (p *Projector) closeSecondBar(secAnchorNS int64) {
	if p.targetSymbol != "" {
		if uIdx, ok := p.symToUnique[p.targetSymbol]; ok {
			for p.symNextSecondNS[uIdx] <= secAnchorNS {
				p.closeSymbolSecondBar(uIdx, p.symNextSecondNS[uIdx])
			}
		}
		return
	}
	for uIdx := range p.uniqueHistories {
		for p.symNextSecondNS[uIdx] <= secAnchorNS {
			p.closeSymbolSecondBar(uIdx, p.symNextSecondNS[uIdx])
		}
	}
}

func (p *Projector) ingestSortedCCM44Tick(tick feed.Tick, updateSnap bool) error {
	ts := tick.SIPTimestampNS

	// 1. Initialize phase start anchors on first sorted tick
	for pIdx, pCfg := range p.phases {
		if p.phaseNextAnchor[pIdx] == 0 {
			offsetNS := int64(pCfg.OffsetMS) * 1_000_000
			firstAnchor := ((ts-offsetNS)/p.cadenceNS)*p.cadenceNS + offsetNS
			if firstAnchor <= ts {
				firstAnchor += p.cadenceNS
			}
			p.phaseNextAnchor[pIdx] = firstAnchor
		}
	}
	if p.nextSecondAnchorNS == 0 {
		secAnchor := (ts / p.cadenceNS) * p.cadenceNS
		if secAnchor <= ts {
			secAnchor += p.cadenceNS
		}
		p.nextSecondAnchorNS = secAnchor
		for uIdx := range p.symNextSecondNS {
			p.symNextSecondNS[uIdx] = secAnchor
		}
	}

	uIdx, ok := p.symToUnique[tick.Symbol]
	if !ok {
		return nil
	}

	if tick.Type == feed.TickQuote {
		if !p.uniqueHistories[uIdx].AcceptQuote(tick.BidPx, tick.AskPx, tick.BidSz, tick.AskSz) {
			return nil
		}
	}

	isLate := p.symNextSecondNS[uIdx] > 0 && ts < p.symNextSecondNS[uIdx]-p.cadenceNS
	if isLate {
		p.lateDroppedTicks++
		if !p.warnedLateDrop {
			p.warnedLateDrop = true
			log.Printf("[PROJECTOR] Late tail tick past closed bar: %s %v ts=%d < boundary=%d (total: %d)",
				tick.Symbol, tick.Type, ts, p.symNextSecondNS[uIdx]-p.cadenceNS, p.lateDroppedTicks)
		}
	} else if !p.isLiveMode {
		for {
			earliestPIdx := -1
			var earliestPhaseAnchor int64 = 0
			for pIdx := range p.phases {
				target := p.phaseNextAnchor[pIdx]
				// Terminal as-of state includes ticks with ts <= target (side='right')
				if ts > target {
					if earliestPhaseAnchor == 0 || target < earliestPhaseAnchor {
						earliestPhaseAnchor = target
						earliestPIdx = pIdx
					}
				}
			}

			// Whole-second bar [secAnchor - 1s, secAnchor) includes ticks with ts < secAnchor (side='left')
			secReady := p.nextSecondAnchorNS > 0 && ts >= p.nextSecondAnchorNS
			if secReady && (earliestPIdx < 0 || p.nextSecondAnchorNS <= earliestPhaseAnchor) {
				p.closeSecondBar(p.nextSecondAnchorNS)
				p.nextSecondAnchorNS += p.cadenceNS
				continue
			}

			if earliestPIdx < 0 {
				break
			}

			if err := p.closePhase(earliestPIdx, earliestPhaseAnchor); err != nil {
				return err
			}
			p.phaseNextAnchor[earliestPIdx] += p.cadenceNS
			p.phaseSlots[earliestPIdx]++
		}
	}

	if updateSnap {
		p.updateSnapshot(uIdx, tick)
	}

	t250 := &p.trackers250ms[uIdx]
	ring250 := p.producer.Ring250ms(uIdx)
	hist := p.uniqueHistories[uIdx]
	if tick.Type == feed.TickTrade {
		if isLate {
			hist.IngestPreWindowCCMTrade(tick.SIPTimestampNS, tick.ParticipantTimestampNS, tick.Price, tick.Size, tick.TradeExch)
		} else {
			hist.IngestCCMTrade(tick.SIPTimestampNS, tick.ParticipantTimestampNS, tick.Price, tick.Size, tick.TradeExch, tick.IsDark)
			t250.OnTrade(tick.SIPTimestampNS, tick.Price, tick.Size, tick.IsDark, ring250)
		}
	} else {
		if isLate {
			hist.IngestPreWindowCCMQuote(tick.SIPTimestampNS, tick.ParticipantTimestampNS, tick.BidPx, tick.AskPx, tick.BidSz, tick.AskSz, tick.BidExch, tick.AskExch)
		} else {
			hist.IngestCCMQuote(tick.SIPTimestampNS, tick.ParticipantTimestampNS, tick.BidPx, tick.AskPx, tick.BidSz, tick.AskSz, tick.BidExch, tick.AskExch)
			t250.OnQuote(tick.SIPTimestampNS, tick.BidPx, tick.AskPx, ring250)
		}
	}
	return nil
}

// IngestTick processes a market tick and commits completed 1Hz frames when time boundaries are crossed.
func (p *Projector) IngestTick(tick feed.Tick) error {
	ts := tick.SIPTimestampNS

	if p.isCCM44 {
		uIdx, ok := p.symToUnique[tick.Symbol]
		if !ok {
			return nil
		}

		if p.isLiveMode {
			// 1. Immediately update snapshot at wire arrival (0ms delay for live execution)
			p.updateSnapshot(uIdx, tick)

			// 2. Track max SIP timestamp and wall time
			if ts > p.maxSIPTimestampNS {
				p.maxSIPTimestampNS = ts
				p.maxSIPWallTime = time.Now()
			}

			// 3. Push to min-heap
			p.liveSeq++
			p.liveHeap.PushItem(liveTickItem{
				tick: tick,
				seq:  p.liveSeq,
			})

			// 4. Drain heap immediately if watermark buffer is 0 (direct test mode)
			if p.watermarkBufferNS == 0 {
				return p.Flush(tick.SIPTimestampNS)
			}
			return nil
		}

		// Historical replay mode: globally sorted stream across all symbols
		if ts > p.maxSIPTimestampNS {
			p.maxSIPTimestampNS = ts
		}
		return p.ingestSortedCCM44Tick(tick, true)
	}

	// 1. Initialize phase start anchors on first tick (5-feature legacy mode)
	for pIdx, pCfg := range p.phases {
		if p.phaseNextAnchor[pIdx] == 0 {
			offsetNS := int64(pCfg.OffsetMS) * 1_000_000
			firstAnchor := ((ts-offsetNS)/p.cadenceNS)*p.cadenceNS + offsetNS
			if firstAnchor <= ts {
				firstAnchor += p.cadenceNS
			}
			p.phaseNextAnchor[pIdx] = firstAnchor
		}
	}

	// 2. Check if any phase has completed its window (5-feature legacy mode)
	for {
		earliestPIdx := -1
		var earliestAnchor int64 = 0

		for pIdx := range p.phases {
			target := p.phaseNextAnchor[pIdx]
			if ts >= target {
				if earliestAnchor == 0 || target < earliestAnchor {
					earliestAnchor = target
					earliestPIdx = pIdx
				}
			}
		}

		if earliestPIdx < 0 {
			break
		}

		// Close window for earliestPIdx at earliestAnchor
		if err := p.closePhase(earliestPIdx, earliestAnchor); err != nil {
			return err
		}
		p.phaseNextAnchor[earliestPIdx] += p.cadenceNS
		p.phaseSlots[earliestPIdx]++
	}

	// 3. Update top-of-book snapshot for unique symbol
	if uIdx, ok := p.symToUnique[tick.Symbol]; ok {
		p.updateSnapshot(uIdx, tick)
		t250 := &p.trackers250ms[uIdx]
		ring250 := p.producer.Ring250ms(uIdx)
		if tick.Type == feed.TickTrade {
			t250.OnTrade(tick.SIPTimestampNS, tick.Price, tick.Size, tick.IsDark, ring250)
		} else {
			t250.OnQuote(tick.SIPTimestampNS, tick.BidPx, tick.AskPx, ring250)
		}
	}

	// 4. Update rolling history for all phases containing this symbol (including cross-assets)
	refs := p.symbolPhaseRefs[tick.Symbol]
	for _, ref := range refs {
		hist := p.histories[ref.phaseIdx][ref.symbolPhaseIdx]
		if tick.Type == feed.TickTrade {
			hist.UpdateTrade(tick.Price, tick.Size)
		} else {
			hist.UpdateQuote(tick.BidPx, tick.AskPx)
		}
	}

	return nil
}

func (p *Projector) closePhase(pIdx int, anchorNS int64) error {
	startNS := time.Now().UnixNano()
	pCfg := p.phases[pIdx]
	slot := p.phaseSlots[pIdx]
	featBuf := p.phaseFeatBuffers[pIdx]

	if p.isCCM44 {
		anchorFloorNS := (anchorNS / p.cadenceNS) * p.cadenceNS
		barSlot := int((anchorFloorNS / p.cadenceNS) & 3)
		for sIdx, sym := range pCfg.Symbols {
			if p.targetSymbol != "" && sym != p.targetSymbol {
				continue
			}
			uIdx, ok := p.symToUnique[sym]
			if !ok {
				continue
			}
			p.trackers250ms[uIdx].AdvanceTo(anchorNS, p.producer.Ring250ms(uIdx))
			for p.symNextSecondNS[uIdx] <= anchorFloorNS {
				p.closeSymbolSecondBar(uIdx, p.symNextSecondNS[uIdx])
			}
			base := sIdx * 44
			if p.symBarAnchor[uIdx][barSlot] == anchorFloorNS {
				copy(featBuf[base:base+36], p.symBarRing[uIdx][barSlot][0:36])
			} else {
				copy(featBuf[base:base+36], p.uniqueLastCCMBar[uIdx][0:36])
			}
			if p.phaseAsOfCaptured[pIdx][uIdx] {
				copy(featBuf[base+36:base+44], p.phaseAsOf[pIdx][uIdx][:])
				p.phaseAsOfCaptured[pIdx][uIdx] = false
			} else {
				p.uniqueHistories[uIdx].WriteCCMTerminalState(&p.uniqueLastCCMBar[uIdx])
				copy(featBuf[base+36:base+44], p.uniqueLastCCMBar[uIdx][36:44])
			}
			if p.targetSymbol != "" {
				if err := p.producer.CommitSymbolMetrics(pIdx, sIdx, anchorNS, featBuf[base:base+44]); err != nil {
					return fmt.Errorf("commit symbol frame p%d s%d @ %d: %w", pIdx, sIdx, anchorNS, err)
				}
			}
		}
	} else {
		for sIdx, sym := range pCfg.Symbols {
			if p.targetSymbol != "" && sym != p.targetSymbol {
				continue
			}
			if uIdx, ok := p.symToUnique[sym]; ok {
				p.trackers250ms[uIdx].AdvanceTo(anchorNS, p.producer.Ring250ms(uIdx))
			}
			hist := p.histories[pIdx][sIdx]
			r1, r5, r15, v1, sp := hist.CloseBar(slot)

			base := sIdx * 5
			featBuf[base] = r1
			featBuf[base+1] = r5
			featBuf[base+2] = r15
			featBuf[base+3] = v1
			featBuf[base+4] = sp
			if p.targetSymbol != "" {
				if err := p.producer.CommitSymbolMetrics(pIdx, sIdx, anchorNS, featBuf[base:base+5]); err != nil {
					return fmt.Errorf("commit symbol frame p%d s%d @ %d: %w", pIdx, sIdx, anchorNS, err)
				}
			}
		}
	}

	if p.targetSymbol == "" {
		if err := p.producer.CommitPhaseFrame(pIdx, anchorNS, featBuf); err != nil {
			return fmt.Errorf("commit phase frame p%d @ %d: %w", pIdx, anchorNS, err)
		}
	}

	if p.coldStartBarsRemaining > 0 {
		p.producer.SetFrameFlags(pIdx, anchorNS, shm.FlagColdStart)
	}

	publishLatNS := time.Now().UnixNano() - startNS
	p.producer.CommitFrameFinalizeWithLatency(anchorNS, publishLatNS)
	if p.firstCommittedAnchor == 0 {
		p.firstCommittedAnchor = anchorNS
	}
	if p.lastCommittedAnchor = anchorNS; p.lastCommittedAnchor < anchorNS {
		p.lastCommittedAnchor = anchorNS
	}
	p.totalCommittedFrames++

	if pIdx == len(p.phases)-1 && p.coldStartBarsRemaining > 0 {
		p.coldStartBarsRemaining--
	}
	return nil
}

func (p *Projector) updateSnapshot(uIdx int, tick feed.Tick) {
	if uIdx < 0 || uIdx >= len(p.snapshots) {
		return
	}
	snap := &p.snapshots[uIdx]
	if tick.SIPTimestampNS > snap.SIPTimestampNS {
		snap.SIPTimestampNS = tick.SIPTimestampNS
		snap.RecvTimestampNS = tick.SIPTimestampNS
	}

	mt := &p.microTrackers[uIdx]
	if tick.Type == feed.TickTrade {
		snap.LastTradePx = tick.Price
		snap.LastTradeSz = tick.Size
		snap.Conditions++
		mt.OnTradeWithExch(tick.SIPTimestampNS, tick.ParticipantTimestampNS, tick.Price, tick.Size, tick.TradeExch)
	} else {
		snap.BidPx = tick.BidPx
		snap.AskPx = tick.AskPx
		snap.BidSz = tick.BidSz
		snap.AskSz = tick.AskSz
		if tick.BidPx > 0 && tick.AskPx > 0 {
			snap.Midprice = (tick.BidPx + tick.AskPx) / 2.0
			snap.Spread = tick.AskPx - tick.BidPx
		}
		mt.OnQuoteWithExch(tick.SIPTimestampNS, tick.ParticipantTimestampNS, tick.BidPx, tick.AskPx, tick.BidExch)
	}

	p.producer.WriteSnapshot(uIdx, snap)
	if p.isLiveMode {
		micro := &p.microSnapshots[uIdx]
		mt.ComputeSnapshot(snap.SIPTimestampNS, micro)
		p.producer.WriteMicroSnapshot(uIdx, micro)
	}
}

// Flush closes remaining bars up to targetEndNS.
func (p *Projector) Flush(targetEndNS int64) error {
	if p.isCCM44 {
		for {
			earliestPIdx := -1
			var earliestPhaseAnchor int64 = 0
			for pIdx := range p.phases {
				target := p.phaseNextAnchor[pIdx]
				if target > 0 && target <= targetEndNS {
					if earliestPhaseAnchor == 0 || target < earliestPhaseAnchor {
						earliestPhaseAnchor = target
						earliestPIdx = pIdx
					}
				}
			}

			secReady := p.nextSecondAnchorNS > 0 && p.nextSecondAnchorNS <= targetEndNS
			if secReady && (earliestPIdx < 0 || p.nextSecondAnchorNS <= earliestPhaseAnchor) {
				if p.isLiveMode {
					for p.liveHeap.Len() > 0 && p.liveHeap.PeekItem().tick.SIPTimestampNS < p.nextSecondAnchorNS {
						item := p.liveHeap.PopItem()
						if err := p.ingestSortedCCM44Tick(item.tick, false); err != nil {
							return err
						}
					}
				}
				p.closeSecondBar(p.nextSecondAnchorNS)
				p.nextSecondAnchorNS += p.cadenceNS
				continue
			}

			if earliestPIdx < 0 {
				break
			}

			if p.isLiveMode {
				for p.liveHeap.Len() > 0 && p.liveHeap.PeekItem().tick.SIPTimestampNS <= earliestPhaseAnchor {
					item := p.liveHeap.PopItem()
					if err := p.ingestSortedCCM44Tick(item.tick, false); err != nil {
						return err
					}
				}
			}

			if err := p.closePhase(earliestPIdx, earliestPhaseAnchor); err != nil {
				return err
			}
			p.phaseNextAnchor[earliestPIdx] += p.cadenceNS
			p.phaseSlots[earliestPIdx]++
		}

		if p.isLiveMode {
			for p.liveHeap.Len() > 0 && p.liveHeap.PeekItem().tick.SIPTimestampNS <= targetEndNS {
				item := p.liveHeap.PopItem()
				if err := p.ingestSortedCCM44Tick(item.tick, false); err != nil {
					return err
				}
			}
		}
	} else {
		for pIdx := range p.phases {
			for p.phaseNextAnchor[pIdx] <= targetEndNS {
				anchor := p.phaseNextAnchor[pIdx]
				if err := p.closePhase(pIdx, anchor); err != nil {
					return err
				}
				p.phaseNextAnchor[pIdx] += p.cadenceNS
				p.phaseSlots[pIdx]++
			}
		}
	}
	snapTS := targetEndNS
	if p.maxSIPTimestampNS > snapTS {
		snapTS = p.maxSIPTimestampNS
	}
	if snapTS > 0 {
		if p.targetSymbol != "" {
			if uIdx, ok := p.symToUnique[p.targetSymbol]; ok {
				t250 := &p.trackers250ms[uIdx]
				t250.AdvanceTo(targetEndNS, p.producer.Ring250ms(uIdx))
				mt := &p.microTrackers[uIdx]
				micro := &p.microSnapshots[uIdx]
				if mt.initialized {
					mt.ComputeSnapshot(snapTS, micro)
				}
				micro.LastCol250ms = t250.LastClosedCol()
				micro.Trailing15mCnt = t250.Trailing15mCnt()
				micro.Trailing15mQ = t250.Trailing15mQ()
				if mt.initialized || micro.LastCol250ms >= 0 {
					p.producer.WriteMicroSnapshot(uIdx, micro)
				}
			}
		} else {
			for uIdx := range p.microTrackers {
				t250 := &p.trackers250ms[uIdx]
				t250.AdvanceTo(targetEndNS, p.producer.Ring250ms(uIdx))
				mt := &p.microTrackers[uIdx]
				micro := &p.microSnapshots[uIdx]
				if mt.initialized {
					mt.ComputeSnapshot(snapTS, micro)
				}
				micro.LastCol250ms = t250.LastClosedCol()
				micro.Trailing15mCnt = t250.Trailing15mCnt()
				micro.Trailing15mQ = t250.Trailing15mQ()
				if mt.initialized || micro.LastCol250ms >= 0 {
					p.producer.WriteMicroSnapshot(uIdx, micro)
				}
			}
		}
	}
	return nil
}

// CommittedFrames returns the total number of frames committed to SHM.
func (p *Projector) CommittedFrames() int {
	return p.totalCommittedFrames
}

// AnchorRange returns the first and last anchor timestamps committed to SHM.
func (p *Projector) AnchorRange() (int64, int64) {
	return p.firstCommittedAnchor, p.lastCommittedAnchor
}

