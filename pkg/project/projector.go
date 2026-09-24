package project

import (
	"fmt"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/feed"
	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

// Projector aggregates ticks and commits 1Hz multi-phase features into POSIX SHM.
type Projector struct {
	producer      *shm.Producer
	cadenceNS     int64
	phases        []shm.PhaseConfig
	uniqueSymbols []string
	symToUnique   map[string]int

	// Per-phase symbol histories: [phaseIdx][symbolPhaseIdx]
	histories [][]*SymbolHistory

	// Mapping: symbol -> []PhaseRef{phaseIdx, symbolPhaseIdx}
	symbolPhaseRefs map[string][]phaseRef

	// Current target anchor per phase
	phaseNextAnchor []int64
	phaseSlots      []int

	featBuffer []float64

	firstCommittedAnchor   int64
	lastCommittedAnchor    int64
	totalCommittedFrames   int
	coldStartBarsRemaining int
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

	for pIdx, p := range phases {
		histories[pIdx] = make([]*SymbolHistory, len(p.Symbols))
		for sIdx, s := range p.Symbols {
			histories[pIdx][sIdx] = &SymbolHistory{}
			symbolPhaseRefs[s] = append(symbolPhaseRefs[s], phaseRef{
				phaseIdx:       pIdx,
				symbolPhaseIdx: sIdx,
			})
		}
	}

	return &Projector{
		producer:        producer,
		cadenceNS:       cadenceNS,
		phases:          phases,
		uniqueSymbols:   uniqueSymbols,
		symToUnique:     symToUnique,
		histories:       histories,
		symbolPhaseRefs: symbolPhaseRefs,
		phaseNextAnchor: make([]int64, len(phases)),
		phaseSlots:      make([]int, len(phases)),
		featBuffer:      make([]float64, 5),
	}
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


// IngestTick processes a market tick and commits completed 1Hz frames when time boundaries are crossed.
func (p *Projector) IngestTick(tick feed.Tick) error {
	ts := tick.SIPTimestampNS

	// 1. Initialize phase start anchors on first tick
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

	// 2. Check if any phase has completed its window
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

	for sIdx := range pCfg.Symbols {
		hist := p.histories[pIdx][sIdx]
		r1, r5, r15, v1, sp := hist.CloseBar(slot)

		p.featBuffer[0] = r1
		p.featBuffer[1] = r5
		p.featBuffer[2] = r15
		p.featBuffer[3] = v1
		p.featBuffer[4] = sp

		if err := p.producer.CommitSymbolMetrics(pIdx, sIdx, anchorNS, p.featBuffer); err != nil {
			return fmt.Errorf("commit metrics p%d s%d @ %d: %w", pIdx, sIdx, anchorNS, err)
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
	p.lastCommittedAnchor = anchorNS
	p.totalCommittedFrames++

	if pIdx == len(p.phases)-1 && p.coldStartBarsRemaining > 0 {
		p.coldStartBarsRemaining--
	}
	return nil
}

func (p *Projector) updateSnapshot(uIdx int, tick feed.Tick) {
	snap := &shm.SymbolSnapshot{
		SIPTimestampNS:  tick.SIPTimestampNS,
		RecvTimestampNS: tick.SIPTimestampNS,
	}

	if tick.Type == feed.TickTrade {
		snap.LastTradePx = tick.Price
		snap.LastTradeSz = tick.Size
	} else {
		snap.BidPx = tick.BidPx
		snap.AskPx = tick.AskPx
		snap.BidSz = tick.BidSz
		snap.AskSz = tick.AskSz
		if tick.BidPx > 0 && tick.AskPx > 0 {
			snap.Midprice = (tick.BidPx + tick.AskPx) / 2.0
			snap.Spread = tick.AskPx - tick.BidPx
		}
	}

	p.producer.WriteSnapshot(uIdx, snap)
}

// Flush closes remaining bars up to targetEndNS.
func (p *Projector) Flush(targetEndNS int64) error {
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
