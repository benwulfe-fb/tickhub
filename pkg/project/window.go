package project

import (
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

const maxHistoryBars = 32

type ccmQuoteEntry struct {
	sipNS  int64
	partNS int64
	bidPx  float64
	askPx  float64
	valid  bool
}

// SymbolHistory tracks past closed bar metrics for multi-second rolling returns
// and full 44-slot CCM 1Hz projection primitives.
type SymbolHistory struct {
	lastPrice   float64
	prices      [maxHistoryBars]float64
	volumes     [maxHistoryBars]float64
	lastBid     float64
	lastAsk     float64
	currentVol  float64
	spreadBps   float64
	hasTraded   bool
	initialized bool

	// CCM 44-slot cumulative session state
	ccmCumQuotes   int64
	ccmCumTrades   int64
	hasAnyQuote    bool
	firstQuote     ccmQuoteEntry
	lastQuoteSIPNS int64
	ccmLastBidPx   float64
	ccmLastAskPx   float64
	ccmLastBidSz   float64
	ccmLastAskSz   float64
	ccmLastTradePx float64
	hasPrevTrade   bool
	prevTradePx    float64
	lastIsBuy      bool
	venueLatest    []ccmQuoteEntry
	venueHasBid    []bool
	venueLastBidPx []float64
	venueHasAsk    []bool
	venueLastAskPx []float64
	quoteBuf       []ccmQuoteEntry
	quoteGPtr      int

	// CCM 44-slot per-1Hz-bar accumulators (reset at each integer second CloseCCMBar)
	ccmBuyVol       float64
	ccmSellVol      float64
	ccmBuyNotional  float64
	ccmSellNotional float64
	ccmBuyTradeCnt  float64
	ccmSellTradeCnt float64
	ccmBuyMidDelta  float64
	ccmSellMidDelta float64
	ccmBidQuoteCnt  float64
	ccmAskQuoteCnt  float64
	ccmDarkBidNotl  float64
	ccmDarkAskNotl  float64
	ccmHighPx       float64
	ccmLowPx        float64
	ccmHighBidPx    float64
	ccmLowBidPx     float64
	ccmHighAskPx    float64
	ccmLowAskPx     float64
	ccmHighPxNS     int64
	ccmLowPxNS      int64
	ccmHighAskPxNS  int64
	ccmLowAskPxNS   int64
	barTradePxs     []float64
	barTradeSzs     []float64

	// Duplicate-NBBO restatement suppression state (ccm/live/massive_feed.py:510-528)
	hasDedupBaseline   bool
	dedupBidPx         float64
	dedupAskPx         float64
	dedupBidSz         float64
	dedupAskSz         float64
	dedupDroppedQuotes uint64
}

func (h *SymbolHistory) recalcSpread() {
	px := h.lastPrice
	if px <= 0 {
		px = 100.0
	}
	if h.lastBid > 0 && h.lastAsk > 0 && px > 0 {
		h.spreadBps = ((h.lastAsk - h.lastBid) / px) * 10000.0
	} else {
		h.spreadBps = 1.0 // default minimum 1 bps
	}
}

func (h *SymbolHistory) UpdateQuote(bid, ask float64) {
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
	h.recalcSpread()
}

func (h *SymbolHistory) UpdateTrade(price, size float64) {
	if price > 0 {
		h.lastPrice = price
		h.hasTraded = true
		h.recalcSpread()
	}
	if size > 0 {
		h.currentVol += size
	}
}

// CloseBar records the bar close at slot index, computes features, and resets current volume.
func (h *SymbolHistory) CloseBar(slot int) (logRet1s, logRet5s, logRet15s, vol1s, spreadBps float64) {
	currSlot := slot % maxHistoryBars
	px := h.lastPrice
	if px <= 0 {
		// Default fallback if uninitialized
		px = 100.0
		h.lastPrice = px
		h.recalcSpread()
	}

	h.prices[currSlot] = px
	h.volumes[currSlot] = h.currentVol
	vol1s = h.currentVol
	h.currentVol = 0 // Reset volume for next 1-second interval

	// Spread in basis points
	spreadBps = h.spreadBps
	if spreadBps <= 0 {
		h.recalcSpread()
		spreadBps = h.spreadBps
	}

	if !h.initialized {
		// Fill backward history with current price on first bar
		for i := 0; i < maxHistoryBars; i++ {
			h.prices[i] = px
		}
		h.initialized = true
		return 0, 0, 0, vol1s, spreadBps
	}

	// 1-second log return: ln(P_T / P_{T-1})
	prev1Slot := (slot - 1 + maxHistoryBars) % maxHistoryBars
	p1 := h.prices[prev1Slot]
	if p1 > 0 {
		if px == p1 {
			logRet1s = 0.0
		} else {
			logRet1s = math.Log(px / p1)
		}
	}

	// 5-second log return: ln(P_T / P_{T-5})
	prev5Slot := (slot - 5 + maxHistoryBars) % maxHistoryBars
	p5 := h.prices[prev5Slot]
	if p5 > 0 {
		if px == p5 {
			logRet5s = 0.0
		} else {
			logRet5s = math.Log(px / p5)
		}
	}

	// 15-second log return: ln(P_T / P_{T-15})
	prev15Slot := (slot - 15 + maxHistoryBars) % maxHistoryBars
	p15 := h.prices[prev15Slot]
	if p15 > 0 {
		if px == p15 {
			logRet15s = 0.0
		} else {
			logRet15s = math.Log(px / p15)
		}
	}

	return logRet1s, logRet5s, logRet15s, vol1s, spreadBps
}

// SeedFromBars reconstructs past bar prices backwards from chronological past bars and current price.
func (h *SymbolHistory) SeedFromBars(lastPx, lastBid, lastAsk float64, bars []shm.FrameBar, cadenceNS int64) {
	if len(bars) == 0 {
		return
	}
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}
	if lastPx <= 0 {
		lastPx = 100.0
	}
	h.lastPrice = lastPx
	if lastBid > 0 {
		h.lastBid = lastBid
	}
	if lastAsk > 0 {
		h.lastAsk = lastAsk
	}
	h.hasTraded = true
	h.initialized = true
	h.recalcSpread()

	px := lastPx
	for i := len(bars) - 1; i >= 0; i-- {
		slot := int(bars[i].AnchorNS / cadenceNS)
		s := (slot%maxHistoryBars + maxHistoryBars) % maxHistoryBars
		h.prices[s] = px
		if len(bars[i].Features) > 3 {
			h.volumes[s] = bars[i].Features[3]
		}
		if len(bars[i].Features) > 0 {
			r1 := bars[i].Features[0]
			px = px * math.Exp(-r1)
		}
	}
}

// SeedPrevailingPrice initializes baseline price and quote state without stitching past return history.
func (h *SymbolHistory) SeedPrevailingPrice(lastPx, lastBid, lastAsk float64) {
	if lastPx <= 0 {
		if lastBid > 0 && lastAsk > 0 {
			lastPx = (lastBid + lastAsk) / 2.0
		} else if lastBid > 0 {
			lastPx = lastBid
		} else if lastAsk > 0 {
			lastPx = lastAsk
		} else {
			lastPx = 100.0
		}
	}
	h.lastPrice = lastPx
	if lastBid > 0 {
		h.lastBid = lastBid
	}
	if lastAsk > 0 {
		h.lastAsk = lastAsk
	}
	h.hasTraded = true
	h.initialized = true
	h.recalcSpread()
	for i := 0; i < maxHistoryBars; i++ {
		h.prices[i] = lastPx
	}
}

const (
	maxMicroQuotes = 4096
	maxMicroTrades = 1024
)

type microQuote struct {
	tsNS    int64
	partNS  int64
	bid     float64
	ask     float64
	mid     float64
	bidExch int16
}

type microTrade struct {
	tsNS  int64
	price float64
	size  float64
	isBuy bool
}

// MicroTracker maintains exact non-discretized point-in-time quote and trade history
// per symbol to populate shm.SymbolMicroSnapshot with zero discretization error.
type MicroTracker struct {
	quotes       [maxMicroQuotes]microQuote
	qHead        int
	qCount       int
	trades       [maxMicroTrades]microTrade
	tHead        int
	tCount       int
	venueLatest  [256]ccmQuoteEntry
	seedBid      float64
	seedAsk      float64
	seedMid      float64
	maxTS        int64
	prevTradePx  float64
	hasPrevTrade bool
	lastIsBuy    bool
	initialized  bool
}

// SeedQuote initializes baseline bid/ask/mid across the tracker.
func (m *MicroTracker) SeedQuote(bid, ask float64) {
	if bid > 0 {
		m.seedBid = bid
	}
	if ask > 0 {
		m.seedAsk = ask
	}
	if m.seedBid > 0 && m.seedAsk > 0 {
		m.seedMid = 0.5 * (m.seedBid + m.seedAsk)
	}
	m.qHead = 0
	m.qCount = 0
	m.tHead = 0
	m.tCount = 0
	for i := range m.venueLatest {
		m.venueLatest[i] = ccmQuoteEntry{}
	}
	m.hasPrevTrade = false
	m.prevTradePx = 0.0
	m.lastIsBuy = true
	m.initialized = true
}

// OnQuote records an incoming quote using tsNS for both SIP and participant timestamps.
func (m *MicroTracker) OnQuote(tsNS int64, bid, ask float64) {
	m.OnQuoteWithExch(tsNS, tsNS, bid, ask, 0)
}

// OnQuoteWithExch records an incoming quote with full exchange and participant metadata.
func (m *MicroTracker) OnQuoteWithExch(tsSIP, tsPart int64, bid, ask float64, bidExch int16) {
	if bid <= 0 || ask <= 0 {
		return
	}
	if tsPart <= 0 {
		tsPart = tsSIP
	}
	mid := 0.5 * (bid + ask)
	m.seedBid = bid
	m.seedAsk = ask
	m.seedMid = mid
	if tsSIP > m.maxTS {
		m.maxTS = tsSIP
	}
	if !m.initialized {
		m.lastIsBuy = true
		m.initialized = true
	}

	// Track venueLatest for hybrid Lee-Ready classification (matching SymbolHistory.recordCCMQuoteState)
	if bidExch >= 0 && int(bidExch) < len(m.venueLatest) {
		m.venueLatest[bidExch] = ccmQuoteEntry{
			sipNS:  tsSIP,
			partNS: tsPart,
			bidPx:  bid,
			askPx:  ask,
			valid:  true,
		}
	}

	// Deduplicate consecutive identical NBBO quotes
	if m.qCount > 0 {
		tailIdx := (m.qHead + m.qCount - 1) & (maxMicroQuotes - 1)
		lastQ := &m.quotes[tailIdx]
		if lastQ.bid == bid && lastQ.ask == ask && lastQ.bidExch == bidExch && tsSIP >= lastQ.tsNS {
			return
		}
	}

	q := microQuote{
		tsNS:    tsSIP,
		partNS:  tsPart,
		bid:     bid,
		ask:     ask,
		mid:     mid,
		bidExch: bidExch,
	}

	if m.qCount < maxMicroQuotes {
		tailIdx := (m.qHead + m.qCount) & (maxMicroQuotes - 1)
		m.quotes[tailIdx] = q
		m.qCount++
	} else {
		m.quotes[m.qHead] = q
		m.qHead = (m.qHead + 1) & (maxMicroQuotes - 1)
	}

	// Prune quotes older than maxTS - 35s from head
	cutoffNS := m.maxTS - 35_000_000_000
	for m.qCount > 1 && m.quotes[m.qHead].tsNS < cutoffNS {
		m.qHead = (m.qHead + 1) & (maxMicroQuotes - 1)
		m.qCount--
	}
}

func (m *MicroTracker) classifyTrade(tsSIP, tsPart int64, price float64, tradeExch int16) bool {
	if tsPart <= 0 {
		tsPart = tsSIP
	}
	targetSIP := tsSIP - 1_000_000

	var candA ccmQuoteEntry
	if m.qCount == 0 {
		candA = ccmQuoteEntry{sipNS: 0, bidPx: m.seedBid, askPx: m.seedAsk, valid: (m.seedBid > 0 && m.seedAsk > 0)}
	} else {
		firstQ := &m.quotes[m.qHead]
		if targetSIP < firstQ.tsNS {
			candA = ccmQuoteEntry{sipNS: firstQ.tsNS, partNS: firstQ.partNS, bidPx: firstQ.bid, askPx: firstQ.ask, valid: true}
		} else {
			lastIdx := (m.qHead + m.qCount - 1) & (maxMicroQuotes - 1)
			lastQ := &m.quotes[lastIdx]
			if targetSIP >= lastQ.tsNS {
				candA = ccmQuoteEntry{sipNS: lastQ.tsNS, partNS: lastQ.partNS, bidPx: lastQ.bid, askPx: lastQ.ask, valid: true}
			} else {
				i := sort.Search(m.qCount, func(k int) bool {
					idx := (m.qHead + k) & (maxMicroQuotes - 1)
					return m.quotes[idx].tsNS > targetSIP
				})
				if i > 0 {
					i--
				}
				idx := (m.qHead + i) & (maxMicroQuotes - 1)
				q := &m.quotes[idx]
				candA = ccmQuoteEntry{sipNS: q.tsNS, partNS: q.partNS, bidPx: q.bid, askPx: q.ask, valid: true}
			}
		}
	}

	qChoice := candA
	if tradeExch >= 0 && int(tradeExch) < len(m.venueLatest) {
		candB := m.venueLatest[tradeExch]
		if candB.valid && candB.sipNS <= tsSIP && candB.partNS <= tsPart && candB.sipNS > candA.sipNS {
			qChoice = candB
		}
	}
	if qChoice.sipNS > tsSIP {
		qChoice = candA
	}

	var midPx float64 = math.NaN()
	if qChoice.valid && qChoice.bidPx > 0 && qChoice.askPx > 0 {
		midPx = 0.5 * (qChoice.bidPx + qChoice.askPx)
	}

	isBuy := m.lastIsBuy
	if !math.IsNaN(midPx) {
		if price > midPx {
			isBuy = true
		} else if price < midPx {
			isBuy = false
		} else if m.hasPrevTrade {
			if price > m.prevTradePx {
				isBuy = true
			} else if price < m.prevTradePx {
				isBuy = false
			}
		}
	} else if m.hasPrevTrade {
		if price > m.prevTradePx {
			isBuy = true
		} else if price < m.prevTradePx {
			isBuy = false
		}
	}

	m.prevTradePx = price
	m.hasPrevTrade = true
	m.lastIsBuy = isBuy
	return isBuy
}

// OnTrade records an incoming trade using tsNS for both SIP and participant timestamps.
func (m *MicroTracker) OnTrade(tsNS int64, price, size float64) {
	m.OnTradeWithExch(tsNS, tsNS, price, size, 0)
}

// OnTradeWithExch records an incoming trade with exchange metadata and applies project_to_1hz Lee-Ready classification.
func (m *MicroTracker) OnTradeWithExch(tsSIP, tsPart int64, price, size float64, tradeExch int16) {
	if price <= 0 || size <= 0 {
		return
	}
	if tsSIP > m.maxTS {
		m.maxTS = tsSIP
	}
	if !m.initialized {
		m.lastIsBuy = true
		m.initialized = true
	}

	isBuy := m.classifyTrade(tsSIP, tsPart, price, tradeExch)

	trade := microTrade{
		tsNS:  tsSIP,
		price: price,
		size:  size,
		isBuy: isBuy,
	}

	if m.tCount < maxMicroTrades {
		tailIdx := (m.tHead + m.tCount) & (maxMicroTrades - 1)
		m.trades[tailIdx] = trade
		m.tCount++
	} else {
		m.trades[m.tHead] = trade
		m.tHead = (m.tHead + 1) & (maxMicroTrades - 1)
	}

	// Prune trades older than maxTS - 10s from head
	cutoffNS := m.maxTS - 10_000_000_000
	for m.tCount > 0 && m.trades[m.tHead].tsNS < cutoffNS {
		m.tHead = (m.tHead + 1) & (maxMicroTrades - 1)
		m.tCount--
	}
}

// quoteAt binary searches for the latest quote with tsNS <= targetTS.
func (m *MicroTracker) quoteAt(targetTS int64) (float64, float64, float64) {
	if m.qCount == 0 {
		return m.seedBid, m.seedAsk, m.seedMid
	}
	firstQ := &m.quotes[m.qHead]
	if targetTS < firstQ.tsNS {
		if firstQ.bid > 0 {
			return firstQ.bid, firstQ.ask, firstQ.mid
		}
		return m.seedBid, m.seedAsk, m.seedMid
	}
	lastIdx := (m.qHead + m.qCount - 1) & (maxMicroQuotes - 1)
	lastQ := &m.quotes[lastIdx]
	if targetTS >= lastQ.tsNS {
		return lastQ.bid, lastQ.ask, lastQ.mid
	}

	i := sort.Search(m.qCount, func(k int) bool {
		idx := (m.qHead + k) & (maxMicroQuotes - 1)
		return m.quotes[idx].tsNS > targetTS
	})
	if i > 0 {
		i--
	}
	idx := (m.qHead + i) & (maxMicroQuotes - 1)
	return m.quotes[idx].bid, m.quotes[idx].ask, m.quotes[idx].mid
}

// ComputeSnapshot populates out with rolling lookback mids/bids/asks, 5s quote volatility,
// 3s trade count, and 5s trade volume/VWAP/imbalance as of tsNS.
func (m *MicroTracker) ComputeSnapshot(tsNS int64, out *shm.SymbolMicroSnapshot) {
	if out == nil {
		return
	}
	if tsNS > m.maxTS {
		m.maxTS = tsNS
	}
	nowTS := m.maxTS
	if nowTS <= 0 {
		return
	}
	out.SIPTimestampNS = nowTS

	if !m.initialized && m.qCount == 0 {
		out.Mid1sAgo = m.seedMid
		out.Mid2sAgo = m.seedMid
		out.Mid3sAgo = m.seedMid
		out.Mid5sAgo = m.seedMid
		out.Mid10sAgo = m.seedMid
		out.Mid30sAgo = m.seedMid
		out.Bid5sAgo = m.seedBid
		out.Ask5sAgo = m.seedAsk
		return
	}

	_, _, out.Mid1sAgo = m.quoteAt(nowTS - 1_000_000_000)
	_, _, out.Mid2sAgo = m.quoteAt(nowTS - 2_000_000_000)
	_, _, out.Mid3sAgo = m.quoteAt(nowTS - 3_000_000_000)
	out.Bid5sAgo, out.Ask5sAgo, out.Mid5sAgo = m.quoteAt(nowTS - 5_000_000_000)
	_, _, out.Mid10sAgo = m.quoteAt(nowTS - 10_000_000_000)
	_, _, out.Mid30sAgo = m.quoteAt(nowTS - 30_000_000_000)

	// 5s quote volatility (std of 20 250ms mid returns in bps across m.quoteAt(nowTS - k*250_000_000))
	var sumR, sumR2 float64
	const nQ = 20
	for k := int64(nQ); k >= 1; k-- {
		_, _, m0 := m.quoteAt(nowTS - k*250_000_000)
		_, _, m1 := m.quoteAt(nowTS - (k-1)*250_000_000)
		var r float64
		if m0 > 0 && m1 > 0 {
			r = ((m1 - m0) / m0) * 10000.0
		}
		sumR += r
		sumR2 += r * r
	}
	meanR := sumR / float64(nQ)
	varR := (sumR2 / float64(nQ)) - (meanR * meanR)
	if varR > 0 {
		out.QuoteVol5sBps = math.Sqrt(varR)
	} else {
		out.QuoteVol5sBps = 0.0
	}

	// 3s trade count (t.tsNS > nowTS - 3s && t.tsNS <= nowTS)
	// 5s trade aggregates (t.tsNS > nowTS - 5s && t.tsNS <= nowTS)
	t3sCutoff := nowTS - 3_000_000_000
	t5sCutoff := nowTS - 5_000_000_000

	var cnt3s uint32
	var vol5s, notl5s, buyVol5s, sellVol5s float64

	for k := m.tCount - 1; k >= 0; k-- {
		idx := (m.tHead + k) & (maxMicroTrades - 1)
		t := &m.trades[idx]
		if t.tsNS <= t5sCutoff {
			break
		}
		if t.tsNS > nowTS {
			continue
		}
		vol5s += t.size
		notl5s += t.price * t.size
		if t.isBuy {
			buyVol5s += t.size
		} else {
			sellVol5s += t.size
		}
		if t.tsNS > t3sCutoff {
			cnt3s++
		}
	}

	out.TradeCount3s = cnt3s
	out.TradeVol5s = vol5s
	if vol5s > 0 {
		out.VWAP5s = notl5s / vol5s
		denom := buyVol5s + sellVol5s
		if denom > 0 {
			out.TradeImbalance5s = (buyVol5s - sellVol5s) / denom
		} else {
			out.TradeImbalance5s = 0.0
		}
	} else {
		out.VWAP5s = 0.0
		out.TradeImbalance5s = 0.0
	}
}

// InitCCM44 initializes the SymbolHistory state for 44-slot CCM 1Hz projection.
func (h *SymbolHistory) InitCCM44() {
	nan := math.NaN()
	h.ccmLastBidPx = nan
	h.ccmLastAskPx = nan
	h.ccmLastBidSz = nan
	h.ccmLastAskSz = nan
	h.ccmLastTradePx = nan
	h.lastIsBuy = true
	h.venueLatest = make([]ccmQuoteEntry, 256)
	h.venueHasBid = make([]bool, 256)
	h.venueLastBidPx = make([]float64, 256)
	h.venueHasAsk = make([]bool, 256)
	h.venueLastAskPx = make([]float64, 256)
	h.quoteBuf = make([]ccmQuoteEntry, 0, 512)
	h.barTradePxs = make([]float64, 0, 256)
	h.barTradeSzs = make([]float64, 0, 256)
	h.hasDedupBaseline = false
	h.dedupBidPx = 0.0
	h.dedupAskPx = 0.0
	h.dedupBidSz = 0.0
	h.dedupAskSz = 0.0
	h.dedupDroppedQuotes = 0
	h.resetCCMBarAccumulators()
}

func (h *SymbolHistory) resetCCMBarAccumulators() {
	nan := math.NaN()
	h.ccmBuyVol = 0.0
	h.ccmSellVol = 0.0
	h.ccmBuyNotional = 0.0
	h.ccmSellNotional = 0.0
	h.ccmBuyTradeCnt = 0.0
	h.ccmSellTradeCnt = 0.0
	h.ccmBuyMidDelta = 0.0
	h.ccmSellMidDelta = 0.0
	h.ccmBidQuoteCnt = 0.0
	h.ccmAskQuoteCnt = 0.0
	h.ccmDarkBidNotl = 0.0
	h.ccmDarkAskNotl = 0.0
	h.ccmHighPx = nan
	h.ccmLowPx = nan
	h.ccmHighBidPx = nan
	h.ccmLowBidPx = nan
	h.ccmHighAskPx = nan
	h.ccmLowAskPx = nan
	h.ccmHighPxNS = 0
	h.ccmLowPxNS = 0
	h.ccmHighAskPxNS = 0
	h.ccmLowAskPxNS = 0
	h.barTradePxs = h.barTradePxs[:0]
	h.barTradeSzs = h.barTradeSzs[:0]
}

func (h *SymbolHistory) recordCCMQuoteState(tsSIP, tsPart int64, bidPx, askPx, bidSz, askSz float64, bidExch, askExch int16) {
	if tsPart <= 0 {
		tsPart = tsSIP
	}
	h.ccmCumQuotes++
	h.lastQuoteSIPNS = tsSIP
	h.ccmLastBidPx = bidPx
	h.ccmLastAskPx = askPx
	h.ccmLastBidSz = bidSz
	h.ccmLastAskSz = askSz

	qe := ccmQuoteEntry{
		sipNS:  tsSIP,
		partNS: tsPart,
		bidPx:  bidPx,
		askPx:  askPx,
		valid:  true,
	}
	if !h.hasAnyQuote {
		h.hasAnyQuote = true
		h.firstQuote = qe
	}
	if bidExch >= 0 && int(bidExch) < len(h.venueLatest) {
		h.venueLatest[bidExch] = qe
	}
	h.quoteBuf = append(h.quoteBuf, qe)
	// Compact old quotes > 100ms behind the latest quote once buffer exceeds 8192 entries
	if len(h.quoteBuf) > 8192 && h.quoteGPtr > 4096 {
		keepFrom := h.quoteGPtr - 1
		cutoffNS := tsSIP - 100_000_000
		for keepFrom > 0 && h.quoteBuf[keepFrom].sipNS >= cutoffNS {
			keepFrom--
		}
		if keepFrom > 2048 {
			nRem := copy(h.quoteBuf, h.quoteBuf[keepFrom:])
			h.quoteBuf = h.quoteBuf[:nRem]
			h.quoteGPtr -= keepFrom
		}
	}
}

// AcceptQuote evaluates wire-level duplicate NBBO restatement suppression per
// ccm/live/massive_feed.py:510-528 and ccm_train/tape_source.py:71-105.
// Invalid-priced quotes (!(bidPx > 0.0 && askPx > 0.0)) return false without updating the baseline.
// Duplicate quotes with identical prices and sizes return false and increment dedupDroppedQuotes.
// Quotes with NaN sizes never compare equal (NaN == NaN is false in Go IEEE 754) and are accepted.
// Distinct valid quotes update the baseline and return true.
func (h *SymbolHistory) AcceptQuote(bidPx, askPx, bidSz, askSz float64) bool {
	if !(bidPx > 0.0 && askPx > 0.0) {
		return false
	}
	if h.hasDedupBaseline && h.dedupBidPx == bidPx && h.dedupAskPx == askPx && h.dedupBidSz == bidSz && h.dedupAskSz == askSz {
		h.dedupDroppedQuotes++
		return false
	}
	h.hasDedupBaseline = true
	h.dedupBidPx = bidPx
	h.dedupAskPx = askPx
	h.dedupBidSz = bidSz
	h.dedupAskSz = askSz
	return true
}

// DedupDroppedQuotes returns the total count of suppressed duplicate NBBO quote restatements.
func (h *SymbolHistory) DedupDroppedQuotes() uint64 {
	return h.dedupDroppedQuotes
}

// IngestPreWindowCCMQuote updates cumulative quote state prior to window start without bar accumulation.
// Note: venueHasBid/venueHasAsk are intentionally updated only in IngestCCMQuote (starting at s_start)
// to match ccm's _project_quotes_1hz_njit, which initializes per-venue BBO state at the start of [q_start_idx, q_end_idx).
func (h *SymbolHistory) IngestPreWindowCCMQuote(tsSIP, tsPart int64, bidPx, askPx, bidSz, askSz float64, bidExch, askExch int16) {
	if !(bidPx > 0.0 && askPx > 0.0) {
		return
	}
	h.recordCCMQuoteState(tsSIP, tsPart, bidPx, askPx, bidSz, askSz, bidExch, askExch)
}

// IngestCCMQuote updates cumulative and current-bar quote state.
func (h *SymbolHistory) IngestCCMQuote(tsSIP, tsPart int64, bidPx, askPx, bidSz, askSz float64, bidExch, askExch int16) {
	if !(bidPx > 0.0 && askPx > 0.0) {
		return
	}
	h.recordCCMQuoteState(tsSIP, tsPart, bidPx, askPx, bidSz, askSz, bidExch, askExch)
	if bidExch >= 0 && int(bidExch) < len(h.venueHasBid) {
		h.venueHasBid[bidExch] = true
		h.venueLastBidPx[bidExch] = bidPx
	}
	if askExch >= 0 && int(askExch) < len(h.venueHasAsk) {
		h.venueHasAsk[askExch] = true
		h.venueLastAskPx[askExch] = askPx
	}
	h.ccmBidQuoteCnt += 1.0
	h.ccmAskQuoteCnt += 1.0
	if math.IsNaN(h.ccmHighBidPx) || bidPx > h.ccmHighBidPx {
		h.ccmHighBidPx = bidPx
	}
	if math.IsNaN(h.ccmLowBidPx) || bidPx < h.ccmLowBidPx {
		h.ccmLowBidPx = bidPx
	}
	if math.IsNaN(h.ccmHighAskPx) || askPx > h.ccmHighAskPx {
		h.ccmHighAskPx = askPx
		h.ccmHighAskPxNS = tsSIP
	}
	if math.IsNaN(h.ccmLowAskPx) || askPx < h.ccmLowAskPx {
		h.ccmLowAskPx = askPx
		h.ccmLowAskPxNS = tsSIP
	}
}

func (h *SymbolHistory) classifyCCMTrade(tsSIP, tsPart int64, px float64, tradeExch int16) (isBuy bool, midPx float64) {
	if tsPart <= 0 {
		tsPart = tsSIP
	}
	midPx = math.NaN()
	if h.hasAnyQuote && len(h.quoteBuf) > 0 {
		targetSIP := tsSIP - 1_000_000
		if h.quoteGPtr > 0 && h.quoteGPtr <= len(h.quoteBuf) && h.quoteBuf[h.quoteGPtr-1].sipNS > targetSIP {
			// Rare out-of-order trade SIP: binary search within retained quoteBuf
			lo, hi := 0, len(h.quoteBuf)
			for lo < hi {
				mid := (lo + hi) >> 1
				if h.quoteBuf[mid].sipNS <= targetSIP {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			h.quoteGPtr = lo
		} else {
			for h.quoteGPtr < len(h.quoteBuf) && h.quoteBuf[h.quoteGPtr].sipNS <= targetSIP {
				h.quoteGPtr++
			}
		}
		var candA ccmQuoteEntry
		if h.quoteGPtr > 0 {
			idx := h.quoteGPtr - 1
			if idx >= len(h.quoteBuf) {
				idx = len(h.quoteBuf) - 1
			}
			candA = h.quoteBuf[idx]
		} else {
			candA = h.firstQuote
		}

		qChoice := candA
		if tradeExch >= 0 && int(tradeExch) < len(h.venueLatest) {
			candB := h.venueLatest[tradeExch]
			if candB.valid && candB.partNS <= tsPart && candB.sipNS > candA.sipNS {
				qChoice = candB
			}
		}
		if qChoice.sipNS > tsSIP {
			qChoice = candA
		}
		if qChoice.valid {
			midPx = 0.5 * (qChoice.bidPx + qChoice.askPx)
		}
	}

	isBuy = h.lastIsBuy
	if px > midPx {
		isBuy = true
	} else if px < midPx {
		isBuy = false
	} else if h.hasPrevTrade {
		if px > h.prevTradePx {
			isBuy = true
		} else if px < h.prevTradePx {
			isBuy = false
		}
	}

	h.ccmCumTrades++
	h.ccmLastTradePx = px
	h.prevTradePx = px
	h.hasPrevTrade = true
	h.lastIsBuy = isBuy
	return isBuy, midPx
}

// IngestPreWindowCCMTrade advances Hybrid Lee-Ready state prior to window start without bar accumulation.
func (h *SymbolHistory) IngestPreWindowCCMTrade(tsSIP, tsPart int64, px, sz float64, tradeExch int16) {
	if !(px > 0.0) {
		return
	}
	_, _ = h.classifyCCMTrade(tsSIP, tsPart, px, tradeExch)
}

// IngestCCMTrade classifies a trade via Hybrid Lee-Ready and accumulates 1Hz bar primitives.
func (h *SymbolHistory) IngestCCMTrade(tsSIP, tsPart int64, px, sz float64, tradeExch int16, isDark bool) {
	if !(px > 0.0) {
		return
	}
	isBuy, midPx := h.classifyCCMTrade(tsSIP, tsPart, px, tradeExch)
	notional := sz * px
	if isBuy {
		h.ccmBuyVol += sz
		h.ccmBuyNotional += notional
		h.ccmBuyTradeCnt += 1.0
		if midPx > 0.0 {
			h.ccmBuyMidDelta += sz * (px - midPx)
		}
	} else {
		h.ccmSellVol += sz
		h.ccmSellNotional += notional
		h.ccmSellTradeCnt += 1.0
		if midPx > 0.0 {
			h.ccmSellMidDelta += sz * (midPx - px)
		}
	}

	if isDark {
		if isBuy {
			h.ccmDarkAskNotl += notional
		} else {
			h.ccmDarkBidNotl += notional
		}
	}

	if math.IsNaN(h.ccmHighPx) || px > h.ccmHighPx {
		h.ccmHighPx = px
		h.ccmHighPxNS = tsSIP
	}
	if math.IsNaN(h.ccmLowPx) || px < h.ccmLowPx {
		h.ccmLowPx = px
		h.ccmLowPxNS = tsSIP
	}

	h.barTradePxs = append(h.barTradePxs, px)
	h.barTradeSzs = append(h.barTradeSzs, sz)
}

// CloseCCMBar finalizes the 36 whole-second bar slots (0..35) for [barCloseNS - 1s, barCloseNS)
// and resets the per-bar accumulators.
func (h *SymbolHistory) CloseCCMBar(barCloseNS int64, out *[44]float64) {
	barStartNS := barCloseNS - 1_000_000_000
	nan := math.NaN()

	out[0] = h.ccmBuyVol
	out[1] = h.ccmSellVol
	out[2] = h.ccmBuyNotional
	out[3] = h.ccmSellNotional
	out[4] = h.ccmBuyTradeCnt
	out[5] = h.ccmSellTradeCnt
	out[6] = h.ccmBuyMidDelta
	out[7] = h.ccmSellMidDelta
	out[8] = h.ccmBidQuoteCnt
	out[9] = h.ccmAskQuoteCnt
	out[10] = h.ccmDarkBidNotl
	out[11] = h.ccmDarkAskNotl
	out[12] = h.ccmBidQuoteCnt

	if h.hasAnyQuote {
		out[13] = float64(barCloseNS-h.lastQuoteSIPNS) / 1e6
	} else {
		out[13] = 0.0
	}

	if h.hasAnyQuote && !math.IsNaN(h.ccmLastBidPx) {
		bbCnt := 0
		for v := 0; v < 256; v++ {
			if h.venueHasBid[v] && h.venueLastBidPx[v] == h.ccmLastBidPx {
				bbCnt++
			}
		}
		out[14] = float64(bbCnt)
	} else {
		out[14] = nan
	}

	if h.hasAnyQuote && !math.IsNaN(h.ccmLastAskPx) {
		boCnt := 0
		for v := 0; v < 256; v++ {
			if h.venueHasAsk[v] && h.venueLastAskPx[v] == h.ccmLastAskPx {
				boCnt++
			}
		}
		out[15] = float64(boCnt)
	} else {
		out[15] = nan
	}

	out[16] = h.ccmHighPx
	out[17] = h.ccmLowPx
	out[18] = h.ccmHighBidPx
	out[19] = h.ccmLowBidPx
	out[20] = h.ccmHighAskPx
	out[21] = h.ccmLowAskPx
	out[22] = h.ccmLastBidPx
	out[23] = h.ccmLastAskPx
	out[24] = h.ccmLastBidSz
	out[25] = h.ccmLastAskSz
	out[26] = h.ccmLastTradePx
	if h.hasAnyQuote {
		out[27] = float64(h.lastQuoteSIPNS)
	} else {
		out[27] = nan
	}

	// Slots 28..29: trade_vol_std_px and trade_vol_skew (NaN on 0-trade bars so reader forward-fills)
	nTrades := len(h.barTradePxs)
	if nTrades == 0 {
		out[28] = nan
		out[29] = nan
	} else {
		var sumW, sumWX float64
		for i := 0; i < nTrades; i++ {
			sz := h.barTradeSzs[i]
			px := h.barTradePxs[i]
			sumW += sz
			sumWX += sz * px
		}
		if sumW > 0.0 {
			meanPx := sumWX / sumW
			var sumWDiff2 float64
			for i := 0; i < nTrades; i++ {
				diff := h.barTradePxs[i] - meanPx
				sumWDiff2 += h.barTradeSzs[i] * diff * diff
			}
			varPx := sumWDiff2 / sumW
			stdPx := math.Sqrt(varPx)
			out[28] = stdPx
			if stdPx > 1e-9 {
				var sumWDiff3 float64
				for i := 0; i < nTrades; i++ {
					diff := h.barTradePxs[i] - meanPx
					sumWDiff3 += h.barTradeSzs[i] * diff * diff * diff
				}
				out[29] = sumWDiff3 / (sumW * stdPx * stdPx * stdPx)
			} else {
				out[29] = 0.0
			}
		} else {
			out[28] = 0.0
			out[29] = 0.0
		}
	}

	// Slots 30..33: nanosecond offsets within [barStartNS, barCloseNS), or -1.0 when unset (0)
	if h.ccmHighPxNS > 0 {
		out[30] = float64(h.ccmHighPxNS - barStartNS)
	} else {
		out[30] = -1.0
	}
	if h.ccmLowPxNS > 0 {
		out[31] = float64(h.ccmLowPxNS - barStartNS)
	} else {
		out[31] = -1.0
	}
	if h.ccmHighAskPxNS > 0 {
		out[32] = float64(h.ccmHighAskPxNS - barStartNS)
	} else {
		out[32] = -1.0
	}
	if h.ccmLowAskPxNS > 0 {
		out[33] = float64(h.ccmLowAskPxNS - barStartNS)
	} else {
		out[33] = -1.0
	}

	// Slots 34..35: cumulative quote_idx and trade_idx at barCloseNS
	out[34] = float64(h.ccmCumQuotes)
	out[35] = float64(h.ccmCumTrades)

	h.resetCCMBarAccumulators()
}

// WriteCCMTerminalState populates slots 36..43 with the symbol's point-in-time state as of phase_anchor_ns.
func (h *SymbolHistory) WriteCCMTerminalState(out *[44]float64) {
	out[36] = h.ccmLastBidPx
	out[37] = h.ccmLastAskPx
	out[38] = h.ccmLastBidSz
	out[39] = h.ccmLastAskSz
	out[40] = h.ccmLastTradePx
	if h.hasAnyQuote {
		out[41] = float64(h.lastQuoteSIPNS)
	} else {
		out[41] = math.NaN()
	}
	out[42] = float64(h.ccmCumQuotes)
	out[43] = float64(h.ccmCumTrades)
}

const (
	bucket250msNS   = 250_000_000
	baseline15mBars = 3600
	atMidEps250ms   = 1e-6
)

var nyLoc250ms *time.Location

func init() {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		loc = time.FixedZone("EDT", -4*3600)
	}
	nyLoc250ms = loc
}

// Tracker250ms tracks 250ms buckets, causal forward-filled mid ticks, and 15-minute (3600-bucket)
// trailing baseline sums per symbol to populate shm.SymbolRing250ms and shm.SymbolMicroSnapshot.
type Tracker250ms struct {
	openNS        int64
	openSet       bool
	nextCol       int64
	lastClosedCol int64

	hasValidQuote bool
	lastBid       float64
	lastAsk       float64
	lastBidTicks  int32
	lastAskTicks  int32
	firstValidCol int64
	firstValidBid int32
	firstValidAsk int32
	lastMidTicks  float64
	hasValidMid   bool

	curLitBuyVol  float64
	curLitSellVol float64

	csNB     float64
	csNS     float64
	csQB     float64
	csQS     float64
	csNBRing [baseline15mBars + 1]float64
	csNSRing [baseline15mBars + 1]float64
	csQBRing [baseline15mBars + 1]float64
	csQSRing [baseline15mBars + 1]float64

	lastCnt15m float64
	lastQ15m   float64
}

// Init initializes Tracker250ms state and resets the SHM SymbolRing250ms header if present.
func (t *Tracker250ms) Init(ring *shm.SymbolRing250ms) {
	t.openNS = 0
	t.openSet = false
	t.nextCol = 0
	t.lastClosedCol = -1
	t.hasValidQuote = false
	t.lastBid = 0
	t.lastAsk = 0
	t.lastBidTicks = 0
	t.lastAskTicks = 0
	t.firstValidCol = -1
	t.firstValidBid = 0
	t.firstValidAsk = 0
	t.lastMidTicks = 10000.0
	t.hasValidMid = false
	t.curLitBuyVol = 0.0
	t.curLitSellVol = 0.0
	t.csNB = 0.0
	t.csNS = 0.0
	t.csQB = 0.0
	t.csQS = 0.0
	t.lastCnt15m = 1.0
	t.lastQ15m = 1.0
	if ring != nil {
		seq := atomic.AddUint64(&ring.SeqLockSeq, 1)
		ring.OpenNS = 0
		ring.LastClosedCol = -1
		ring.FirstValidCol = -1
		ring.FirstValidBid = 0
		ring.FirstValidAsk = 0
		ring.Trailing15mCnt = 1.0
		ring.Trailing15mQ = 1.0
		ring.LiveBidTicks = 0
		ring.LiveAskTicks = 0
		for k := range ring.Bars {
			ring.Bars[k] = shm.Bar250ms{BucketCol: -1}
		}
		atomic.StoreUint64(&ring.SeqLockSeq, seq+1)
	}
}

func (t *Tracker250ms) ensureOpenNS(ts int64, ring *shm.SymbolRing250ms) {
	if t.openSet || ts <= 0 {
		return
	}
	if ts < 1_000_000_000_000_000_000 {
		t.openNS = 0
	} else {
		tm := time.Unix(0, ts).In(nyLoc250ms)
		rthOpen := time.Date(tm.Year(), tm.Month(), tm.Day(), 9, 30, 0, 0, nyLoc250ms).UnixNano()
		rthClose := rthOpen + 23400*1_000_000_000
		if ts <= rthClose {
			t.openNS = rthOpen
		} else {
			t.openNS = (ts / 1_000_000_000) * 1_000_000_000
		}
	}
	t.openSet = true
}

// LastClosedCol returns the index of the latest closed 250ms bucket (-1 if none).
func (t *Tracker250ms) LastClosedCol() int32 {
	return int32(t.lastClosedCol)
}

// Trailing15mCnt returns the latest trailing 15-minute (3600-bucket) trade-count baseline sum.
func (t *Tracker250ms) Trailing15mCnt() float32 {
	return float32(t.lastCnt15m)
}

// Trailing15mQ returns the latest trailing 15-minute (3600-bucket) sqrt-volume baseline sum.
func (t *Tracker250ms) Trailing15mQ() float32 {
	return float32(t.lastQ15m)
}

// AdvanceTo closes all 250ms buckets b with openNS + (b + 1) * 250_000_000 <= ts.
func (t *Tracker250ms) AdvanceTo(ts int64, ring *shm.SymbolRing250ms) {
	if ts <= 0 {
		return
	}
	if !t.openSet {
		t.ensureOpenNS(ts, ring)
	}
	if ts < t.openNS+(t.nextCol+1)*bucket250msNS {
		return
	}
	targetCol := (ts-t.openNS)/bucket250msNS - 1
	if targetCol < t.nextCol {
		return
	}
	if t.lastClosedCol < 0 && targetCol > shm.MaxRing250msBars {
		t.nextCol = targetCol - shm.MaxRing250msBars
	} else if targetCol-t.nextCol > 100000 {
		t.nextCol = targetCol - baseline15mBars
	}

	var seq uint64
	if ring != nil {
		seq = atomic.AddUint64(&ring.SeqLockSeq, 1)
	}

	for b := t.nextCol; b <= targetCol; b++ {
		bidTicks := t.lastBidTicks
		askTicks := t.lastAskTicks
		vb := t.curLitBuyVol
		vs := t.curLitSellVol
		t.curLitBuyVol = 0.0
		t.curLitSellVol = 0.0

		if t.hasValidQuote && t.firstValidCol < 0 {
			t.firstValidCol = b
			t.firstValidBid = bidTicks
			t.firstValidAsk = askTicks
		}

		if bidTicks > 0 && askTicks > bidTicks {
			t.hasValidMid = true
			t.lastMidTicks = 0.5 * float64(bidTicks+askTicks)
		} else if !t.hasValidMid {
			t.lastMidTicks = 10000.0
		}

		var nb, ns float64
		if vb > 0.0 {
			nb += 1.0
			if vb >= 200.0 {
				nb += 1.0
			}
			if vb >= 800.0 {
				nb += 1.0
			}
		}
		if vs > 0.0 {
			ns += 1.0
			if vs >= 200.0 {
				ns += 1.0
			}
			if vs >= 800.0 {
				ns += 1.0
			}
		}
		var qb, qs float64
		if vb > 0.0 {
			qb = math.Sqrt(vb)
		}
		if vs > 0.0 {
			qs = math.Sqrt(vs)
		}

		t.csNB += nb
		t.csNS += ns
		t.csQB += qb
		t.csQS += qs

		enRing := int((b + 1) % (baseline15mBars + 1))
		t.csNBRing[enRing] = t.csNB
		t.csNSRing[enRing] = t.csNS
		t.csQBRing[enRing] = t.csQB
		t.csQSRing[enRing] = t.csQS

		st := b - baseline15mBars + 1
		if st < 0 {
			st = 0
		}
		stRing := int(st % (baseline15mBars + 1))
		cnt15m := (t.csNB - t.csNBRing[stRing]) + (t.csNS - t.csNSRing[stRing])
		if cnt15m < 1.0 {
			cnt15m = 1.0
		}
		q15m := (t.csQB - t.csQBRing[stRing]) + (t.csQS - t.csQSRing[stRing])
		if q15m < 1.0 {
			q15m = 1.0
		}
		t.lastCnt15m = cnt15m
		t.lastQ15m = q15m
		t.lastClosedCol = b
		t.nextCol = b + 1

		if ring != nil {
			slot := b & (shm.MaxRing250msBars - 1)
			ring.Bars[slot] = shm.Bar250ms{
				BucketCol:  b,
				BidTicks:   bidTicks,
				AskTicks:   askTicks,
				LitBuyVol:  vb,
				LitSellVol: vs,
				Cnt15m:     cnt15m,
				Q15m:       q15m,
				MidTicks:   t.lastMidTicks,
			}
			if t.firstValidCol == b && b > 0 {
				for k := int64(0); k < shm.MaxRing250msBars; k++ {
					if ring.Bars[k].BucketCol >= 0 && ring.Bars[k].BucketCol < b && ring.Bars[k].BidTicks == 0 {
						ring.Bars[k].BidTicks = bidTicks
						ring.Bars[k].AskTicks = askTicks
					}
				}
			}
		}
	}

	if ring != nil {
		ring.OpenNS = t.openNS
		ring.LastClosedCol = t.lastClosedCol
		ring.FirstValidCol = t.firstValidCol
		ring.FirstValidBid = t.firstValidBid
		ring.FirstValidAsk = t.firstValidAsk
		ring.Trailing15mCnt = t.lastCnt15m
		ring.Trailing15mQ = t.lastQ15m
		ring.LiveBidTicks = t.lastBidTicks
		ring.LiveAskTicks = t.lastAskTicks
		atomic.StoreUint64(&ring.SeqLockSeq, seq+1)
	}
}

// OnQuote ingests a quote into the 250ms tracker, closing any completed buckets prior to ts.
func (t *Tracker250ms) OnQuote(ts int64, bidPx, askPx float64, ring *shm.SymbolRing250ms) {
	if !(bidPx > 0.0 && askPx >= bidPx) {
		return
	}
	t.AdvanceTo(ts, ring)
	t.hasValidQuote = true
	t.lastBid = bidPx
	t.lastAsk = askPx
	t.lastBidTicks = int32(math.RoundToEven(bidPx * 10000.0))
	t.lastAskTicks = int32(math.RoundToEven(askPx * 10000.0))
}

// OnTrade ingests a trade into the 250ms tracker, closing any completed buckets prior to ts
// and classifying lit volume against the contemporaneous matched quote midpoint.
func (t *Tracker250ms) OnTrade(ts int64, price, size float64, isDark bool, ring *shm.SymbolRing250ms) {
	if !(price > 0.0 && size > 0.0) {
		return
	}
	t.AdvanceTo(ts, ring)
	if !t.openSet || ts < t.openNS || !t.hasValidQuote || isDark {
		return
	}
	midU := 0.5 * (t.lastBid + t.lastAsk)
	if price < midU-atMidEps250ms {
		t.curLitSellVol += size
	} else if price > midU+atMidEps250ms {
		t.curLitBuyVol += size
	}
}


