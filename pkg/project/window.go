package project

import (
	"math"
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
	microBucketNS   int64 = 250_000_000 // 250ms per bucket
	numMicroBuckets int64 = 128         // 32 seconds of 250ms buckets
)

type microBucket struct {
	bucketIdx      int64
	bid            float64
	ask            float64
	mid            float64
	tradeVol       float64
	tradeNotional  float64
	signedTradeVol float64
	tradeCount     uint32
}

// MicroTracker maintains a fixed-size 250ms ring buffer (128 buckets = 32s)
// per symbol to populate shm.SymbolMicroSnapshot with zero heap allocations.
type MicroTracker struct {
	buckets       [numMicroBuckets]microBucket
	lastBucketIdx int64
	lastBid       float64
	lastAsk       float64
	lastMid       float64
	initialized   bool
}

func (m *MicroTracker) advanceTo(tsNS int64) {
	if tsNS <= 0 {
		return
	}
	targetIdx := tsNS / microBucketNS
	if !m.initialized {
		for i := int64(0); i < numMicroBuckets; i++ {
			bIdx := targetIdx - (numMicroBuckets - 1 - i)
			slot := bIdx & (numMicroBuckets - 1)
			m.buckets[slot] = microBucket{
				bucketIdx: bIdx,
				bid:       m.lastBid,
				ask:       m.lastAsk,
				mid:       m.lastMid,
			}
		}
		m.lastBucketIdx = targetIdx
		m.initialized = true
		return
	}
	if targetIdx <= m.lastBucketIdx {
		return
	}
	gap := targetIdx - m.lastBucketIdx
	if gap >= numMicroBuckets {
		for i := int64(0); i < numMicroBuckets; i++ {
			bIdx := targetIdx - (numMicroBuckets - 1 - i)
			slot := bIdx & (numMicroBuckets - 1)
			m.buckets[slot] = microBucket{
				bucketIdx: bIdx,
				bid:       m.lastBid,
				ask:       m.lastAsk,
				mid:       m.lastMid,
			}
		}
	} else {
		for bIdx := m.lastBucketIdx + 1; bIdx <= targetIdx; bIdx++ {
			slot := bIdx & (numMicroBuckets - 1)
			m.buckets[slot] = microBucket{
				bucketIdx: bIdx,
				bid:       m.lastBid,
				ask:       m.lastAsk,
				mid:       m.lastMid,
			}
		}
	}
	m.lastBucketIdx = targetIdx
}

// SeedQuote initializes baseline bid/ask/mid across the ring buffer.
func (m *MicroTracker) SeedQuote(bid, ask float64) {
	if bid > 0 {
		m.lastBid = bid
	}
	if ask > 0 {
		m.lastAsk = ask
	}
	if m.lastBid > 0 && m.lastAsk > 0 {
		m.lastMid = 0.5 * (m.lastBid + m.lastAsk)
	}
	for i := range m.buckets {
		m.buckets[i].bid = m.lastBid
		m.buckets[i].ask = m.lastAsk
		m.buckets[i].mid = m.lastMid
	}
}

// OnQuote updates the current 250ms bucket with the latest quote and recomputes mid.
func (m *MicroTracker) OnQuote(tsNS int64, bid, ask float64) {
	m.advanceTo(tsNS)
	if bid > 0 {
		m.lastBid = bid
	}
	if ask > 0 {
		m.lastAsk = ask
	}
	if m.lastBid > 0 && m.lastAsk > 0 {
		m.lastMid = 0.5 * (m.lastBid + m.lastAsk)
	}
	if m.initialized {
		slot := m.lastBucketIdx & (numMicroBuckets - 1)
		m.buckets[slot].bid = m.lastBid
		m.buckets[slot].ask = m.lastAsk
		m.buckets[slot].mid = m.lastMid
	}
}

// OnTrade accumulates trade count, volume, notional, and signed volume in the current 250ms bucket.
func (m *MicroTracker) OnTrade(tsNS int64, price, size float64) {
	m.advanceTo(tsNS)
	if !m.initialized || price <= 0 || size <= 0 {
		return
	}
	slot := m.lastBucketIdx & (numMicroBuckets - 1)
	b := &m.buckets[slot]
	b.tradeCount++
	b.tradeVol += size
	b.tradeNotional += price * size
	if m.lastMid > 0 {
		if price >= m.lastMid {
			b.signedTradeVol += size
		} else {
			b.signedTradeVol -= size
		}
	}
}

func (m *MicroTracker) bucketAtLag(lag int64) *microBucket {
	bIdx := m.lastBucketIdx - lag
	slot := bIdx & (numMicroBuckets - 1)
	return &m.buckets[slot]
}

// ComputeSnapshot populates out with rolling lookback mids/bids/asks, 5s quote volatility,
// 3s trade count, and 5s trade volume/VWAP/imbalance as of tsNS.
func (m *MicroTracker) ComputeSnapshot(tsNS int64, out *shm.SymbolMicroSnapshot) {
	if out == nil {
		return
	}
	if tsNS > 0 {
		m.advanceTo(tsNS)
		out.SIPTimestampNS = tsNS
	}
	if !m.initialized {
		out.Mid1sAgo = m.lastMid
		out.Mid2sAgo = m.lastMid
		out.Mid3sAgo = m.lastMid
		out.Mid5sAgo = m.lastMid
		out.Mid10sAgo = m.lastMid
		out.Mid30sAgo = m.lastMid
		out.Bid5sAgo = m.lastBid
		out.Ask5sAgo = m.lastAsk
		return
	}

	out.Mid1sAgo = m.bucketAtLag(4).mid
	out.Mid2sAgo = m.bucketAtLag(8).mid
	out.Mid3sAgo = m.bucketAtLag(12).mid
	b5 := m.bucketAtLag(20)
	out.Mid5sAgo = b5.mid
	out.Bid5sAgo = b5.bid
	out.Ask5sAgo = b5.ask
	out.Mid10sAgo = m.bucketAtLag(40).mid
	out.Mid30sAgo = m.bucketAtLag(120).mid

	// 5s quote volatility (std of 20 bucket-to-bucket mid returns in bps)
	var sumR, sumR2 float64
	const nQ = 20
	for lag := int64(nQ); lag >= 1; lag-- {
		m0 := m.bucketAtLag(lag).mid
		m1 := m.bucketAtLag(lag - 1).mid
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

	// 3s trade count (last 12 buckets: lag 0..11) and 5s trade aggregates (last 20 buckets: lag 0..19)
	var cnt3s uint32
	var vol5s, notl5s, signed5s float64
	for lag := int64(0); lag < 20; lag++ {
		b := m.bucketAtLag(lag)
		if lag < 12 {
			cnt3s += b.tradeCount
		}
		vol5s += b.tradeVol
		notl5s += b.tradeNotional
		signed5s += b.signedTradeVol
	}
	out.TradeCount3s = cnt3s
	out.TradeVol5s = vol5s
	if vol5s > 0 {
		out.VWAP5s = notl5s / vol5s
		out.TradeImbalance5s = signed5s / vol5s
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

