package project

import (
	"math"
	"time"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
)

const maxHistoryBars = 32

// SymbolHistory tracks past closed bar metrics for multi-second rolling returns.
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
