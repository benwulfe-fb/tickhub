package project

import (
	"math"
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
	hasTraded   bool
	initialized bool
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
}

func (h *SymbolHistory) UpdateTrade(price, size float64) {
	if price > 0 {
		h.lastPrice = price
		h.hasTraded = true
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
	}

	h.prices[currSlot] = px
	h.volumes[currSlot] = h.currentVol
	vol1s = h.currentVol
	h.currentVol = 0 // Reset volume for next 1-second interval

	// Spread in basis points
	if h.lastBid > 0 && h.lastAsk > 0 && px > 0 {
		spreadBps = ((h.lastAsk - h.lastBid) / px) * 10000.0
	} else {
		spreadBps = 1.0 // default minimum 1 bps
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
		logRet1s = math.Log(px / p1)
	}

	// 5-second log return: ln(P_T / P_{T-5})
	prev5Slot := (slot - 5 + maxHistoryBars) % maxHistoryBars
	p5 := h.prices[prev5Slot]
	if p5 > 0 {
		logRet5s = math.Log(px / p5)
	}

	// 15-second log return: ln(P_T / P_{T-15})
	prev15Slot := (slot - 15 + maxHistoryBars) % maxHistoryBars
	p15 := h.prices[prev15Slot]
	if p15 > 0 {
		logRet15s = math.Log(px / p15)
	}

	return logRet1s, logRet5s, logRet15s, vol1s, spreadBps
}
