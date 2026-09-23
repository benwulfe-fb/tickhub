package feed

import (
	"testing"
)

type mockReader struct {
	ticks []Tick
	idx   int
}

func (m *mockReader) Next() bool {
	if m.idx >= len(m.ticks) {
		return false
	}
	m.idx++
	return true
}

func (m *mockReader) Tick() Tick {
	return m.ticks[m.idx-1]
}

func (m *mockReader) Err() error { return nil }
func (m *mockReader) Close() error { return nil }

func TestMergerTieBreakingAndChronology(t *testing.T) {
	// Stream 1: AAPL trade @ 100, MSFT quote @ 100, SPY trade @ 200
	r1 := &mockReader{
		ticks: []Tick{
			{SIPTimestampNS: 100, Symbol: "AAPL", Type: TickTrade, Price: 150.0},
			{SIPTimestampNS: 200, Symbol: "SPY", Type: TickTrade, Price: 400.0},
		},
	}

	// Stream 2: AAPL quote @ 100, MSFT trade @ 100, SPY quote @ 200
	r2 := &mockReader{
		ticks: []Tick{
			{SIPTimestampNS: 100, Symbol: "AAPL", Type: TickQuote, Price: 150.0},
			{SIPTimestampNS: 100, Symbol: "MSFT", Type: TickQuote, Price: 300.0},
			{SIPTimestampNS: 100, Symbol: "MSFT", Type: TickTrade, Price: 300.1},
			{SIPTimestampNS: 200, Symbol: "SPY", Type: TickQuote, Price: 400.0},
		},
	}

	merger := NewMerger([]TickReader{r1, r2})
	defer merger.Close()

	var result []Tick
	for merger.Next() {
		result = append(result, merger.Tick())
	}

	if len(result) != 6 {
		t.Fatalf("expected 6 ticks, got %d", len(result))
	}

	// Verify order:
	// At ts=100:
	// 1. AAPL Quote
	// 2. MSFT Quote
	// 3. AAPL Trade
	// 4. MSFT Trade
	// At ts=200:
	// 5. SPY Quote
	// 6. SPY Trade

	expected := []struct {
		ts   int64
		sym  string
		typ  TickType
	}{
		{100, "AAPL", TickQuote},
		{100, "MSFT", TickQuote},
		{100, "AAPL", TickTrade},
		{100, "MSFT", TickTrade},
		{200, "SPY", TickQuote},
		{200, "SPY", TickTrade},
	}

	for i, exp := range expected {
		got := result[i]
		if got.SIPTimestampNS != exp.ts || got.Symbol != exp.sym || got.Type != exp.typ {
			t.Errorf("tick %d: expected (%d, %s, %s), got (%d, %s, %s)",
				i, exp.ts, exp.sym, exp.typ, got.SIPTimestampNS, got.Symbol, got.Type)
		}
	}
}
