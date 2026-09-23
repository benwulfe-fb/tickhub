package feed

type TickType uint8

const (
	TickQuote TickType = 0
	TickTrade TickType = 1
)

func (t TickType) String() string {
	switch t {
	case TickQuote:
		return "QUOTE"
	case TickTrade:
		return "TRADE"
	default:
		return "UNKNOWN"
	}
}

// Tick represents a normalized market tick (either a trade or a quote).
type Tick struct {
	SIPTimestampNS int64
	Symbol         string
	Type           TickType
	Price          float64
	Size           float64
	BidPx          float64
	AskPx          float64
	BidSz          float64
	AskSz          float64
}

// TickReader is an iterator over a chronological stream of Ticks.
type TickReader interface {
	Next() bool
	Tick() Tick
	Err() error
	Close() error
}
