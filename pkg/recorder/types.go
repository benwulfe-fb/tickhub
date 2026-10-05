package recorder

import (
	"time"
)

// MetricRow represents a canonical 1Hz top-of-book snapshot and feature projection record.
type MetricRow struct {
	TimestampNS int64     `parquet:"ts_ns"`
	RecvTSNS    int64     `parquet:"recv_ts_ns"`
	SeqLockSeq  uint64    `parquet:"seqlock_seq"`
	Symbol      string    `parquet:"symbol"`
	PhaseID     uint32    `parquet:"phase_id"`
	PhaseSlot   uint32    `parquet:"phase_slot"`
	BidPX       float64   `parquet:"bid_px"`
	AskPX       float64   `parquet:"ask_px"`
	BidSZ       float64   `parquet:"bid_sz"`
	AskSZ       float64   `parquet:"ask_sz"`
	MidPX       float64   `parquet:"mid_px"`
	SpreadPX    float64   `parquet:"spread_px"`
	SpreadBPS   float64   `parquet:"spread_bps"`
	LastTradePX float64   `parquet:"last_trade_px"`
	LastTradeSZ float64   `parquet:"last_trade_sz"`
	BidExch     uint32    `parquet:"bid_exch"`
	AskExch     uint32    `parquet:"ask_exch"`
	TradeExch   uint32    `parquet:"trade_exch"`
	Conditions  uint32    `parquet:"conditions"`
	Imbalance   float64   `parquet:"imbalance"`
	Microprice  float64   `parquet:"microprice"`
	Vol1S       float64   `parquet:"vol_1s"`
	LogRet1S    float64   `parquet:"log_ret_1s"`
	LogRet5S    float64   `parquet:"log_ret_5s"`
	LogRet15S   float64   `parquet:"log_ret_15s"`
	Features    []float64 `parquet:"features,list"`
}

// Config configures the 1Hz telemetry recorder.
type Config struct {
	SHMName       string        // Shared memory segment to attach to (default: "tickhub_live")
	OutDir        string        // Output directory for daily partitioned Parquet files (default: "./recordings")
	OutFile       string        // Optional direct file path override
	Symbols       []string      // Symbol whitelist; if empty, all symbols in SHM directory are recorded
	FlushInterval time.Duration // Interval to flush buffered rows to disk (default: 10s)
	BatchSize     int           // Max buffered rows before immediate flush (default: 5000)
	PollInterval  time.Duration // Polling interval for anchor checks (default: 50ms)
}

// Stats holds running operational telemetry for the recorder.
type Stats struct {
	TotalRows     int64
	FlushedRows   int64
	FilesWritten  int64
	DroppedFrames int64
	AnchorLagNS   int64
	LastAnchorNS  int64
}
