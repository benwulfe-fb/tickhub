package playback

import (
	"time"
)

// Config configures the 1Hz telemetry playback engine.
type Config struct {
	SHMName            string        // Target SHM segment to create (default: "tickhub_playback")
	SourcePath         string        // Path to a single Parquet file or directory containing *.parquet
	Speed              float64       // Replay speed multiplier (1.0 = real-time, 5.0 = 5x, 0 = unthrottled)
	Loop               bool          // Whether to loop playback when EOF reached
	UnlinkOnExit       bool          // Whether to unlink SHM segment on shutdown (default: true)
	FlowControlTimeout time.Duration // Max timeout waiting on consumer advance in unthrottled mode (default: 10s)
}

// Stats holds operational playback metrics.
type Stats struct {
	TotalRows          int64
	AnchorsReplayed    int64
	FlowControlWaitNS  int64
	ElapsedTime        time.Duration
}
