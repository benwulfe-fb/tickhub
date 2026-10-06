package recorder

import "time"

// Config configures the 1Hz telemetry recorder.
type Config struct {
	SHMName       string        // Shared memory segment to attach to (default: "tickhub_live")
	OutDir        string        // Output directory for daily partitioned Parquet files (default: "./recordings")
	OutFile       string        // Optional direct file path override
	Symbols       []string      // Symbol whitelist; if empty, all symbols in SHM directory are recorded
	FlushInterval time.Duration // Interval to flush buffered rows to disk (default: 10s)
	BatchSize     int           // Max buffered rows before immediate flush (default: 5000)
	PollInterval  time.Duration // Polling interval for anchor checks (default: 50ms)
	RTHOnly       bool          // Only record regular trading hours (09:30:00 - 16:00:00 ET, Mon-Fri) (default: true)
	Timezone      string        // Timezone for RTH gating (default: "America/New_York")
	RTHStart      string        // RTH start time HH:MM:SS (default: "09:30:00")
	RTHEnd        string        // RTH end time HH:MM:SS (default: "16:00:00")
}

// DefaultConfig returns the default recorder configuration with RTHOnly enabled.
func DefaultConfig() Config {
	return Config{
		SHMName:       "tickhub_live",
		OutDir:        "./recordings",
		FlushInterval: 10 * time.Second,
		BatchSize:     5000,
		PollInterval:  50 * time.Millisecond,
		RTHOnly:       true,
		Timezone:      "America/New_York",
		RTHStart:      "09:30:00",
		RTHEnd:        "16:00:00",
	}
}
