package recorder

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/benwulfe-fb/tickhub/pkg/shm"
	"github.com/parquet-go/parquet-go"
	"gopkg.in/yaml.v3"
)

type symbolRef struct {
	name     string
	dirIdx   int
	phaseIdx int
	symIdx   int // index inside phase
	phaseID  uint32
}

// Recorder attaches to a local POSIX SHM segment and records 1Hz metrics to Parquet.
type Recorder struct {
	cfg         Config
	mu          sync.Mutex
	seg         *shm.Segment
	configYAML  []byte
	symbols     []symbolRef
	trackedMap  map[string]bool
	buffer      []MetricRow
	lastAnchor  int64
	lastFlush   time.Time
	stats       Stats
	currentFile *os.File
	writer      *parquet.GenericWriter[MetricRow]
	currentPath string
	closed      atomic.Bool
	tzLoc       *time.Location
	rthStartH   int
	rthStartM   int
	rthStartS   int
	rthEndH     int
	rthEndM     int
	rthEndS     int
	lastRTHLog  time.Time
}

// NewRecorder initializes a new Recorder instance attached to the specified SHM segment.
func NewRecorder(cfg Config) (*Recorder, error) {
	if cfg.SHMName == "" {
		cfg.SHMName = "tickhub_live"
	}
	if cfg.OutDir == "" {
		cfg.OutDir = "./recordings"
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.Timezone == "" {
		cfg.Timezone = "America/New_York"
	}
	if cfg.RTHStart == "" {
		cfg.RTHStart = "09:30:00"
	}
	if cfg.RTHEnd == "" {
		cfg.RTHEnd = "16:00:00"
	}

	tzLoc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, fmt.Errorf("load timezone %q: %w", cfg.Timezone, err)
	}

	startH, startM, startS, err := parseTimeHMS(cfg.RTHStart)
	if err != nil {
		return nil, fmt.Errorf("invalid RTHStart %q: %w", cfg.RTHStart, err)
	}
	endH, endM, endS, err := parseTimeHMS(cfg.RTHEnd)
	if err != nil {
		return nil, fmt.Errorf("invalid RTHEnd %q: %w", cfg.RTHEnd, err)
	}

	cleanName := strings.TrimPrefix(cfg.SHMName, "/dev/shm/")
	cleanName = strings.TrimPrefix(cleanName, "tickhub_")

	seg, err := shm.AttachSegment(cleanName, true)
	if err != nil {
		return nil, fmt.Errorf("attach to /dev/shm/tickhub_%s: %w", cleanName, err)
	}

	hdr := seg.Header()
	raw := seg.Bytes()

	// Extract embedded config YAML from SHM if present
	var configYAML []byte
	if hdr.ConfigOffset > 0 && hdr.ConfigLen > 0 {
		end := uintptr(hdr.ConfigOffset) + uintptr(hdr.ConfigLen)
		if end <= uintptr(seg.Size()) {
			configYAML = make([]byte, hdr.ConfigLen)
			copy(configYAML, raw[hdr.ConfigOffset:end])
		}
	}

	trackedMap := make(map[string]bool)
	for _, s := range cfg.Symbols {
		s = strings.TrimSpace(s)
		if s != "" {
			trackedMap[s] = true
		}
	}

	// Map phase symbols from embedded config YAML if present
	phaseSymMap := make(map[string]struct {
		pIdx    int
		sIdx    int
		phaseID uint32
	})

	if len(configYAML) > 0 {
		var parsed struct {
			Phases []struct {
				ID      uint32   `yaml:"id"`
				Symbols []string `yaml:"symbols"`
			} `yaml:"phases"`
		}
		if err := yaml.Unmarshal(configYAML, &parsed); err == nil {
			for pIdx, p := range parsed.Phases {
				for sIdx, sym := range p.Symbols {
					phaseSymMap[sym] = struct {
						pIdx    int
						sIdx    int
						phaseID uint32
					}{pIdx: pIdx, sIdx: sIdx, phaseID: p.ID}
				}
			}
		}
	}

	// Discover symbols from SHM directory
	var symbols []symbolRef
	totalSymbols := int(hdr.TotalSymbols)
	if totalSymbols > shm.MaxDirectorySymbols {
		totalSymbols = shm.MaxDirectorySymbols
	}

	for i := 0; i < totalSymbols; i++ {
		dirEntry := (*shm.SymbolDirectoryEntry)(unsafe.Pointer(&raw[shm.DirectoryOffset+uintptr(i*16)]))
		name := strings.TrimRight(string(dirEntry.Name[:]), "\x00")
		if name == "" {
			continue
		}

		if len(trackedMap) > 0 && !trackedMap[name] {
			continue
		}

		pInfo, ok := phaseSymMap[name]
		pIdx := -1
		sIdx := -1
		var pID uint32 = 0
		if ok {
			pIdx = pInfo.pIdx
			sIdx = pInfo.sIdx
			pID = pInfo.phaseID
		} else if int(hdr.NumPhases) > 0 {
			// Default to phase 0 if single phase
			pIdx = 0
			sIdx = i
			pID = hdr.Phases[0].PhaseID
		}

		symbols = append(symbols, symbolRef{
			name:     name,
			dirIdx:   i,
			phaseIdx: pIdx,
			symIdx:   sIdx,
			phaseID:  pID,
		})
	}

	return &Recorder{
		cfg:        cfg,
		seg:        seg,
		configYAML: configYAML,
		symbols:    symbols,
		trackedMap: trackedMap,
		buffer:     make([]MetricRow, 0, cfg.BatchSize*2),
		lastFlush:  time.Now(),
		tzLoc:      tzLoc,
		rthStartH:  startH,
		rthStartM:  startM,
		rthStartS:  startS,
		rthEndH:    endH,
		rthEndM:    endM,
		rthEndS:    endS,
	}, nil
}

// parseTimeHMS parses "HH:MM" or "HH:MM:SS" into hours, minutes, seconds.
func parseTimeHMS(s string) (int, int, int, error) {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != 2 && len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("expected HH:MM or HH:MM:SS, got %q", s)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil || h < 0 || h > 23 {
		return 0, 0, 0, fmt.Errorf("invalid hour in %q", s)
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 0 || m > 59 {
		return 0, 0, 0, fmt.Errorf("invalid minute in %q", s)
	}
	sec := 0
	if len(parts) == 3 {
		sec, err = strconv.Atoi(parts[2])
		if err != nil || sec < 0 || sec > 59 {
			return 0, 0, 0, fmt.Errorf("invalid second in %q", s)
		}
	}
	return h, m, sec, nil
}

// IsRTH checks whether the given anchor timestamp (nanoseconds) falls within Regular Trading Hours (Mon-Fri 09:30:00 - 16:00:00 in configured timezone).
func (r *Recorder) IsRTH(anchorNS int64) bool {
	t := time.Unix(0, anchorNS).In(r.tzLoc)

	// Monday through Friday only
	weekday := t.Weekday()
	if weekday == time.Saturday || weekday == time.Sunday {
		return false
	}

	start := time.Date(t.Year(), t.Month(), t.Day(), r.rthStartH, r.rthStartM, r.rthStartS, 0, r.tzLoc)
	end := time.Date(t.Year(), t.Month(), t.Day(), r.rthEndH, r.rthEndM, r.rthEndS, 0, r.tzLoc)

	return !t.Before(start) && !t.After(end)
}

// Start runs the polling loop, capturing 1Hz snapshots on anchor advances.
func (r *Recorder) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if r.closed.Load() {
				return nil
			}
			if err := r.PollOnce(); err != nil {
				return err
			}
		}
	}
}

// PollOnce inspects SHM and records a row for each tracked symbol if an anchor has advanced.
func (r *Recorder) PollOnce() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return nil
	}

	hdr := r.seg.Header()
	anchorNS := atomic.LoadInt64(&hdr.LastWrittenAnchorNS)
	if anchorNS == 0 || anchorNS <= r.lastAnchor {
		// Check if time-based flush is due even without new anchor
		if time.Since(r.lastFlush) >= r.cfg.FlushInterval && len(r.buffer) > 0 {
			return r.flushLocked()
		}
		return nil
	}

	// RTH gating: if enabled and anchor falls outside RTH, flush any remaining buffer and skip row recording
	if r.cfg.RTHOnly && !r.IsRTH(anchorNS) {
		if len(r.buffer) > 0 {
			if err := r.flushLocked(); err != nil {
				return fmt.Errorf("flush at RTH close: %w", err)
			}
		}
		r.lastAnchor = anchorNS

		now := time.Now().UnixNano()
		r.stats.AnchorLagNS = now - anchorNS
		r.stats.LastAnchorNS = anchorNS

		if r.lastRTHLog.IsZero() || time.Since(r.lastRTHLog) >= 15*time.Minute {
			t := time.Unix(0, anchorNS).In(r.tzLoc)
			log.Printf("[RECORD] Outside RTH (%s %s); skipping recording (anchor: %d)",
				t.Weekday(), t.Format("15:04:05 MST"), anchorNS)
			r.lastRTHLog = time.Now()
		}
		return nil
	}

	// Reset RTH log throttle so the next transition outside RTH immediately logs once
	r.lastRTHLog = time.Time{}

	cadenceNS := int64(hdr.CadenceInterval)
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}

	// Detect and track skipped frames due to polling latency
	if r.lastAnchor > 0 {
		skipped := (anchorNS - r.lastAnchor) / cadenceNS
		if skipped > 1 {
			r.stats.DroppedFrames += (skipped - 1) * int64(len(r.symbols))
		}
	}

	raw := r.seg.Bytes()
	now := time.Now().UnixNano()
	r.stats.AnchorLagNS = now - anchorNS
	r.stats.LastAnchorNS = anchorNS

	for _, s := range r.symbols {
		snapPtr := (*shm.SymbolSnapshot)(unsafe.Pointer(&raw[shm.SnapshotOffset+uintptr(s.dirIdx*128)]))

		// 1,000-spin SeqLock read with runtime.Gosched()
		var snap shm.SymbolSnapshot
		var readOk bool
		for spin := 0; spin < 1000; spin++ {
			seq1 := atomic.LoadUint64(&snapPtr.SeqLockSeq)
			if seq1%2 != 0 {
				runtime.Gosched()
				continue
			}
			snap = *snapPtr
			seq2 := atomic.LoadUint64(&snapPtr.SeqLockSeq)
			if seq1 == seq2 {
				readOk = true
				break
			}
			runtime.Gosched()
		}
		if !readOk {
			r.stats.DroppedFrames++
			continue
		}

		// Extract features from phase ring buffer verbatim without recalculation (SSoT preserved)
		var features []float64
		var slot uint32 = 0
		var logRet1s, logRet5s, logRet15s, vol1s, spreadBps float64

		if s.phaseIdx >= 0 && s.phaseIdx < int(hdr.NumPhases) {
			pInfo := hdr.Phases[s.phaseIdx]
			slot = uint32((anchorNS / cadenceNS) & int64(hdr.MaxFrames-1))
			frameOffset := uintptr(pInfo.RingOffsetBytes) + uintptr(slot)*uintptr(pInfo.FrameStrideBytes)

			if frameOffset+uintptr(pInfo.FrameStrideBytes) <= uintptr(r.seg.Size()) {
				frameHdr := (*shm.FrameHeader)(unsafe.Pointer(&raw[frameOffset]))
				if atomic.LoadInt64(&frameHdr.AnchorNS) == anchorNS && s.symIdx >= 0 && s.symIdx < int(pInfo.NumSymbols) {
					nFeat := int(pInfo.NumFeatures)
					featOffset := frameOffset + 64 + uintptr(pInfo.NumSymbols)*8 + uintptr(s.symIdx)*uintptr(nFeat)*8
					if featOffset+uintptr(nFeat*8) <= uintptr(r.seg.Size()) {
						featSlice := unsafe.Slice((*float64)(unsafe.Pointer(&raw[featOffset])), nFeat)
						features = make([]float64, nFeat)
						copy(features, featSlice)

						if nFeat >= 5 {
							logRet1s = features[0]
							logRet5s = features[1]
							logRet15s = features[2]
							vol1s = features[3]
							spreadBps = features[4]
						}
					}
				}
			}
		}

		row := MetricRow{
			TimestampNS: anchorNS,
			RecvTSNS:    snap.RecvTimestampNS,
			SeqLockSeq:  snap.SeqLockSeq,
			Symbol:      s.name,
			PhaseID:     s.phaseID,
			PhaseSlot:   slot,
			BidPX:       snap.BidPx,
			AskPX:       snap.AskPx,
			BidSZ:       snap.BidSz,
			AskSZ:       snap.AskSz,
			MidPX:       snap.Midprice,
			SpreadPX:    snap.Spread,
			SpreadBPS:   spreadBps,
			LastTradePX: snap.LastTradePx,
			LastTradeSZ: snap.LastTradeSz,
			BidExch:     snap.BidExch,
			AskExch:     snap.AskExch,
			TradeExch:   snap.TradeExch,
			Conditions:  snap.Conditions,
			Imbalance:   0.0,
			Microprice:  0.0,
			Vol1S:       vol1s,
			LogRet1S:    logRet1s,
			LogRet5S:    logRet5s,
			LogRet15S:   logRet15s,
			Features:    features,
		}

		r.buffer = append(r.buffer, row)
		r.stats.TotalRows++
	}

	r.lastAnchor = anchorNS

	if len(r.buffer) >= r.cfg.BatchSize || time.Since(r.lastFlush) >= r.cfg.FlushInterval {
		return r.flushLocked()
	}
	return nil
}

// targetFilePath computes the destination Parquet path based on anchor timestamp or config.
func (r *Recorder) targetFilePath(tsNS int64) string {
	if r.cfg.OutFile != "" {
		return r.cfg.OutFile
	}
	t := time.Unix(0, tsNS).UTC()
	dateDir := filepath.Join(r.cfg.OutDir, t.Format("2006-01-02"))
	return filepath.Join(dateDir, "1hz_metrics.parquet")
}

func (r *Recorder) flushLocked() error {
	if len(r.buffer) == 0 {
		r.lastFlush = time.Now()
		return nil
	}

	targetPath := r.targetFilePath(r.buffer[0].TimestampNS)

	// If rotating to a new file, close previous writer
	if r.writer != nil && r.currentPath != targetPath {
		if err := r.writer.Close(); err != nil {
			return fmt.Errorf("close parquet writer for %s: %w", r.currentPath, err)
		}
		if err := r.currentFile.Close(); err != nil {
			return fmt.Errorf("close file %s: %w", r.currentPath, err)
		}
		r.writer = nil
		r.currentFile = nil
	}

	if r.writer == nil {
		if err := os.MkdirAll(filepath.Dir(targetPath), 0775); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(targetPath), err)
		}

		f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0664)
		if err != nil {
			return fmt.Errorf("open %s: %w", targetPath, err)
		}

		writer := parquet.NewGenericWriter[MetricRow](f, parquet.Compression(&parquet.Snappy))
		if len(r.configYAML) > 0 {
			writer.SetKeyValueMetadata("tickhub_config_yaml", string(r.configYAML))
		}

		r.currentFile = f
		r.writer = writer
		r.currentPath = targetPath
		r.stats.FilesWritten++
	}

	n, err := r.writer.Write(r.buffer)
	if err != nil {
		return fmt.Errorf("write parquet rows to %s: %w", r.currentPath, err)
	}

	if err := r.writer.Flush(); err != nil {
		return fmt.Errorf("flush parquet row group to %s: %w", r.currentPath, err)
	}

	r.stats.FlushedRows += int64(n)
	r.buffer = r.buffer[:0]
	r.lastFlush = time.Now()
	return nil
}

// Flush forces any buffered rows to disk.
func (r *Recorder) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flushLocked()
}

// Stats returns a copy of operational telemetry.
func (r *Recorder) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// Close cleanly flushes remaining buffers, writes the Parquet footer, and detaches SHM.
func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Swap(true) {
		return nil
	}

	var errs []string
	if err := r.flushLocked(); err != nil {
		errs = append(errs, fmt.Sprintf("final flush: %v", err))
	}

	if r.writer != nil {
		if err := r.writer.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("close parquet writer: %v", err))
		}
		r.writer = nil
	}

	if r.currentFile != nil {
		if err := r.currentFile.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("close parquet file: %v", err))
		}
		r.currentFile = nil
	}

	if r.seg != nil {
		if err := r.seg.Close(false); err != nil {
			errs = append(errs, fmt.Sprintf("close SHM segment: %v", err))
		}
		r.seg = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("recorder close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
