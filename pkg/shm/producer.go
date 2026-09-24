package shm

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// PhaseConfig defines geometry for a phase within the daemon configuration.
type PhaseConfig struct {
	ID       uint32
	Name     string
	OffsetMS uint32
	Symbols  []string
}

// Config defines the complete configuration needed to allocate and run SHM.
type Config struct {
	Name            string
	MaxFrames       uint32 // Must be power of 2
	Phases          []PhaseConfig
	UniqueSymbols   []string
	Features        []string
	CadenceInterval time.Duration
	Permissions     uint32
	UnlinkOnExit    bool
	Mode            uint32 // ModeLiveStreaming (0) or ModeHistoricalReplay (1)
	AllowRecovery   bool   // Attempt warm/resident recovery if segment is present
}

var (
	ErrFlowControlTimeout = fmt.Errorf("replay flow control timeout waiting for consumer")
	ErrConsumerDeadlock   = fmt.Errorf("consumer process terminated or deadlocked")
)

// Producer manages writes into POSIX shared memory.
type Producer struct {
	cfg           Config
	segment       *Segment
	header        *GlobalHeader
	snapshots     []*SymbolSnapshot
	phaseOffsets  []uintptr
	phaseStrides  []uintptr
	symbolMap     map[string]int // symbol name -> directory index
	phaseSymMap   []map[string]int // phase index -> (symbol name -> phase symbol index)
	firstAnchorNS int64
	downtimeNS    int64
	missedAnchors uint64
}

// CreateProducer calculates memory requirements, allocates the segment, formats headers,
// and returns a ready-to-write Producer instance.
func CreateProducer(cfg Config) (*Producer, error) {
	if cfg.MaxFrames == 0 || (cfg.MaxFrames&(cfg.MaxFrames-1)) != 0 {
		return nil, fmt.Errorf("max_frames must be a power of 2 (got %d)", cfg.MaxFrames)
	}
	if len(cfg.Phases) == 0 || len(cfg.Phases) > MaxPhases {
		return nil, fmt.Errorf("number of phases must be between 1 and %d (got %d)", MaxPhases, len(cfg.Phases))
	}
	if len(cfg.UniqueSymbols) > MaxSnapshotSymbols {
		return nil, fmt.Errorf("unique symbols (%d) exceed max snapshot capacity (%d)", len(cfg.UniqueSymbols), MaxSnapshotSymbols)
	}

	numFeatures := uint32(len(cfg.Features))
	currentOffset := DataAreaOffset

	phaseOffsets := make([]uintptr, len(cfg.Phases))
	phaseStrides := make([]uintptr, len(cfg.Phases))
	phaseInfos := [MaxPhases]PhaseInfo{}

	for i, p := range cfg.Phases {
		nSym := uint32(len(p.Symbols))
		// Frame: 64B header + nSym*8B symbol anchors + nSym*nFeatures*8B features
		frameSize := uintptr(64 + nSym*8 + nSym*numFeatures*8)
		// Align frame stride to 64-byte cache line
		frameStride := (frameSize + 63) &^ 63

		ringSize := uintptr(cfg.MaxFrames) * frameStride

		phaseOffsets[i] = currentOffset
		phaseStrides[i] = frameStride

		phaseInfos[i] = PhaseInfo{
			PhaseID:          p.ID,
			OffsetMS:         p.OffsetMS,
			NumSymbols:       nSym,
			NumFeatures:      numFeatures,
			FrameStrideBytes: uint64(frameStride),
			RingOffsetBytes:  uint64(currentOffset),
		}

		currentOffset += ringSize
	}

	totalSize := int64(currentOffset)

	seg, err := OpenOrCreateSegment(cfg.Name, totalSize, cfg.Permissions)
	if err != nil {
		return nil, err
	}

	header := seg.Header()
	header.Magic = MagicBytes
	header.Version = CurrentABIVersion
	header.Status = StatusBooting
	header.Mode = cfg.Mode
	header.MaxFrames = cfg.MaxFrames
	header.NumPhases = uint32(len(cfg.Phases))
	header.TotalSymbols = uint32(len(cfg.UniqueSymbols))
	header.CadenceInterval = uint64(cfg.CadenceInterval.Nanoseconds())
	header.DaemonPID = int64(os.Getpid())
	header.HeartbeatNS = time.Now().UnixNano()
	header.Phases = phaseInfos
	header.BootID = generateBootID()
	header.Generation = 1
	header.RecoveryMode = RecoveryModeColdStart

	// Format Symbol Directory and Unique Snapshots
	raw := seg.Bytes()
	symMap := make(map[string]int)
	snapshots := make([]*SymbolSnapshot, len(cfg.UniqueSymbols))

	for i, s := range cfg.UniqueSymbols {
		symMap[s] = i

		// Write directory entry at DirectoryOffset + i*16
		dirEntry := (*SymbolDirectoryEntry)(unsafe.Pointer(&raw[DirectoryOffset+uintptr(i*16)]))
		copy(dirEntry.Name[:], []byte(s))
		dirEntry.LotSize = 100
		dirEntry.SymbolIndex = uint16(i)

		// Point snapshot pointer to SnapshotOffset + i*128
		snapPtr := (*SymbolSnapshot)(unsafe.Pointer(&raw[SnapshotOffset+uintptr(i*128)]))
		snapshots[i] = snapPtr
	}

	phaseSymMap := make([]map[string]int, len(cfg.Phases))
	for i, p := range cfg.Phases {
		m := make(map[string]int)
		for sIdx, s := range p.Symbols {
			m[s] = sIdx
		}
		phaseSymMap[i] = m
	}

	return &Producer{
		cfg:          cfg,
		segment:      seg,
		header:       header,
		snapshots:    snapshots,
		phaseOffsets: phaseOffsets,
		phaseStrides: phaseStrides,
		symbolMap:    symMap,
		phaseSymMap:  phaseSymMap,
	}, nil
}

func generateBootID() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	id := binary.LittleEndian.Uint64(b[:])
	if id == 0 {
		id = uint64(time.Now().UnixNano())
	}
	return id
}

// CreateProducerWithRecovery checks if an existing segment can be re-attached for warm or gap recovery.
// Returns the Producer, the determined recovery mode (ColdStart, WarmSubCadence, WarmResidentGap), and error.
func CreateProducerWithRecovery(cfg Config) (*Producer, uint32, error) {
	if !cfg.AllowRecovery {
		p, err := CreateProducer(cfg)
		return p, RecoveryModeColdStart, err
	}

	if cfg.MaxFrames == 0 || (cfg.MaxFrames&(cfg.MaxFrames-1)) != 0 {
		return nil, RecoveryModeColdStart, fmt.Errorf("max_frames must be a power of 2 (got %d)", cfg.MaxFrames)
	}
	if len(cfg.Phases) == 0 || len(cfg.Phases) > MaxPhases {
		return nil, RecoveryModeColdStart, fmt.Errorf("number of phases must be between 1 and %d (got %d)", MaxPhases, len(cfg.Phases))
	}
	if len(cfg.UniqueSymbols) > MaxSnapshotSymbols {
		return nil, RecoveryModeColdStart, fmt.Errorf("unique symbols (%d) exceed max snapshot capacity (%d)", len(cfg.UniqueSymbols), MaxSnapshotSymbols)
	}

	numFeatures := uint32(len(cfg.Features))
	currentOffset := DataAreaOffset

	phaseOffsets := make([]uintptr, len(cfg.Phases))
	phaseStrides := make([]uintptr, len(cfg.Phases))

	for i, p := range cfg.Phases {
		nSym := uint32(len(p.Symbols))
		frameSize := uintptr(64 + nSym*8 + nSym*numFeatures*8)
		frameStride := (frameSize + 63) &^ 63
		ringSize := uintptr(cfg.MaxFrames) * frameStride

		phaseOffsets[i] = currentOffset
		phaseStrides[i] = frameStride
		currentOffset += ringSize
	}

	totalSize := int64(currentOffset)

	seg, err := AttachSegment(cfg.Name, false)
	if err == nil {
		hdr := seg.Header()
		featuresMatch := true
		for pIdx := range cfg.Phases {
			if hdr.Phases[pIdx].NumFeatures != numFeatures {
				featuresMatch = false
				break
			}
		}

		if seg.Size() == totalSize &&
			hdr.Magic == MagicBytes &&
			hdr.Version == CurrentABIVersion &&
			hdr.MaxFrames == cfg.MaxFrames &&
			hdr.NumPhases == uint32(len(cfg.Phases)) &&
			hdr.TotalSymbols == uint32(len(cfg.UniqueSymbols)) &&
			featuresMatch {

			lastAnchor := atomic.LoadInt64(&hdr.LastWrittenAnchorNS)
			now := time.Now().UnixNano()
			downtime := now - lastAnchor
			cadence := int64(cfg.CadenceInterval)
			if cadence <= 0 {
				cadence = int64(time.Second)
			}
			maxRingNS := int64(cfg.MaxFrames) * cadence

			if lastAnchor > 0 && downtime > 0 && downtime < maxRingNS {
				// Eligible for resident recovery!
				// Step A: Atomic Quarantine
				atomic.StoreUint32(&hdr.Status, StatusBooting)

				var recMode uint32
				if downtime < cadence {
					recMode = RecoveryModeWarmSubCadence
				} else {
					recMode = RecoveryModeWarmResidentGap
				}

				raw := seg.Bytes()
				symMap := make(map[string]int)
				snapshots := make([]*SymbolSnapshot, len(cfg.UniqueSymbols))
				for i, s := range cfg.UniqueSymbols {
					symMap[s] = i
					snapPtr := (*SymbolSnapshot)(unsafe.Pointer(&raw[SnapshotOffset+uintptr(i*128)]))
					snapshots[i] = snapPtr
				}

				phaseSymMap := make([]map[string]int, len(cfg.Phases))
				for i, p := range cfg.Phases {
					m := make(map[string]int)
					for sIdx, s := range p.Symbols {
						m[s] = sIdx
					}
					phaseSymMap[i] = m
				}

				// Step C: Atomic Metadata Publishing
				atomic.AddUint32(&hdr.Generation, 1)
				atomic.StoreUint32(&hdr.RecoveryMode, recMode)
				atomic.StoreInt64(&hdr.DaemonPID, int64(os.Getpid()))
				atomic.StoreInt64(&hdr.HeartbeatNS, now)
				newBootID := generateBootID()
				atomic.StoreUint64(&hdr.BootID, newBootID)

				prod := &Producer{
					cfg:           cfg,
					segment:       seg,
					header:        hdr,
					snapshots:     snapshots,
					phaseOffsets:  phaseOffsets,
					phaseStrides:  phaseStrides,
					symbolMap:     symMap,
					phaseSymMap:   phaseSymMap,
					downtimeNS:    downtime,
					missedAnchors: uint64(downtime / cadence),
				}
				return prod, recMode, nil
			}
		}
		_ = seg.Close(false)
	}

	// Fallback to cold start creation
	p, err := CreateProducer(cfg)
	return p, RecoveryModeColdStart, err
}

// SetStatus updates daemon operational state.
func (p *Producer) SetStatus(status uint32) {
	atomic.StoreUint32(&p.header.Status, status)
}

// DowntimeNS returns estimated downtime nanoseconds if recovered from resident segment.
func (p *Producer) DowntimeNS() int64 {
	return p.downtimeNS
}

// MissedAnchors returns number of anchors skipped during downtime.
func (p *Producer) MissedAnchors() uint64 {
	return p.missedAnchors
}

// WriteSnapshot performs a lock-free SeqLock write of top-of-book for unique symbolIdx.
func (p *Producer) WriteSnapshot(symbolIdx int, snap *SymbolSnapshot) {
	if symbolIdx < 0 || symbolIdx >= len(p.snapshots) {
		return
	}
	target := p.snapshots[symbolIdx]

	// 1. Advance SeqLock sequence to ODD (Store-Release)
	seq := atomic.AddUint64(&target.SeqLockSeq, 1)

	// 2. Write fields
	target.SIPTimestampNS = snap.SIPTimestampNS
	target.RecvTimestampNS = snap.RecvTimestampNS
	target.BidPx = snap.BidPx
	target.AskPx = snap.AskPx
	target.BidSz = snap.BidSz
	target.AskSz = snap.AskSz
	target.LastTradePx = snap.LastTradePx
	target.LastTradeSz = snap.LastTradeSz
	target.Midprice = snap.Midprice
	target.Spread = snap.Spread
	target.BidExch = snap.BidExch
	target.AskExch = snap.AskExch
	target.TradeExch = snap.TradeExch
	target.Conditions = snap.Conditions

	// 3. Advance SeqLock sequence to EVEN (Store-Release)
	atomic.StoreUint64(&target.SeqLockSeq, seq+1)
}

// WaitConsumerAdvance checks SHM ring buffer capacity in ModeHistoricalReplay.
// Pauses producer if consumer hasn't read enough frames ahead to prevent overruns.
func (p *Producer) WaitConsumerAdvance(anchorNS int64, timeout time.Duration) error {
	if atomic.LoadUint32(&p.header.Mode) != ModeHistoricalReplay {
		return nil
	}

	cadenceNS := int64(p.header.CadenceInterval)
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}

	maxFrames := int64(p.header.MaxFrames)
	if maxFrames <= 1 {
		return nil
	}

	// Track start anchor
	if atomic.LoadInt64(&p.header.FirstAnchorNS) == 0 {
		atomic.CompareAndSwapInt64(&p.header.FirstAnchorNS, 0, anchorNS)
	}
	p.firstAnchorNS = atomic.LoadInt64(&p.header.FirstAnchorNS)

	start := time.Now()
	var lastHB int64
	lastHBTime := time.Now()

	for {
		lastRead := atomic.LoadInt64(&p.header.LastReadAnchorNS)

		// Before first consumer read, allow filling buffer up to maxFrames - 2
		if lastRead == 0 {
			first := atomic.LoadInt64(&p.firstAnchorNS)
			if (anchorNS - first) < (maxFrames-2)*cadenceNS {
				return nil
			}
		} else {
			// Once consumer has started reading, ensure anchor is within ring buffer ahead of lastRead
			if (anchorNS - lastRead) < (maxFrames-2)*cadenceNS {
				return nil
			}
		}

		// Buffer is full. Check timeout
		if timeout > 0 && time.Since(start) > timeout {
			atomic.AddUint64(&p.header.DroppedTickCount, 1)
			return ErrFlowControlTimeout
		}

		// Check consumer liveness every 100ms
		if time.Since(lastHBTime) > 100*time.Millisecond {
			lastHBTime = time.Now()
			pid := atomic.LoadInt64(&p.header.ConsumerPID)
			if pid > 0 {
				if err := syscall.Kill(int(pid), 0); err != nil && errors.Is(err, syscall.ESRCH) {
					atomic.AddUint64(&p.header.DroppedTickCount, 1)
					return ErrConsumerDeadlock
				}
			}
			hb := atomic.LoadInt64(&p.header.ConsumerHeartbeat)
			if hb != lastHB {
				lastHB = hb
			}
		}

		runtime.Gosched()
		time.Sleep(50 * time.Microsecond)
	}
}

// CommitSymbolMetrics writes D feature float64 values for a symbol within a phase at anchorNS.
// Performs atomic Store-Release on symbol_anchor_ns[symbolPhaseIdx].
func (p *Producer) CommitSymbolMetrics(phaseIdx int, symbolPhaseIdx int, anchorNS int64, features []float64) error {
	if phaseIdx < 0 || phaseIdx >= len(p.phaseOffsets) {
		return fmt.Errorf("invalid phase index %d", phaseIdx)
	}

	// In replay mode, throttle on symbol 0 of phase 0
	if phaseIdx == 0 && symbolPhaseIdx == 0 {
		if err := p.WaitConsumerAdvance(anchorNS, 10*time.Second); err != nil {
			return err
		}
	}

	cadenceNS := int64(p.header.CadenceInterval)
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}

	slot := uint32((anchorNS / cadenceNS) & int64(p.header.MaxFrames-1))
	frameOffset := p.phaseOffsets[phaseIdx] + uintptr(slot)*p.phaseStrides[phaseIdx]
	raw := p.segment.Bytes()

	// Verify or write frame header
	frameHdr := (*FrameHeader)(unsafe.Pointer(&raw[frameOffset]))
	if atomic.LoadInt64(&frameHdr.AnchorNS) != anchorNS {
		atomic.StoreInt64(&frameHdr.AnchorNS, anchorNS)
		frameHdr.StartTimestampNS = anchorNS - cadenceNS
		frameHdr.EndTimestampNS = anchorNS
		frameHdr.NumSymbols = p.header.Phases[phaseIdx].NumSymbols
		frameHdr.NumFeatures = p.header.Phases[phaseIdx].NumFeatures
		atomic.StoreUint32(&frameHdr.Flags, 0)
	}

	nSym := uintptr(p.header.Phases[phaseIdx].NumSymbols)
	nFeat := uintptr(p.header.Phases[phaseIdx].NumFeatures)

	// Offsets:
	// FrameHeader: 64 bytes
	// SymbolAnchors: 64 + symbolPhaseIdx * 8
	// Features: 64 + nSym*8 + symbolPhaseIdx * nFeat * 8
	anchorPtr := (*int64)(unsafe.Pointer(&raw[frameOffset+64+uintptr(symbolPhaseIdx)*8]))
	featOffset := frameOffset + 64 + nSym*8 + uintptr(symbolPhaseIdx)*nFeat*8

	featSlice := unsafe.Slice((*float64)(unsafe.Pointer(&raw[featOffset])), nFeat)
	copy(featSlice, features)

	// Store-Release anchor timestamp on symbol
	atomic.StoreInt64(anchorPtr, anchorNS)
	return nil
}

// CommitFrameFinalize marks the overall frame committed and updates last_written_anchor_ns.
func (p *Producer) CommitFrameFinalize(anchorNS int64) {
	if atomic.LoadInt64(&p.header.FirstAnchorNS) == 0 {
		atomic.CompareAndSwapInt64(&p.header.FirstAnchorNS, 0, anchorNS)
	}
	atomic.StoreInt64(&p.header.LastWrittenAnchorNS, anchorNS)
}

// ResetAnchors resets tracking anchors for chunked replay.
// Ring buffer slot addressing is deterministic via modulo anchor time:
// slot = (anchorNS / cadenceNS) & (MaxFrames - 1).
// Resetting FirstAnchorNS and LastReadAnchorNS allows WaitConsumerAdvance
// to permit unthrottled burst writing for the upcoming chunk window.
func (p *Producer) ResetAnchors(firstAnchorNS int64) {
	atomic.StoreInt64(&p.header.FirstAnchorNS, firstAnchorNS)
	atomic.StoreInt64(&p.firstAnchorNS, firstAnchorNS)
	atomic.StoreInt64(&p.header.LastWrittenAnchorNS, 0)
	atomic.StoreInt64(&p.header.LastReadAnchorNS, 0)
}

// PublishTelemetry updates dynamic latency, watermark, and tick counters atomically.
func (p *Producer) PublishTelemetry(publishLatencyNS, watermarkBufferNS int64, droppedTicks, totalTicks uint64) {
	atomic.StoreInt64(&p.header.AnchorPublishLatencyNS, publishLatencyNS)
	atomic.StoreInt64(&p.header.WatermarkBufferNS, watermarkBufferNS)
	atomic.StoreUint64(&p.header.DroppedTickCount, droppedTicks)
	atomic.StoreUint64(&p.header.TotalTickCount, totalTicks)
	atomic.StoreInt64(&p.header.HeartbeatNS, time.Now().UnixNano())
}

// Header returns the typed GlobalHeader pointer.
func (p *Producer) Header() *GlobalHeader {
	return p.header
}

// Bytes returns the raw underlying mapped byte slice.
func (p *Producer) Bytes() []byte {
	return p.segment.Bytes()
}

// WriteRawSlot writes raw slot bytes directly into the ring buffer for phaseIdx at slotIdx.
func (p *Producer) WriteRawSlot(phaseIdx int, slotIdx uint32, rawBytes []byte) error {
	if phaseIdx < 0 || phaseIdx >= len(p.phaseOffsets) {
		return fmt.Errorf("invalid phase index %d", phaseIdx)
	}
	if slotIdx >= p.header.MaxFrames {
		return fmt.Errorf("slotIdx %d out of range [0,%d)", slotIdx, p.header.MaxFrames)
	}
	stride := p.phaseStrides[phaseIdx]
	if uintptr(len(rawBytes)) != stride {
		return fmt.Errorf("raw slot size mismatch: got %d, expected %d", len(rawBytes), stride)
	}
	raw := p.segment.Bytes()
	offset := p.phaseOffsets[phaseIdx] + uintptr(slotIdx)*stride
	copy(raw[offset:offset+stride], rawBytes)
	return nil
}

// ReadRawSlot reads raw slot bytes directly from the ring buffer for phaseIdx at slotIdx, returning a safe copy.
func (p *Producer) ReadRawSlot(phaseIdx int, slotIdx uint32) ([]byte, error) {
	if phaseIdx < 0 || phaseIdx >= len(p.phaseOffsets) {
		return nil, fmt.Errorf("invalid phase index %d", phaseIdx)
	}
	if slotIdx >= p.header.MaxFrames {
		return nil, fmt.Errorf("slotIdx %d out of range [0,%d)", slotIdx, p.header.MaxFrames)
	}
	stride := p.phaseStrides[phaseIdx]
	raw := p.segment.Bytes()
	offset := p.phaseOffsets[phaseIdx] + uintptr(slotIdx)*stride
	out := make([]byte, stride)
	copy(out, raw[offset:offset+stride])
	return out, nil
}
// Segment returns the underlying shared memory Segment.
func (p *Producer) Segment() *Segment {
	return p.segment
}

// Snapshot returns the snapshot pointer for symbol directory index.
func (p *Producer) Snapshot(symbolIdx int) *SymbolSnapshot {
	if symbolIdx < 0 || symbolIdx >= len(p.snapshots) {
		return nil
	}
	return p.snapshots[symbolIdx]
}

// Snapshots returns all symbol snapshots.
func (p *Producer) Snapshots() []*SymbolSnapshot {
	return p.snapshots
}

// SetFrameFlags sets the Flags bitfield on the FrameHeader for phaseIdx at anchorNS.
func (p *Producer) SetFrameFlags(phaseIdx int, anchorNS int64, flags uint32) {
	if phaseIdx < 0 || phaseIdx >= len(p.phaseOffsets) {
		return
	}
	cadenceNS := int64(p.header.CadenceInterval)
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}
	slot := uint32((anchorNS / cadenceNS) & int64(p.header.MaxFrames-1))
	frameOffset := p.phaseOffsets[phaseIdx] + uintptr(slot)*p.phaseStrides[phaseIdx]
	raw := p.segment.Bytes()
	frameHdr := (*FrameHeader)(unsafe.Pointer(&raw[frameOffset]))
	atomic.StoreUint32(&frameHdr.Flags, flags)
}

// FrameBar stores an anchor timestamp and extracted features from a past frame bar.
type FrameBar struct {
	AnchorNS int64
	Features []float64
}

// ReadHistoryBars extracts up to maxBars historical bars for symbolPhaseIdx in phaseIdx from the ring buffer.
// Returned bars are ordered chronologically (oldest to newest).
func (p *Producer) ReadHistoryBars(phaseIdx int, symbolPhaseIdx int, maxBars int) []FrameBar {
	if phaseIdx < 0 || phaseIdx >= len(p.phaseOffsets) || maxBars <= 0 {
		return nil
	}
	lastAnchor := atomic.LoadInt64(&p.header.LastWrittenAnchorNS)
	if lastAnchor == 0 {
		return nil
	}
	cadenceNS := int64(p.header.CadenceInterval)
	if cadenceNS <= 0 {
		cadenceNS = int64(time.Second)
	}

	nSym := uintptr(p.header.Phases[phaseIdx].NumSymbols)
	nFeat := uintptr(p.header.Phases[phaseIdx].NumFeatures)
	raw := p.segment.Bytes()

	bars := make([]FrameBar, 0, maxBars)

	for step := maxBars - 1; step >= 0; step-- {
		anchor := lastAnchor - int64(step)*cadenceNS
		slot := uint32((anchor / cadenceNS) & int64(p.header.MaxFrames-1))
		frameOffset := p.phaseOffsets[phaseIdx] + uintptr(slot)*p.phaseStrides[phaseIdx]

		frameHdr := (*FrameHeader)(unsafe.Pointer(&raw[frameOffset]))
		if atomic.LoadInt64(&frameHdr.AnchorNS) != anchor {
			continue
		}

		anchorPtr := (*int64)(unsafe.Pointer(&raw[frameOffset+64+uintptr(symbolPhaseIdx)*8]))
		if atomic.LoadInt64(anchorPtr) != anchor {
			continue
		}

		featOffset := frameOffset + 64 + nSym*8 + uintptr(symbolPhaseIdx)*nFeat*8
		featSlice := unsafe.Slice((*float64)(unsafe.Pointer(&raw[featOffset])), nFeat)
		feats := make([]float64, nFeat)
		copy(feats, featSlice)

		bars = append(bars, FrameBar{
			AnchorNS: anchor,
			Features: feats,
		})
	}

	return bars
}

// Close cleanly detaches memory and unlinks if configured.
func (p *Producer) Close() error {
	if p.segment != nil && p.segment.data != nil {
		p.SetStatus(StatusClosed)
		return p.segment.Close(p.cfg.UnlinkOnExit)
	}
	return nil
}
