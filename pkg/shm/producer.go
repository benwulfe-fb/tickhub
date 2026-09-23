package shm

import (
	"fmt"
	"os"
	"sync/atomic"
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
}

// Producer manages writes into POSIX shared memory.
type Producer struct {
	cfg          Config
	segment      *Segment
	header       *GlobalHeader
	snapshots    []*SymbolSnapshot
	phaseOffsets []uintptr
	phaseStrides []uintptr
	symbolMap    map[string]int // symbol name -> directory index
	phaseSymMap  []map[string]int // phase index -> (symbol name -> phase symbol index)
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
	header.Mode = ModeLiveStreaming
	header.MaxFrames = cfg.MaxFrames
	header.NumPhases = uint32(len(cfg.Phases))
	header.TotalSymbols = uint32(len(cfg.UniqueSymbols))
	header.CadenceInterval = uint64(cfg.CadenceInterval.Nanoseconds())
	header.DaemonPID = int64(os.Getpid())
	header.HeartbeatNS = time.Now().UnixNano()
	header.Phases = phaseInfos

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

// SetStatus updates daemon operational state.
func (p *Producer) SetStatus(status uint32) {
	atomic.StoreUint32(&p.header.Status, status)
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

// CommitSymbolMetrics writes D feature float64 values for a symbol within a phase at anchorNS.
// Performs atomic Store-Release on symbol_anchor_ns[symbolPhaseIdx].
func (p *Producer) CommitSymbolMetrics(phaseIdx int, symbolPhaseIdx int, anchorNS int64, features []float64) error {
	if phaseIdx < 0 || phaseIdx >= len(p.phaseOffsets) {
		return fmt.Errorf("invalid phase index %d", phaseIdx)
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
	atomic.StoreInt64(&p.header.LastWrittenAnchorNS, anchorNS)
}

// PublishTelemetry updates dynamic latency, watermark, and tick counters atomically.
func (p *Producer) PublishTelemetry(publishLatencyNS, watermarkBufferNS int64, droppedTicks, totalTicks uint64) {
	atomic.StoreInt64(&p.header.AnchorPublishLatencyNS, publishLatencyNS)
	atomic.StoreInt64(&p.header.WatermarkBufferNS, watermarkBufferNS)
	atomic.StoreUint64(&p.header.DroppedTickCount, droppedTicks)
	atomic.StoreUint64(&p.header.TotalTickCount, totalTicks)
	atomic.StoreInt64(&p.header.HeartbeatNS, time.Now().UnixNano())
}

// Close cleanly detaches memory and unlinks if configured.
func (p *Producer) Close() error {
	p.SetStatus(StatusClosed)
	return p.segment.Close(p.cfg.UnlinkOnExit)
}
