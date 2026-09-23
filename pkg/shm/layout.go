package shm

import "unsafe"

const (
	MagicBytes        uint64 = 0x5449434B48554231 // "TICKHUB1"
	CurrentABIVersion uint32 = 1

	HeaderOffset    uintptr = 0x00000000
	DirectoryOffset uintptr = 0x00000400
	SnapshotOffset  uintptr = 0x00001000
	DataAreaOffset  uintptr = 0x00010000 // 64 KB boundary (page aligned)

	MaxDirectorySymbols = 192
	MaxSnapshotSymbols  = 128
	MaxPhases           = 8

	// Status flags
	StatusUninitialized uint32 = 0
	StatusBooting       uint32 = 1
	StatusRunning       uint32 = 2
	StatusHalting       uint32 = 3
	StatusClosed        uint32 = 4

	// Operational modes
	ModeLiveStreaming   uint32 = 0
	ModeHistoricalReplay uint32 = 1
)

// PhaseInfo describes a single phase's geometry in the SHM header.
type PhaseInfo struct {
	PhaseID          uint32
	OffsetMS         uint32
	NumSymbols       uint32
	NumFeatures      uint32
	FrameStrideBytes uint64
	RingOffsetBytes  uint64
}

// GlobalHeader resides at offset 0x00000000 (exactly 1,024 bytes).
type GlobalHeader struct {
	Magic           uint64
	Version         uint32
	Status          uint32
	Mode            uint32
	MaxFrames       uint32
	NumPhases       uint32
	TotalSymbols    uint32
	CadenceInterval uint64 // nanoseconds (e.g. 1_000_000_000)
	DaemonPID       int64
	HeartbeatNS     int64

	// Producer Cache Line (Aligned to 64 bytes at offset 0x0040)
	_pad0                 [8]byte
	AnchorPublishLatencyNS int64
	WatermarkBufferNS     int64
	LastWrittenAnchorNS   int64
	DroppedTickCount      uint64
	TotalTickCount        uint64
	_padProducer          [24]byte

	// Consumer Cache Line (Aligned to 64 bytes at offset 0x0080)
	LastReadAnchorNS  int64
	ConsumerPID       int64
	ConsumerHeartbeat int64
	_padConsumer      [40]byte

	// Phase geometry descriptors (up to 8 phases = 8 * 32 = 256 bytes)
	Phases [MaxPhases]PhaseInfo

	_reserved [576]byte
}

// SymbolDirectoryEntry describes a symbol in the directory (16 bytes).
type SymbolDirectoryEntry struct {
	Name        [8]byte // null-padded ASCII ticker (e.g. "AAPL\0\0\0\0")
	LotSize     uint32
	SymbolIndex uint16
	Flags       uint16
}

// SymbolSnapshot holds top-of-book state protected by a SeqLock (128 bytes).
type SymbolSnapshot struct {
	SeqLockSeq      uint64
	SIPTimestampNS  int64
	RecvTimestampNS int64
	BidPx           float64
	AskPx           float64
	BidSz           float64
	AskSz           float64
	LastTradePx     float64
	LastTradeSz     float64
	Midprice        float64
	Spread          float64
	BidExch         uint32
	AskExch         uint32
	TradeExch       uint32
	Conditions      uint32
	_pad            [24]byte
}

// FrameHeader starts each frame in a phase ring buffer (64 bytes).
type FrameHeader struct {
	AnchorNS         int64
	StartTimestampNS int64
	EndTimestampNS   int64
	NumSymbols       uint32
	NumFeatures      uint32
	Flags            uint32
	_pad             [28]byte
}

// Compile-time struct size verifications to guarantee binary ABI stability.
var (
	_ [1024]byte = [unsafe.Sizeof(GlobalHeader{})]byte{}
	_ [16]byte   = [unsafe.Sizeof(SymbolDirectoryEntry{})]byte{}
	_ [128]byte  = [unsafe.Sizeof(SymbolSnapshot{})]byte{}
	_ [64]byte   = [unsafe.Sizeof(FrameHeader{})]byte{}
	_ [32]byte   = [unsafe.Sizeof(PhaseInfo{})]byte{}
)

