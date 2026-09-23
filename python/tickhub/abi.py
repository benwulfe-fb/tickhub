import ctypes

MAGIC_BYTES = 0x5449434B48554231  # "TICKHUB1"
CURRENT_ABI_VERSION = 1

HEADER_OFFSET = 0x00000000
DIRECTORY_OFFSET = 0x00000400
SNAPSHOT_OFFSET = 0x00001000
DATA_AREA_OFFSET = 0x00010000

MAX_DIRECTORY_SYMBOLS = 192
MAX_SNAPSHOT_SYMBOLS = 128
MAX_PHASES = 8

STATUS_UNINITIALIZED = 0
STATUS_BOOTING = 1
STATUS_RUNNING = 2
STATUS_HALTING = 3
STATUS_CLOSED = 4

MODE_LIVE_STREAMING = 0
MODE_HISTORICAL_REPLAY = 1


class PhaseInfo(ctypes.Structure):
    _fields_ = [
        ("phase_id", ctypes.c_uint32),
        ("offset_ms", ctypes.c_uint32),
        ("num_symbols", ctypes.c_uint32),
        ("num_features", ctypes.c_uint32),
        ("frame_stride_bytes", ctypes.c_uint64),
        ("ring_offset_bytes", ctypes.c_uint64),
    ]


class GlobalHeader(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_uint64),
        ("version", ctypes.c_uint32),
        ("status", ctypes.c_uint32),
        ("mode", ctypes.c_uint32),
        ("max_frames", ctypes.c_uint32),
        ("num_phases", ctypes.c_uint32),
        ("total_symbols", ctypes.c_uint32),
        ("cadence_interval", ctypes.c_uint64),
        ("daemon_pid", ctypes.c_int64),
        ("heartbeat_ns", ctypes.c_int64),
        ("first_anchor_ns", ctypes.c_int64),
        ("anchor_publish_latency_ns", ctypes.c_int64),
        ("watermark_buffer_ns", ctypes.c_int64),
        ("last_written_anchor_ns", ctypes.c_int64),
        ("dropped_tick_count", ctypes.c_uint64),
        ("total_tick_count", ctypes.c_uint64),
        ("_pad_producer", ctypes.c_uint8 * 24),
        ("last_read_anchor_ns", ctypes.c_int64),
        ("consumer_pid", ctypes.c_int64),
        ("consumer_heartbeat", ctypes.c_int64),
        ("_pad_consumer", ctypes.c_uint8 * 40),
        ("phases", PhaseInfo * MAX_PHASES),
        ("_reserved", ctypes.c_uint8 * 576),
    ]


class SymbolDirectoryEntry(ctypes.Structure):
    _fields_ = [
        ("name", ctypes.c_char * 8),
        ("lot_size", ctypes.c_uint32),
        ("symbol_index", ctypes.c_uint16),
        ("flags", ctypes.c_uint16),
    ]


class SymbolSnapshot(ctypes.Structure):
    _fields_ = [
        ("seqlock_seq", ctypes.c_uint64),
        ("sip_timestamp_ns", ctypes.c_int64),
        ("recv_timestamp_ns", ctypes.c_int64),
        ("bid_px", ctypes.c_double),
        ("ask_px", ctypes.c_double),
        ("bid_sz", ctypes.c_double),
        ("ask_sz", ctypes.c_double),
        ("last_trade_px", ctypes.c_double),
        ("last_trade_sz", ctypes.c_double),
        ("midprice", ctypes.c_double),
        ("spread", ctypes.c_double),
        ("bid_exch", ctypes.c_uint32),
        ("ask_exch", ctypes.c_uint32),
        ("trade_exch", ctypes.c_uint32),
        ("conditions", ctypes.c_uint32),
        ("_pad", ctypes.c_uint8 * 24),
    ]


class FrameHeader(ctypes.Structure):
    _fields_ = [
        ("anchor_ns", ctypes.c_int64),
        ("start_timestamp_ns", ctypes.c_int64),
        ("end_timestamp_ns", ctypes.c_int64),
        ("num_symbols", ctypes.c_uint32),
        ("num_features", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("_pad", ctypes.c_uint8 * 28),
    ]


# Exact size verifications against Go SHM layout
assert ctypes.sizeof(PhaseInfo) == 32, f"PhaseInfo size mismatch: {ctypes.sizeof(PhaseInfo)}"
assert ctypes.sizeof(GlobalHeader) == 1024, f"GlobalHeader size mismatch: {ctypes.sizeof(GlobalHeader)}"
assert ctypes.sizeof(SymbolDirectoryEntry) == 16, f"SymbolDirectoryEntry size mismatch: {ctypes.sizeof(SymbolDirectoryEntry)}"
assert ctypes.sizeof(SymbolSnapshot) == 128, f"SymbolSnapshot size mismatch: {ctypes.sizeof(SymbolSnapshot)}"
assert ctypes.sizeof(FrameHeader) == 64, f"FrameHeader size mismatch: {ctypes.sizeof(FrameHeader)}"

# Cache line alignment checks
assert GlobalHeader.anchor_publish_latency_ns.offset == 64, f"Producer line misaligned: {GlobalHeader.anchor_publish_latency_ns.offset}"
assert GlobalHeader.last_read_anchor_ns.offset == 128, f"Consumer line misaligned: {GlobalHeader.last_read_anchor_ns.offset}"
assert GlobalHeader.phases.offset == 192, f"Phases array misaligned: {GlobalHeader.phases.offset}"
