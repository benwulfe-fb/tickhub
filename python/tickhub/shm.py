import asyncio
import ctypes
import logging
import mmap
import os
import time
from pathlib import Path
from typing import Any, Optional, Union

import numpy as np
import yaml

logger = logging.getLogger("tickhub.shm")
logger.addHandler(logging.NullHandler())

from .abi import (
    CURRENT_ABI_VERSION,
    DATA_AREA_OFFSET,
    DIRECTORY_OFFSET,
    HEADER_OFFSET,
    MAGIC_BYTES,
    MAX_PHASES,
    MODE_HISTORICAL_REPLAY,
    MODE_LIVE_STREAMING,
    SNAPSHOT_OFFSET,
    MICRO_SNAPSHOT_OFFSET,
    CONFIG_AREA_OFFSET,
    MAX_CONFIG_BYTES,
    FLAG_COLD_START,
    RECOVERY_MODE_COLD_START,
    RECOVERY_MODE_WARM_SUB_CADENCE,
    RECOVERY_MODE_WARM_RESIDENT_GAP,
    CMD_IDLE,
    CMD_REPLAY_CHUNK,
    CMD_SHUTDOWN,
    CONTROL_STATUS_IDLE,
    CONTROL_STATUS_BUSY,
    CONTROL_STATUS_READY,
    CONTROL_STATUS_ERROR,
    CONTROL_STATUS_EOF,
    ControlRequest,
    ControlResponse,
    FrameHeader,
    GlobalHeader,
    PhaseInfo,
    SymbolDirectoryEntry,
    SymbolSnapshot,
    SymbolMicroSnapshot,
)
from .atomic import cpu_pause, load_acquire_i64, thread_fence_acquire, thread_fence_release


CCM_44_FEATURES: tuple[str, ...] = (
    "buy_volume",
    "sell_volume",
    "buy_notional",
    "sell_notional",
    "buy_trade_count",
    "sell_trade_count",
    "buy_mid_delta_notional",
    "sell_mid_delta_notional",
    "bid_quote_count",
    "ask_quote_count",
    "dark_bid_trade_primitive",
    "dark_ask_trade_primitive",
    "nbbo_quote_rate_primitive",
    "nbbo_staleness_ms_primitive",
    "exchanges_at_bb_primitive",
    "exchanges_at_bo_primitive",
    "high_px",
    "low_px",
    "high_bid_px",
    "low_bid_px",
    "high_ask_px",
    "low_ask_px",
    "last_bid_px",
    "last_ask_px",
    "last_bid_sz",
    "last_ask_sz",
    "last_trade_px",
    "sip_timestamp",
    "trade_vol_std_px",
    "trade_vol_skew",
    "high_px_ns",
    "low_px_ns",
    "high_ask_px_ns",
    "low_ask_px_ns",
    "quote_idx",
    "trade_idx",
    "asof_bid_px",
    "asof_ask_px",
    "asof_bid_sz",
    "asof_ask_sz",
    "asof_trade_px",
    "asof_sip_timestamp",
    "asof_quote_idx",
    "asof_trade_idx",
)

_CCM_ZERO_KEYS: tuple[str, ...] = CCM_44_FEATURES[0:16]
_CCM_NAN_KEYS: tuple[str, ...] = CCM_44_FEATURES[16:30]
_CCM_INT64_KEYS: tuple[str, ...] = CCM_44_FEATURES[30:34]


class HeartbeatTimeoutError(Exception):
    """Raised when the TickHub daemon heartbeat has stalled > 3.0s."""
    pass


class LaggedAnchorError(Exception):
    """Raised when a requested anchor has fallen off the ring buffer and was overwritten."""

    def __init__(self, symbol: str, phase: str, target_anchor_ns: int, latest_anchor_ns: int):
        self.symbol = symbol
        self.phase = phase
        self.target_anchor_ns = target_anchor_ns
        self.latest_anchor_ns = latest_anchor_ns
        super().__init__(
            f"Cursor for symbol '{symbol}' in phase '{phase}' lagged behind SHM ring buffer. "
            f"Requested anchor {target_anchor_ns}, but ring buffer has advanced (latest={latest_anchor_ns}). "
            f"Must call cursor.rebegin() to resynchronize."
        )


class SymbolCursor:
    """Cursor tracking reading state for a specific symbol within a specific phase."""

    __slots__ = (
        "symbol",
        "phase",
        "phase_idx",
        "symbol_idx",
        "target_anchor_ns",
        "cadence_ns",
        "hub",
    )

    def __init__(
        self,
        symbol: str,
        phase: str,
        phase_idx: int,
        symbol_idx: int,
        target_anchor_ns: int,
        cadence_ns: int,
        hub: "TickHubReader",
    ):
        self.symbol = symbol
        self.phase = phase
        self.phase_idx = phase_idx
        self.symbol_idx = symbol_idx
        self.target_anchor_ns = target_anchor_ns
        self.cadence_ns = cadence_ns
        self.hub = hub

    @property
    def staleness_ns(self) -> int:
        """Difference between current wall time and the effective anchor publication time."""
        now_ns = time.time_ns()
        effective_anchor_ns = self.target_anchor_ns + self.hub.anchor_publish_latency_ns
        return now_ns - effective_anchor_ns

    @property
    def is_stale(self) -> bool:
        """True if target anchor is older than one full cadence interval."""
        return self.staleness_ns >= self.cadence_ns

    def next(self) -> None:
        """Advance cursor by one cadence interval."""
        self.target_anchor_ns += self.cadence_ns

    def rebegin(
        self,
        history_steps: int = 0,
        out_buffer: Optional[Any] = None,
    ) -> int:
        """Resynchronizes cursor to the latest anchor on this phase's lattice, optionally loading history."""
        loaded = 0
        if history_steps > 0 and out_buffer is not None:
            loaded = self.hub.load_symbol_history(
                self.symbol, self.phase, history_steps, out_buffer
            )

        # Set target anchor to the next upcoming bar for this phase
        latest_anchor = self.hub.get_latest_phase_anchor(self.phase)
        self.target_anchor_ns = latest_anchor + self.cadence_ns
        return loaded

    def __repr__(self) -> str:
        return (
            f"SymbolCursor(symbol='{self.symbol}', phase='{self.phase}', "
            f"target_anchor_ns={self.target_anchor_ns}, stale={self.is_stale})"
        )


def _get_mmap_address(m: mmap.mmap) -> int:
    """Extract raw memory address from mmap buffer without requiring write permissions."""
    class Py_buffer(ctypes.Structure):
        _fields_ = [
            ("buf", ctypes.c_void_p),
            ("obj", ctypes.py_object),
            ("len", ctypes.c_ssize_t),
            ("itemsize", ctypes.c_ssize_t),
            ("readonly", ctypes.c_int),
            ("ndim", ctypes.c_int),
            ("format", ctypes.c_char_p),
            ("shape", ctypes.POINTER(ctypes.c_ssize_t)),
            ("strides", ctypes.POINTER(ctypes.c_ssize_t)),
            ("suboffsets", ctypes.POINTER(ctypes.c_ssize_t)),
            ("internal", ctypes.c_void_p),
        ]

    view = Py_buffer()
    res = ctypes.pythonapi.PyObject_GetBuffer(ctypes.py_object(m), ctypes.byref(view), 0)
    if res != 0:
        raise RuntimeError("Failed to get mmap buffer pointer")
    addr = view.buf
    ctypes.pythonapi.PyBuffer_Release(ctypes.byref(view))
    return addr


class TickHubReader:
    """Client for reading 1Hz market metrics and top-of-book snapshots directly from POSIX SHM."""

    def __init__(
        self,
        config_path_or_dict: Union[str, Path, dict, None] = None,
        shm_path_override: Optional[str] = None,
    ):
        if config_path_or_dict is None:
            self.config = {}
        elif isinstance(config_path_or_dict, dict):
            self.config = config_path_or_dict
        elif isinstance(config_path_or_dict, (str, Path)):
            p_str = str(config_path_or_dict)
            is_shm = False
            if p_str.startswith("/dev/shm/"):
                is_shm = True
            elif os.path.exists(p_str):
                try:
                    with open(p_str, "rb") as f:
                        magic = f.read(8)
                        if magic == b"TICKHUB1":
                            is_shm = True
                except OSError as e:
                    raise OSError(f"Failed to inspect candidate path '{p_str}': {e}") from e

            if is_shm:
                if shm_path_override is not None and shm_path_override != p_str:
                    raise ValueError(
                        f"Conflicting shm_path_override '{shm_path_override}' provided with positional SHM path '{p_str}'"
                    )
                self.config = {}
                shm_path_override = p_str
            else:
                with open(config_path_or_dict, "r") as f:
                    self.config = yaml.safe_load(f)
        else:
            raise TypeError("config_path_or_dict must be a file path, dict, or None")

        # Parse SHM segment name
        shm_name = (
            shm_path_override
            or self.config.get("shm", {}).get("name")
            or self.config.get("shm_name")
            or "tickhub_default"
        )
        if shm_name.startswith("/dev/shm/"):
            shm_name = shm_name[len("/dev/shm/"):]
        elif shm_name.startswith("dev/shm/"):
            shm_name = shm_name[len("dev/shm/"):]
        clean_name = shm_name.lstrip("/")
        if not clean_name.startswith("tickhub_"):
            clean_name = "tickhub_" + clean_name
        self._shm_file_path = f"/dev/shm/{clean_name}"

        # Open and map POSIX SHM
        if not os.path.exists(self._shm_file_path):
            raise FileNotFoundError(f"TickHub SHM segment not found at {self._shm_file_path}")

        self._writable = False
        try:
            self._fd = os.open(self._shm_file_path, os.O_RDWR)
            self._file_size = os.fstat(self._fd).st_size
            self._mmap = mmap.mmap(self._fd, 0, prot=mmap.PROT_READ | mmap.PROT_WRITE)
            self._writable = True
        except (OSError, PermissionError):
            self._fd = os.open(self._shm_file_path, os.O_RDONLY)
            self._file_size = os.fstat(self._fd).st_size
            self._mmap = mmap.mmap(self._fd, 0, prot=mmap.PROT_READ)
            self._writable = False

        self._base_addr = _get_mmap_address(self._mmap)

        # Map GlobalHeader
        self._header = GlobalHeader.from_address(self._base_addr + HEADER_OFFSET)
        if self._header.magic != MAGIC_BYTES:
            self.close()
            raise ValueError(f"Invalid TickHub magic 0x{self._header.magic:X} (expected 0x{MAGIC_BYTES:X})")
        if self._header.version != CURRENT_ABI_VERSION:
            self.close()
            raise ValueError(f"ABI version mismatch: server={self._header.version}, client={CURRENT_ABI_VERSION}")

        self.max_frames = self._header.max_frames
        self.cadence_ns = self._header.cadence_interval or 1_000_000_000
        self.num_phases = self._header.num_phases
        self.total_symbols = self._header.total_symbols

        # Read published config from SHM if present
        self.raw_config_yaml: Optional[str] = None
        shm_config: dict[str, Any] = {}
        c_off = self._header.config_offset or CONFIG_AREA_OFFSET
        c_len = self._header.config_len
        if c_len > 0 and c_off + c_len <= self._file_size:
            raw_bytes = bytes(self._mmap[c_off : c_off + c_len])
            try:
                self.raw_config_yaml = raw_bytes.decode("utf-8")
                parsed = yaml.safe_load(self.raw_config_yaml)
                if isinstance(parsed, dict):
                    shm_config = parsed
            except Exception as e:
                logger.warning("Failed to parse published config YAML from SHM: %s", e)
        elif self._header.config_len == 0 and c_off + 64 <= self._file_size:
            # Fallback probe at CONFIG_AREA_OFFSET
            probe = bytes(self._mmap[CONFIG_AREA_OFFSET : CONFIG_AREA_OFFSET + 256])
            if b"shm:" in probe or b"phases:" in probe or b"features:" in probe or b"symbols:" in probe:
                null_idx = probe.find(b"\x00")
                read_len = null_idx if null_idx != -1 else 256
                try:
                    self.raw_config_yaml = bytes(self._mmap[CONFIG_AREA_OFFSET : CONFIG_AREA_OFFSET + read_len]).decode("utf-8")
                    parsed = yaml.safe_load(self.raw_config_yaml)
                    if isinstance(parsed, dict):
                        shm_config = parsed
                except Exception:
                    pass

        # If caller did not provide config dict or file, use the published SHM config
        if not self.config and shm_config:
            self.config = shm_config
        elif self.config and shm_config:
            merged = dict(shm_config)
            merged.update(self.config)
            self.config = merged

        self.features: list[str] = list(self.config.get("features", []))

        # Directory and Snapshot lookups
        self._symbol_to_dir_idx: dict[str, int] = {}
        for i in range(self.total_symbols):
            entry = SymbolDirectoryEntry.from_address(self._base_addr + DIRECTORY_OFFSET + i * 16)
            name = entry.name.decode("ascii", errors="replace").rstrip("\x00")
            if name:
                self._symbol_to_dir_idx[name] = i

        # Phase metadata and symbol indices
        self._phase_name_to_idx: dict[str, int] = {}
        self._phase_idx_to_name: dict[int, str] = {}
        self._phase_symbols: dict[str, list[str]] = {}
        self._phase_sym_to_idx: dict[str, dict[str, int]] = {}
        self._phase_infos: list[PhaseInfo] = []

        cfg_phases = self.config.get("phases", [])
        for i in range(self.num_phases):
            p_info = self._header.phases[i]
            self._phase_infos.append(p_info)
            p_cfg = cfg_phases[i] if i < len(cfg_phases) else {}
            p_name = p_cfg.get("name") or f"phase_{p_info.offset_ms}ms"
            self._phase_name_to_idx[p_name] = i
            self._phase_idx_to_name[i] = p_name

            symbols = p_cfg.get("symbols", [])
            if not symbols:
                symbols = sorted(self._symbol_to_dir_idx.keys(), key=lambda s: self._symbol_to_dir_idx[s])
            self._phase_symbols[p_name] = symbols
            sym_map = {s: s_i for s_i, s in enumerate(symbols)}
            self._phase_sym_to_idx[p_name] = sym_map

        # Base pointer address for raw atomic loads
        self._base_addr = ctypes.cast(ctypes.c_char_p(ctypes.addressof(self._header)), ctypes.c_void_p).value
        self._boot_id = self._header.boot_id
        self._generation = self._header.generation
        self._auto_realign = bool(self.config.get("auto_realign", False))

    @property
    def symbols(self) -> list[str]:
        """All unique symbols in directory order."""
        return sorted(self._symbol_to_dir_idx.keys(), key=lambda s: self._symbol_to_dir_idx[s])

    @property
    def phase_names(self) -> list[str]:
        """List of configured phase names."""
        return [self._phase_idx_to_name[i] for i in range(self.num_phases)]

    @property
    def phase_symbols(self) -> dict[str, list[str]]:
        """Mapping from phase name to symbols in that phase."""
        return dict(self._phase_symbols)

    @property
    def boot_id(self) -> int:
        """UUID of the currently running TickHub daemon."""
        return self._header.boot_id

    @property
    def generation(self) -> int:
        """SHM segment generation sequence number."""
        return self._header.generation

    @property
    def recovery_mode(self) -> int:
        """Recovery mode (0=Cold, 1=WarmSubCadence, 2=WarmResidentGap)."""
        return self._header.recovery_mode

    @property
    def is_warm_recovered(self) -> bool:
        """True if the daemon recovered from resident SHM."""
        return self.recovery_mode in (RECOVERY_MODE_WARM_SUB_CADENCE, RECOVERY_MODE_WARM_RESIDENT_GAP)

    def get_frame_flags(self, phase_idx: int, anchor_ns: int) -> int:
        """Reads the FrameHeader.flags bitfield for phase_idx at anchor_ns."""
        p_info = self._phase_infos[phase_idx]
        slot = (anchor_ns // self.cadence_ns) & (self.max_frames - 1)
        frame_offset = p_info.ring_offset_bytes + slot * p_info.frame_stride_bytes
        frame_hdr = FrameHeader.from_address(self._base_addr + frame_offset)
        return frame_hdr.flags

    @property
    def is_cold_start(self) -> bool:
        """True if the latest frame has FlagColdStart set."""
        latest = self.last_written_anchor_ns
        if latest == 0:
            return True
        p_info = self._phase_infos[0]
        offset_ns = p_info.offset_ms * 1_000_000
        phase0_anchor = ((latest - offset_ns) // self.cadence_ns) * self.cadence_ns + offset_ns
        return bool(self.get_frame_flags(0, phase0_anchor) & FLAG_COLD_START)

    def reconnect_if_needed(self, cursors: Optional[list[SymbolCursor]] = None) -> bool:
        """Monitors daemon liveness, BootID changes, and handles transparent recovery."""
        if self._header is None:
            return False

        # 1. Heartbeat freshness check
        hb = self._header.heartbeat_ns
        now_ns = time.time_ns()
        if hb > 0 and (now_ns - hb) > 3_000_000_000:
            raise HeartbeatTimeoutError(f"TickHub daemon heartbeat stalled > 3.0s (age: {(now_ns - hb)/1e9:.2f}s)")

        # 2. StatusBooting backoff
        if self._header.status == 1:  # STATUS_BOOTING
            start_wait = time.time_ns()
            while self._header.status == 1:
                if time.time_ns() - start_wait > 2_000_000_000:
                    break
                time.sleep(0.01)

        # 3. BootID restart detection
        curr_boot = self._header.boot_id
        if curr_boot != 0 and curr_boot != self._boot_id:
            self._boot_id = curr_boot
            self._generation = self._header.generation
            rec_mode = self._header.recovery_mode
            latest_anchor = self.last_written_anchor_ns

            logger.warning(
                f"[RESILIENCE] TickHub daemon restart detected: BootID={curr_boot:#x}, "
                f"Generation={self._generation}, RecoveryMode={rec_mode}"
            )
            if self.is_cold_start:
                logger.warning(
                    "[RESILIENCE] Restart recovery active with FlagColdStart=True. "
                    "Model trade execution inhibited during warmup."
                )

            if cursors and latest_anchor > 0:
                for c in cursors:
                    p_info = self._phase_infos[c.phase_idx]
                    offset_ns = p_info.offset_ms * 1_000_000
                    phase_latest = ((latest_anchor - offset_ns) // self.cadence_ns) * self.cadence_ns + offset_ns
                    oldest_valid = phase_latest - int(self.max_frames - 1) * self.cadence_ns

                    if rec_mode in (RECOVERY_MODE_COLD_START, RECOVERY_MODE_WARM_RESIDENT_GAP):
                        c.target_anchor_ns = phase_latest
                    elif c.target_anchor_ns < oldest_valid:
                        c.target_anchor_ns = phase_latest
            return True
        return False

    @property
    def anchor_publish_latency_ns(self) -> int:
        """Dynamic publishing latency published by the Go daemon."""
        addr = self._base_addr + GlobalHeader.anchor_publish_latency_ns.offset
        return load_acquire_i64(addr)

    @property
    def watermark_buffer_ns(self) -> int:
        """Watermark buffer duration in nanoseconds."""
        addr = self._base_addr + GlobalHeader.watermark_buffer_ns.offset
        return load_acquire_i64(addr)

    @property
    def heartbeat_ns(self) -> int:
        """Daemon heartbeat in epoch nanoseconds."""
        if self._header is None:
            return 0
        return self._header.heartbeat_ns

    @property
    def last_written_anchor_ns(self) -> int:
        """Timestamp of last committed frame anchor."""
        addr = self._base_addr + GlobalHeader.last_written_anchor_ns.offset
        return load_acquire_i64(addr)

    @property
    def status(self) -> int:
        return self._header.status

    @property
    def dropped_ticks(self) -> int:
        return self._header.dropped_tick_count

    @property
    def total_ticks(self) -> int:
        return self._header.total_tick_count

    @property
    def symbols(self) -> list[str]:
        return list(self._symbol_to_dir_idx.keys())

    @property
    def phases(self) -> list[str]:
        return list(self._phase_name_to_idx.keys())

    @property
    def first_anchor_ns(self) -> int:
        """Timestamp of first committed frame anchor in SHM."""
        addr = self._base_addr + GlobalHeader.first_anchor_ns.offset
        return load_acquire_i64(addr)

    @property
    def is_replay_mode(self) -> bool:
        """True if the SHM segment is configured for lossless historical replay."""
        return self._header.mode == MODE_HISTORICAL_REPLAY

    def get_phase_symbols(self, phase_name: str) -> list[str]:
        """Returns the list of symbols configured for the given phase."""
        if phase_name not in self._phase_symbols:
            raise KeyError(f"Phase '{phase_name}' not found. Available: {self.phases}")
        return self._phase_symbols[phase_name]

    def get_latest_phase_anchor(self, phase_name: str) -> int:
        """Calculates the latest committed or eligible anchor on the given phase's lattice."""
        phase_idx = self._phase_name_to_idx[phase_name]
        offset_ms = self._phase_infos[phase_idx].offset_ms
        offset_ns = offset_ms * 1_000_000
        last_written = self.last_written_anchor_ns
        if last_written > 0:
            return ((last_written - offset_ns) // self.cadence_ns) * self.cadence_ns + offset_ns

        now_ns = time.time_ns()
        latency_ns = self.anchor_publish_latency_ns

        # Anchor lattice equation: (now - offset - latency) // cadence * cadence + offset
        anchor = ((now_ns - offset_ns - latency_ns) // self.cadence_ns) * self.cadence_ns + offset_ns
        return anchor

    def begin_all(
        self,
        history_steps: int = 0,
        out_tensor: Optional[Any] = None,
    ) -> tuple[list[SymbolCursor], int]:
        """Initializes cursors for all symbols across all phases.

        Returns:
            (all_cursors, steps_loaded)
        """
        all_cursors: list[SymbolCursor] = []
        steps_loaded = 0

        if self.is_replay_mode:
            # In replay mode, wait for producer to write initial frame
            while self.first_anchor_ns == 0 and self.last_written_anchor_ns == 0:
                cpu_pause()
                time.sleep(0.001)

        for phase_name in self.phases:
            p_idx = self._phase_name_to_idx[phase_name]
            symbols = self._phase_symbols[phase_name]
            offset_ms = self._phase_infos[p_idx].offset_ms
            offset_ns = offset_ms * 1_000_000

            if self.is_replay_mode:
                first = self.first_anchor_ns
                target_anchor = ((first - offset_ns) // self.cadence_ns) * self.cadence_ns + offset_ns
                if target_anchor < first:
                    target_anchor += self.cadence_ns
            else:
                latest_anchor = self.get_latest_phase_anchor(phase_name)
                target_anchor = latest_anchor

            for s_idx, sym in enumerate(symbols):
                cursor = SymbolCursor(
                    symbol=sym,
                    phase=phase_name,
                    phase_idx=p_idx,
                    symbol_idx=s_idx,
                    target_anchor_ns=target_anchor,
                    cadence_ns=self.cadence_ns,
                    hub=self,
                )
                all_cursors.append(cursor)

        return all_cursors, steps_loaded

    def snapshot(self, symbol: str) -> dict[str, Any]:
        """Performs a lock-free SeqLock read of the latest top-of-book and rolling microstructure snapshot for symbol."""
        if symbol not in self._symbol_to_dir_idx:
            raise KeyError(f"Symbol '{symbol}' not found in directory")

        dir_idx = self._symbol_to_dir_idx[symbol]
        snap_offset = SNAPSHOT_OFFSET + dir_idx * 128
        seq_addr = self._base_addr + snap_offset
        snap = SymbolSnapshot.from_address(self._base_addr + snap_offset)

        micro_offset = MICRO_SNAPSHOT_OFFSET + dir_idx * 128
        micro_seq_addr = self._base_addr + micro_offset
        micro = SymbolMicroSnapshot.from_address(self._base_addr + micro_offset)

        for _ in range(100):
            seq1 = load_acquire_i64(seq_addr)
            if seq1 & 1 != 0:
                cpu_pause()
                continue

            thread_fence_acquire()
            bid_px = snap.bid_px
            ask_px = snap.ask_px
            bid_sz = snap.bid_sz
            ask_sz = snap.ask_sz
            last_trade_px = snap.last_trade_px
            last_trade_sz = snap.last_trade_sz
            midprice = snap.midprice
            spread = snap.spread
            sip_ts = snap.sip_timestamp_ns
            recv_ts = snap.recv_timestamp_ns
            bid_exch = snap.bid_exch
            ask_exch = snap.ask_exch
            trade_exch = snap.trade_exch
            conditions = snap.conditions
            thread_fence_acquire()

            seq2 = load_acquire_i64(seq_addr)
            if seq1 == seq2:
                out = {
                    "symbol": symbol,
                    "seq": seq1,
                    "bid_px": bid_px,
                    "ask_px": ask_px,
                    "bid_sz": bid_sz,
                    "ask_sz": ask_sz,
                    "last_trade_px": last_trade_px,
                    "last_trade_sz": last_trade_sz,
                    "midprice": midprice,
                    "spread": spread,
                    "sip_timestamp_ns": sip_ts,
                    "recv_timestamp_ns": recv_ts,
                    "bid_exch": bid_exch,
                    "ask_exch": ask_exch,
                    "trade_exch": trade_exch,
                    "conditions": conditions,
                }
                for _ in range(100):
                    mseq1 = load_acquire_i64(micro_seq_addr)
                    if mseq1 & 1 != 0:
                        cpu_pause()
                        continue
                    thread_fence_acquire()
                    mid_1s = micro.mid_1s_ago
                    mid_2s = micro.mid_2s_ago
                    mid_3s = micro.mid_3s_ago
                    mid_5s = micro.mid_5s_ago
                    mid_10s = micro.mid_10s_ago
                    mid_30s = micro.mid_30s_ago
                    bid_5s = micro.bid_5s_ago
                    ask_5s = micro.ask_5s_ago
                    q_vol_5s = micro.quote_vol_5s_bps
                    t_vol_5s = micro.trade_vol_5s
                    vwap_5s = micro.vwap_5s
                    t_imb_5s = micro.trade_imbalance_5s
                    t_cnt_3s = micro.trade_count_3s
                    thread_fence_acquire()
                    mseq2 = load_acquire_i64(micro_seq_addr)
                    if mseq1 == mseq2:
                        out["mid_1s_ago"] = mid_1s
                        out["mid_2s_ago"] = mid_2s
                        out["mid_3s_ago"] = mid_3s
                        out["mid_5s_ago"] = mid_5s
                        out["mid_10s_ago"] = mid_10s
                        out["mid_30s_ago"] = mid_30s
                        out["bid_5s_ago"] = bid_5s
                        out["ask_5s_ago"] = ask_5s
                        out["quote_vol_5s_bps"] = q_vol_5s
                        out["trade_vol_5s"] = t_vol_5s
                        out["vwap_5s"] = vwap_5s
                        out["trade_imbalance_5s"] = t_imb_5s
                        out["trade_count_3s"] = int(t_cnt_3s)
                        break
                    cpu_pause()
                return out
            cpu_pause()

        raise RuntimeError(f"SeqLock read contention timeout for symbol '{symbol}'")

    async def load(
        self,
        cursors: list[SymbolCursor],
        out_matrix: Any,
        timeout_ms: float = 1500.0,
    ) -> list[str]:
        """Asynchronously loads metrics for the given cursors into out_matrix.

        If cursors are not stale, sleeps until the target anchor publish window.
        Returns list of symbols that could not be queried within timeout_ms.
        """
        if not cursors:
            return []

        # Check staleness: if in replay mode or ANY cursor is stale, skip sleep and drain immediately
        any_stale = any(c.is_stale for c in cursors)
        min_target_ns = min(c.target_anchor_ns for c in cursors)
        last_written = self.last_written_anchor_ns
        if not self.is_replay_mode and not any_stale and min_target_ns > last_written:
            now_ns = time.time_ns()
            latency_ns = self.anchor_publish_latency_ns
            ready_ts_ns = min_target_ns + latency_ns
            sleep_sec = (ready_ts_ns - now_ns) / 1e9
            if sleep_sec > 0.0001:
                await asyncio.sleep(min(sleep_sec, 0.05))

        return self._load_into_matrix(cursors, out_matrix, timeout_ms)

    def load_sync(
        self,
        cursors: list[SymbolCursor],
        out_matrix: Any,
        timeout_ms: float = 1500.0,
    ) -> list[str]:
        """Synchronously loads metrics for cursors into out_matrix."""
        if not cursors:
            return []

        any_stale = any(c.is_stale for c in cursors)
        min_target_ns = min(c.target_anchor_ns for c in cursors)
        last_written = self.last_written_anchor_ns
        if not self.is_replay_mode and not any_stale and min_target_ns > last_written:
            now_ns = time.time_ns()
            latency_ns = self.anchor_publish_latency_ns
            ready_ts_ns = min_target_ns + latency_ns
            sleep_sec = (ready_ts_ns - now_ns) / 1e9
            if sleep_sec > 0.0001:
                time.sleep(min(sleep_sec, 0.05))

        return self._load_into_matrix(cursors, out_matrix, timeout_ms)

    def _load_into_matrix(
        self,
        cursors: list[SymbolCursor],
        out_matrix: Any,
        timeout_ms: float = 1500.0,
    ) -> list[str]:
        """Internal zero-allocation extractor into PyTorch tensor or NumPy matrix."""
        # Convert torch.Tensor to numpy array view if needed (zero-copy)
        if hasattr(out_matrix, "numpy"):
            np_view = out_matrix.numpy()
        elif isinstance(out_matrix, np.ndarray):
            np_view = out_matrix
        else:
            raise TypeError("out_matrix must be a numpy.ndarray or torch.Tensor")

        failed_symbols: list[str] = []
        timeout_ns = int(timeout_ms * 1_000_000)

        # Check for daemon restart or heartbeat stall
        self.reconnect_if_needed(cursors)

        # Process cursors
        for out_idx, cursor in enumerate(cursors):
            p_info = self._phase_infos[cursor.phase_idx]
            target_anchor = cursor.target_anchor_ns
            slot = (target_anchor // self.cadence_ns) & (self.max_frames - 1)
            frame_offset = p_info.ring_offset_bytes + slot * p_info.frame_stride_bytes

            # Check for ring buffer lag (overwritten frame)
            last_written = self.last_written_anchor_ns
            offset_ns = p_info.offset_ms * 1_000_000
            phase_latest = ((last_written - offset_ns) // self.cadence_ns) * self.cadence_ns + offset_ns
            oldest_valid = phase_latest - int(self.max_frames - 1) * self.cadence_ns
            if not self.is_replay_mode and target_anchor < oldest_valid and last_written > 0:
                if self._auto_realign:
                    gap_ms = (oldest_valid - target_anchor) / 1e6
                    logger.warning(
                        f"[SLO-VIOLATION] [RESILIENCE] Frame lag / buffer overrun detected: "
                        f"symbol={cursor.symbol}, phase={cursor.phase}, target={target_anchor} < oldest_valid={oldest_valid} "
                        f"(gap={gap_ms:.1f}ms). Auto-realigning cursor to {phase_latest}."
                    )
                    target_anchor = phase_latest
                    cursor.target_anchor_ns = phase_latest
                    slot = (target_anchor // self.cadence_ns) & (self.max_frames - 1)
                    frame_offset = p_info.ring_offset_bytes + slot * p_info.frame_stride_bytes
                else:
                    raise LaggedAnchorError(cursor.symbol, cursor.phase, target_anchor, last_written)

            # Symbol anchor address
            anchor_addr = self._base_addr + frame_offset + 64 + cursor.symbol_idx * 8

            # Wait for producer to commit this symbol's target anchor
            start_wait = time.time_ns()
            committed = False
            while True:
                loaded_anchor = load_acquire_i64(anchor_addr)
                if loaded_anchor == target_anchor:
                    committed = True
                    break
                if loaded_anchor > target_anchor:
                    if not self.is_replay_mode:
                        if self._auto_realign:
                            target_anchor = loaded_anchor
                            cursor.target_anchor_ns = loaded_anchor
                            slot = (target_anchor // self.cadence_ns) & (self.max_frames - 1)
                            frame_offset = p_info.ring_offset_bytes + slot * p_info.frame_stride_bytes
                            anchor_addr = self._base_addr + frame_offset + 64 + cursor.symbol_idx * 8
                            continue
                        # Slot was already overwritten by future anchor
                        raise LaggedAnchorError(cursor.symbol, cursor.phase, target_anchor, loaded_anchor)
                    else:
                        break

                if (time.time_ns() - start_wait) > timeout_ns:
                    failed_symbols.append(cursor.symbol)
                    break
                if self.status == 4:  # StatusClosed
                    failed_symbols.append(cursor.symbol)
                    break
                cpu_pause()

            if not committed:
                continue

            # Copy feature row directly into np_view[out_idx, :]
            n_sym = p_info.num_symbols
            n_feat = p_info.num_features
            feat_offset = frame_offset + 64 + n_sym * 8 + cursor.symbol_idx * n_feat * 8

            src_features = np.frombuffer(
                self._mmap,
                dtype=np.float64,
                count=n_feat,
                offset=feat_offset,
            )
            np_view[out_idx, :n_feat] = src_features

        return failed_symbols

    def next(self, cursors: list[SymbolCursor]) -> None:
        """Synchronously advances all cursors in the collection by one cadence step."""
        for c in cursors:
            c.next()

    def commit_read(self, cursors: list[SymbolCursor]) -> None:
        """Signals progress to Go daemon in replay mode using min target anchor across active cursors.

        Updating LastReadAnchorNS allows the Go daemon to advance without buffer overruns.
        """
        if not cursors or not getattr(self, "_writable", False):
            return
        min_anchor = min(c.target_anchor_ns for c in cursors)
        addr = self._base_addr + GlobalHeader.last_read_anchor_ns.offset
        hb_addr = self._base_addr + GlobalHeader.consumer_heartbeat.offset
        pid_addr = self._base_addr + GlobalHeader.consumer_pid.offset

        ctypes.c_int64.from_address(pid_addr).value = os.getpid()
        ctypes.c_int64.from_address(hb_addr).value = time.time_ns()
        ctypes.c_int64.from_address(addr).value = min_anchor
        thread_fence_acquire()

    def request_chunk(
        self,
        symbol: str,
        date_str: str,
        start_anchor_ns: int,
        end_anchor_ns: int,
        timeout_s: float = 5.0,
    ) -> tuple[int, int]:
        """Commands the persistent Go tickhub worker daemon to replay a chunk into SHM.

        Ring buffer slots are addressed deterministically via modulo anchor time:
            slot = (anchor_ns // cadence_ns) & (max_frames - 1)
        Frames for anchors in (start_anchor_ns, end_anchor_ns] are written directly
        to their respective modulo slots. Client readers consume frames by advancing
        cursors initialized to start_anchor_ns + cadence_ns.

        Args:
            symbol: Ticker symbol (e.g. 'DASH')
            date_str: Date in 'YYYY-MM-DD' or 'YYYYMMDD' format
            start_anchor_ns: Window start nanoseconds (inclusive)
            end_anchor_ns: Window end nanoseconds (exclusive)
            timeout_s: Maximum seconds to wait for daemon response

        Returns:
            (num_frames_written, cold_start_frames)
        """
        if not getattr(self, "_writable", False):
            raise PermissionError("TickHubReader must be opened O_RDWR to issue control line requests")

        req = self._header.control_req
        resp = self._header.control_resp

        # Monotonic request sequence number
        req_id = max(getattr(self, "_last_req_id", 0), resp.response_id) + 1
        self._last_req_id = req_id

        date_int = int(date_str.replace("-", ""))
        sym_bytes = symbol.encode("ascii")[:8].ljust(8, b"\x00")

        # Register consumer PID
        pid_addr = self._base_addr + GlobalHeader.consumer_pid.offset
        hb_addr = self._base_addr + GlobalHeader.consumer_heartbeat.offset
        ctypes.c_int64.from_address(pid_addr).value = os.getpid()
        ctypes.c_int64.from_address(hb_addr).value = time.time_ns()

        req.symbol = sym_bytes
        req.date = date_int
        req.start_anchor_ns = start_anchor_ns
        req.end_anchor_ns = end_anchor_ns
        req.command = CMD_REPLAY_CHUNK
        thread_fence_release()  # Store-release fence guarantees payload fields visible before request_id
        req.request_id = req_id

        timeout_ns = int(timeout_s * 1_000_000_000)
        t0 = time.time_ns()

        spin_count = 0
        while True:
            if resp.response_id == req_id:
                status = resp.status
                if status == CONTROL_STATUS_READY:
                    return resp.num_frames_written, resp.cold_start_frames
                elif status == CONTROL_STATUS_EOF:
                    return 0, resp.cold_start_frames
                elif status == CONTROL_STATUS_ERROR:
                    err_msg = resp.error_msg.decode("ascii", errors="replace").rstrip("\x00")
                    raise RuntimeError(f"Daemon error servicing chunk: {err_msg}")

            if (time.time_ns() - t0) > timeout_ns:
                raise TimeoutError(f"Timed out after {timeout_s}s waiting for chunk response for {symbol} {date_str}")

            cpu_pause()
            spin_count += 1
            if spin_count > 200:
                time.sleep(0.0001)

    def shutdown_worker(self) -> None:
        """Sends CmdShutdown to worker daemon."""
        if getattr(self, "_writable", False):
            self._header.control_req.command = CMD_SHUTDOWN
            thread_fence_release()

    def load_symbol_history(
        self,
        symbol: str,
        phase: str,
        historical_steps: int,
        out_buffer: Any,
    ) -> int:
        """Loads backward in time from the latest available anchor up to historical_steps."""
        if phase not in self._phase_name_to_idx:
            raise KeyError(f"Phase '{phase}' not found")
        p_idx = self._phase_name_to_idx[phase]
        if symbol not in self._phase_sym_to_idx[phase]:
            raise KeyError(f"Symbol '{symbol}' not in phase '{phase}'")
        s_idx = self._phase_sym_to_idx[phase][symbol]

        p_info = self._phase_infos[p_idx]
        n_sym = p_info.num_symbols
        n_feat = p_info.num_features

        if hasattr(out_buffer, "numpy"):
            np_view = out_buffer.numpy()
        else:
            np_view = out_buffer

        latest_anchor = self.get_latest_phase_anchor(phase)
        steps_to_read = min(historical_steps, int(self.max_frames - 1))
        steps_loaded = 0

        # Read backward: oldest at index 0, latest at index (steps_loaded - 1)
        temp_rows: list[np.ndarray] = []
        for step in range(steps_to_read):
            anchor = latest_anchor - step * self.cadence_ns
            slot = (anchor // self.cadence_ns) & (self.max_frames - 1)
            frame_offset = p_info.ring_offset_bytes + slot * p_info.frame_stride_bytes
            anchor_addr = self._base_addr + frame_offset + 64 + s_idx * 8

            loaded_anchor = load_acquire_i64(anchor_addr)
            if loaded_anchor != anchor:
                break

            feat_offset = frame_offset + 64 + n_sym * 8 + s_idx * n_feat * 8
            feat = np.frombuffer(
                self._mmap,
                dtype=np.float64,
                count=n_feat,
                offset=feat_offset,
            ).copy()
            temp_rows.append(feat)
            steps_loaded += 1

        # Place in chronological order into out_buffer
        for i, row in enumerate(reversed(temp_rows)):
            np_view[i, :n_feat] = row

        return steps_loaded

    def load_window_1hz(
        self,
        symbol: str,
        anchor_ns: int,
        span_s: int = 360,
        out_dict: Optional[dict[str, Any]] = None,
        timeout_ms: float = 50.0,
    ) -> Optional[dict[str, Any]]:
        """Loads a `span_s + 1`-row 1Hz projection dict directly from the 44-slot SHM ring buffer.

        Rows `0 .. span_s - 1` hold the `span_s` whole-second 1Hz bars strictly preceding
        `floor(anchor_ns / 1s) * 1s` (`[anchor_floor_ns - span_s * 1s, anchor_floor_ns)`).
        Row `span_s` holds the terminal point-in-time state as of `anchor_ns`.
        """
        anchor_ns_i = int(anchor_ns)
        anchor_floor_ns = (anchor_ns_i // self.cadence_ns) * self.cadence_ns
        offset_ms = (anchor_ns_i - anchor_floor_ns) // 1_000_000

        chosen_p_idx = -1
        chosen_s_idx = -1
        phase_anchor_ns = anchor_floor_ns

        # 1. Exact phase offset match containing symbol
        for p_idx in range(self.num_phases):
            p_name = self._phase_idx_to_name[p_idx]
            s_map = self._phase_sym_to_idx.get(p_name, {})
            if symbol in s_map and int(self._phase_infos[p_idx].offset_ms) == offset_ms:
                chosen_p_idx = p_idx
                chosen_s_idx = s_map[symbol]
                phase_anchor_ns = anchor_floor_ns + int(self._phase_infos[p_idx].offset_ms) * 1_000_000
                break

        # 2. Fallback: pick phase containing symbol whose anchor in [anchor_floor_ns, anchor_ns_i] is largest
        if chosen_p_idx < 0:
            best_anchor = -1
            for p_idx in range(self.num_phases):
                p_name = self._phase_idx_to_name[p_idx]
                s_map = self._phase_sym_to_idx.get(p_name, {})
                if symbol not in s_map:
                    continue
                cand_anchor = anchor_floor_ns + int(self._phase_infos[p_idx].offset_ms) * 1_000_000
                if cand_anchor <= anchor_ns_i and cand_anchor > best_anchor:
                    best_anchor = cand_anchor
                    chosen_p_idx = p_idx
                    chosen_s_idx = s_map[symbol]
                    phase_anchor_ns = cand_anchor

        # 3. Final fallback: first phase containing symbol
        if chosen_p_idx < 0:
            for p_idx in range(self.num_phases):
                p_name = self._phase_idx_to_name[p_idx]
                s_map = self._phase_sym_to_idx.get(p_name, {})
                if symbol in s_map:
                    chosen_p_idx = p_idx
                    chosen_s_idx = s_map[symbol]
                    phase_anchor_ns = anchor_floor_ns + int(self._phase_infos[p_idx].offset_ms) * 1_000_000
                    break

        if chosen_p_idx < 0:
            raise KeyError(f"Symbol '{symbol}' not found in any configured phase")

        p_info = self._phase_infos[chosen_p_idx]
        n_sym = int(p_info.num_symbols)
        n_feat = int(p_info.num_features)
        if n_feat < 44:
            raise ValueError(f"load_window_1hz requires 44-feature CCM schema, found num_features={n_feat}")

        # Wait up to timeout_ms for the target frame at phase_anchor_ns to be committed
        target_slot = (phase_anchor_ns // self.cadence_ns) & (self.max_frames - 1)
        target_frame_off = p_info.ring_offset_bytes + target_slot * p_info.frame_stride_bytes
        target_anchor_addr = self._base_addr + target_frame_off + 64 + chosen_s_idx * 8

        timeout_ns = int(timeout_ms * 1_000_000)
        t0 = time.time_ns()
        while True:
            loaded_target = load_acquire_i64(target_anchor_addr)
            if loaded_target == phase_anchor_ns:
                break
            if loaded_target > phase_anchor_ns or (time.time_ns() - t0) >= timeout_ns or self.status == 4:
                return None
            cpu_pause()

        oldest_anchor_ns = phase_anchor_ns - (span_s - 1) * self.cadence_ns
        raw_mat = np.empty((span_s, 44), dtype=np.float64)

        for i in range(span_s):
            step_anchor = oldest_anchor_ns + i * self.cadence_ns
            slot = (step_anchor // self.cadence_ns) & (self.max_frames - 1)
            frame_off = p_info.ring_offset_bytes + slot * p_info.frame_stride_bytes
            anchor_addr = self._base_addr + frame_off + 64 + chosen_s_idx * 8

            if load_acquire_i64(anchor_addr) != step_anchor:
                return None

            feat_off = frame_off + 64 + n_sym * 8 + chosen_s_idx * n_feat * 8
            raw_mat[i, :] = np.frombuffer(
                self._mmap,
                dtype=np.float64,
                count=44,
                offset=feat_off,
            )
            if load_acquire_i64(anchor_addr) != step_anchor:
                return None

        sample_len = span_s + 1
        s_start = anchor_floor_ns - span_s * self.cadence_ns
        grid_window = s_start + np.arange(sample_len, dtype=np.int64) * self.cadence_ns

        if out_dict is None:
            proj_res: dict[str, Any] = {
                "quote_idx": np.zeros(sample_len, dtype=np.int32),
                "trade_idx": np.zeros(sample_len, dtype=np.int32),
                "sample_len": sample_len,
                "anchor_timestamp": grid_window,
            }
            for k in _CCM_ZERO_KEYS:
                proj_res[k] = np.zeros(sample_len, dtype=np.float64)
            for k in _CCM_NAN_KEYS:
                proj_res[k] = np.full(sample_len, np.nan, dtype=np.float64)
            for k in _CCM_INT64_KEYS:
                proj_res[k] = np.zeros(sample_len, dtype=np.int64)
        else:
            proj_res = out_dict
            proj_res["sample_len"] = sample_len
            cur_grid = proj_res.get("anchor_timestamp")
            if isinstance(cur_grid, np.ndarray) and cur_grid.shape == (sample_len,):
                cur_grid[:] = grid_window
            else:
                proj_res["anchor_timestamp"] = grid_window
            for k in ("quote_idx", "trade_idx"):
                arr = proj_res.get(k)
                if not isinstance(arr, np.ndarray) or arr.shape != (sample_len,) or arr.dtype != np.int32:
                    proj_res[k] = np.zeros(sample_len, dtype=np.int32)
            for k in _CCM_ZERO_KEYS:
                arr = proj_res.get(k)
                if not isinstance(arr, np.ndarray) or arr.shape != (sample_len,) or arr.dtype != np.float64:
                    proj_res[k] = np.zeros(sample_len, dtype=np.float64)
            for k in _CCM_NAN_KEYS:
                arr = proj_res.get(k)
                if not isinstance(arr, np.ndarray) or arr.shape != (sample_len,) or arr.dtype != np.float64:
                    proj_res[k] = np.full(sample_len, np.nan, dtype=np.float64)
            for k in _CCM_INT64_KEYS:
                arr = proj_res.get(k)
                if not isinstance(arr, np.ndarray) or arr.shape != (sample_len,) or arr.dtype != np.int64:
                    proj_res[k] = np.zeros(sample_len, dtype=np.int64)

        # Populate Slots 0..15 (zeros_keys)
        for col_idx, k in enumerate(_CCM_ZERO_KEYS):
            arr = proj_res[k]
            arr[:span_s] = raw_mat[:, col_idx]
            arr[span_s] = np.nan if k in ("exchanges_at_bb_primitive", "exchanges_at_bo_primitive") else 0.0

        # Populate Slots 16..29 (nan_keys)
        for offset_idx, k in enumerate(_CCM_NAN_KEYS):
            col_idx = 16 + offset_idx
            arr = proj_res[k]
            arr[:span_s] = raw_mat[:, col_idx]
            arr[span_s] = np.nan

        # Populate Slots 30..33 (int64_keys reconstructed from bar_start_ns + offset)
        bar_starts = grid_window[:span_s]
        for offset_idx, k in enumerate(_CCM_INT64_KEYS):
            col_idx = 30 + offset_idx
            offs = raw_mat[:, col_idx]
            arr = proj_res[k]
            arr[:span_s] = np.where(offs < 0.0, np.int64(0), bar_starts + offs.astype(np.int64))
            arr[span_s] = 0

        # Populate Slots 34..35 (quote_idx, trade_idx) and Slots 36..43 (terminal row span_s)
        term_row = raw_mat[span_s - 1]
        q_idx_arr = proj_res["quote_idx"]
        t_idx_arr = proj_res["trade_idx"]
        q_idx_arr[:span_s] = raw_mat[:, 34].astype(np.int32)
        q_idx_arr[span_s] = np.int32(int(term_row[42]))
        t_idx_arr[:span_s] = raw_mat[:, 35].astype(np.int32)
        t_idx_arr[span_s] = np.int32(int(term_row[43]))

        lbp = proj_res["last_bid_px"]
        lap = proj_res["last_ask_px"]
        lbs = proj_res["last_bid_sz"]
        las = proj_res["last_ask_sz"]
        ltp = proj_res["last_trade_px"]
        st = proj_res["sip_timestamp"]

        lbp[span_s] = term_row[36]
        lap[span_s] = term_row[37]
        lbs[span_s] = term_row[38]
        las[span_s] = term_row[39]
        ltp[span_s] = term_row[40]
        st[span_s] = term_row[41]

        if self.is_replay_mode and getattr(self, "_writable", False) and oldest_anchor_ns > 0:
            addr = self._base_addr + GlobalHeader.last_read_anchor_ns.offset
            cur_read = ctypes.c_int64.from_address(addr).value
            if oldest_anchor_ns > cur_read:
                hb_addr = self._base_addr + GlobalHeader.consumer_heartbeat.offset
                pid_addr = self._base_addr + GlobalHeader.consumer_pid.offset
                ctypes.c_int64.from_address(pid_addr).value = os.getpid()
                ctypes.c_int64.from_address(hb_addr).value = time.time_ns()
                thread_fence_release()
                ctypes.c_int64.from_address(addr).value = oldest_anchor_ns

        # 1. Quote State Backfill (matching _projection_kernel_njit)
        valid_q = np.flatnonzero(~np.isnan(lbp))
        if valid_q.size > 0:
            first_q = int(valid_q[0])
            if first_q > 0:
                lbp[:first_q] = lbp[first_q]
                lap[:first_q] = lap[first_q]
                lbs[:first_q] = lbs[first_q]
                las[:first_q] = las[first_q]
                st[:first_q] = st[first_q]
                bb_fill_end = min(first_q, span_s)
                proj_res["exchanges_at_bb_primitive"][:bb_fill_end] = 0.0
                proj_res["exchanges_at_bo_primitive"][:bb_fill_end] = 0.0

        # 2. Trade State Backfill (matching _projection_kernel_njit)
        valid_t = np.flatnonzero(~np.isnan(ltp))
        if valid_t.size > 0:
            first_t = int(valid_t[0])
            if first_t > 0:
                ltp[:first_t] = ltp[first_t]

        # 3. Volume std_px and skew causal forward-fill from 0.0 at window start (matching _projection_kernel_njit)
        std_arr = proj_res["trade_vol_std_px"]
        skew_arr = proj_res["trade_vol_skew"]
        btc_arr = proj_res["buy_trade_count"]
        stc_arr = proj_res["sell_trade_count"]
        prev_std = 0.0
        prev_skew = 0.0
        for k in range(span_s):
            if (btc_arr[k] + stc_arr[k]) == 0.0:
                std_arr[k] = prev_std
                skew_arr[k] = prev_skew
            else:
                prev_std = std_arr[k]
                prev_skew = skew_arr[k]
        std_arr[span_s] = prev_std
        skew_arr[span_s] = prev_skew

        return proj_res

    def close(self) -> None:
        """Closes memory map and underlying file descriptor."""
        if hasattr(self, "_mmap") and self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if hasattr(self, "_fd") and self._fd >= 0:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = -1

    def __enter__(self) -> "TickHubReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()
