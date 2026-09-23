"""TickHub Python client library for zero-allocation POSIX SHM reading."""

from .abi import (
    CURRENT_ABI_VERSION,
    MAGIC_BYTES,
    MAX_PHASES,
    FrameHeader,
    GlobalHeader,
    PhaseInfo,
    SymbolDirectoryEntry,
    SymbolSnapshot,
)
from .atomic import cpu_pause, load_acquire_i64, thread_fence_acquire
from .shm import LaggedAnchorError, SymbolCursor, TickHubReader

__all__ = [
    "TickHubReader",
    "SymbolCursor",
    "LaggedAnchorError",
    "GlobalHeader",
    "PhaseInfo",
    "SymbolDirectoryEntry",
    "SymbolSnapshot",
    "FrameHeader",
    "MAGIC_BYTES",
    "CURRENT_ABI_VERSION",
    "load_acquire_i64",
    "thread_fence_acquire",
    "cpu_pause",
]
