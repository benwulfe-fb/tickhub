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

def __getattr__(name: str):
    if name == "export_features":
        from .export import export_features
        return export_features
    raise AttributeError(f"module 'tickhub' has no attribute '{name}'")

__all__ = [
    "TickHubReader",
    "SymbolCursor",
    "LaggedAnchorError",
    "export_features",
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
