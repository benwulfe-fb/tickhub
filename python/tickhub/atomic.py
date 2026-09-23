import ctypes
import os
from pathlib import Path

_LIB_DIR = Path(__file__).parent.resolve()
_LIB_PATH = _LIB_DIR / "libtickhub_atomic.so"

if not _LIB_PATH.exists():
    raise RuntimeError(f"TickHub atomic C library not found at {_LIB_PATH}. Compile with 'make' in c/ first.")

_lib = ctypes.CDLL(str(_LIB_PATH))

# int64_t tickhub_atomic_load_acquire_i64(const int64_t* addr)
_lib.tickhub_atomic_load_acquire_i64.argtypes = [ctypes.c_void_p]
_lib.tickhub_atomic_load_acquire_i64.restype = ctypes.c_int64

# void tickhub_atomic_thread_fence_acquire(void)
_lib.tickhub_atomic_thread_fence_acquire.argtypes = []
_lib.tickhub_atomic_thread_fence_acquire.restype = None

# void tickhub_cpu_pause(void)
_lib.tickhub_cpu_pause.argtypes = []
_lib.tickhub_cpu_pause.restype = None


def load_acquire_i64(addr: int | ctypes.c_void_p) -> int:
    """Atomic 64-bit load with acquire barrier semantics."""
    return _lib.tickhub_atomic_load_acquire_i64(ctypes.c_void_p(int(addr)))


def thread_fence_acquire() -> None:
    """Hardware thread fence with acquire ordering."""
    _lib.tickhub_atomic_thread_fence_acquire()


def cpu_pause() -> None:
    """Low-power CPU pause instruction (_mm_pause / isb) for spin loops."""
    _lib.tickhub_cpu_pause()
