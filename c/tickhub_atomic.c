#define _GNU_SOURCE
#include <stdatomic.h>
#include <stdint.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

// Atomic 64-bit load with strict Acquire semantics
int64_t tickhub_atomic_load_acquire_i64(const int64_t* addr) {
    return atomic_load_explicit((const _Atomic int64_t*)addr, memory_order_acquire);
}

// Hardware thread fence with Acquire ordering
void tickhub_atomic_thread_fence_acquire(void) {
    atomic_thread_fence(memory_order_acquire);
}

// Low-power CPU pause instruction for microsecond spin loops
void tickhub_cpu_pause(void) {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("isb" ::: "memory");
#else
    atomic_thread_fence(memory_order_seq_cst);
#endif
}
