#ifndef TICKHUB_H
#define TICKHUB_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TICKHUB_MAGIC 0x5449434B48554231ULL // "TICKHUB1"
#define TICKHUB_CURRENT_ABI_VERSION 1
#define TICKHUB_MAX_PHASES 8
#define TICKHUB_MAX_SNAPSHOT_SYMBOLS 1024
#define TICKHUB_FLAG_COLD_START 0x01

#define TICKHUB_RECOVERY_MODE_COLD_START 0
#define TICKHUB_RECOVERY_MODE_WARM_SUB_CADENCE 1
#define TICKHUB_RECOVERY_MODE_WARM_RESIDENT_GAP 2

#pragma pack(push, 1)

typedef struct {
    uint32_t phase_id;
    uint32_t offset_ms;
    uint32_t num_symbols;
    uint32_t num_features;
    uint64_t frame_stride_bytes;
    uint64_t ring_offset_bytes;
} PhaseInfo; // 32 bytes

typedef struct {
    uint64_t request_id;
    uint32_t command;
    uint32_t date;
    char     symbol[8];
    int64_t  start_anchor_ns;
    int64_t  end_anchor_ns;
    uint8_t  _pad[24];
} ControlRequest; // 64 bytes

typedef struct {
    uint64_t response_id;
    uint32_t status;
    uint32_t num_frames_written;
    uint32_t cold_start_frames;
    uint32_t _pad;
    int64_t  first_anchor_ns;
    int64_t  last_anchor_ns;
    char     error_msg[24];
} ControlResponse; // 64 bytes

typedef struct {
    uint64_t magic;                    // offset 0
    uint32_t version;                  // offset 8
    uint32_t status;                   // offset 12
    uint32_t mode;                     // offset 16
    uint32_t max_frames;               // offset 20
    uint32_t num_phases;               // offset 24
    uint32_t total_symbols;            // offset 28
    uint64_t cadence_interval;         // offset 32
    int64_t  daemon_pid;               // offset 40
    int64_t  heartbeat_ns;             // offset 48
    int64_t  first_anchor_ns;          // offset 56
    int64_t  anchor_publish_latency_ns;// offset 64 (Producer cache line)
    int64_t  watermark_buffer_ns;      // offset 72
    int64_t  last_written_anchor_ns;   // offset 80
    uint64_t dropped_tick_count;       // offset 88
    uint64_t total_tick_count;         // offset 96
    uint64_t boot_id;                  // offset 104
    uint32_t generation;               // offset 112
    uint32_t recovery_mode;            // offset 116
    uint8_t  _pad_producer[8];         // offset 120
    int64_t  last_read_anchor_ns;      // offset 128 (Consumer cache line)
    int64_t  consumer_pid;             // offset 136
    int64_t  consumer_heartbeat;       // offset 144
    uint8_t  _pad_consumer[40];        // offset 152
    PhaseInfo phases[TICKHUB_MAX_PHASES]; // offset 192 (8 * 32 = 256 bytes)
    ControlRequest control_req;        // offset 448 (64 bytes)
    ControlResponse control_resp;      // offset 512 (64 bytes)
    uint64_t config_offset;            // offset 576 (8 bytes)
    uint32_t config_len;               // offset 584 (4 bytes)
    uint8_t  _pad_config[52];          // offset 588 (52 bytes)
    uint8_t  _reserved[384];           // offset 640 (384 bytes, Total = 1024 bytes)
} GlobalHeader;

typedef struct {
    uint64_t seqlock_seq;              // offset 0
    int64_t  sip_timestamp_ns;         // offset 8
    int64_t  recv_timestamp_ns;        // offset 16
    double   bid_px;                   // offset 24
    double   ask_px;                   // offset 32
    double   bid_sz;                   // offset 40
    double   ask_sz;                   // offset 48
    double   last_trade_px;            // offset 56
    double   last_trade_sz;            // offset 64
    double   midprice;                 // offset 72
    double   spread;                   // offset 80
    uint16_t bid_exch;                 // offset 88
    uint16_t ask_exch;                 // offset 90
    uint16_t trade_exch;               // offset 92
    uint16_t conditions;               // offset 94
    uint8_t  _pad[32];                 // offset 96 (Total = 128 bytes)
} SymbolSnapshot;

typedef struct {
    char     name[8];                  // offset 0
    uint32_t lot_size;                 // offset 8
    uint16_t symbol_index;             // offset 12
    uint8_t  _pad[2];                  // offset 14 (Total = 16 bytes)
} SymbolDirectoryEntry;

typedef struct {
    int64_t  anchor_ns;                // offset 0
    int64_t  start_timestamp_ns;       // offset 8
    int64_t  end_timestamp_ns;         // offset 16
    uint32_t num_symbols;              // offset 24
    uint32_t num_features;             // offset 28
    uint32_t flags;                    // offset 32
    uint8_t  _pad[28];                 // offset 36 (Total = 64 bytes)
} FrameHeader;

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif // TICKHUB_H
