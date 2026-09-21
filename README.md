# TickHub

A realtime marketdata hub for tickdata. TickHub is a low latency, zero allocation daemon that allows you to forward marketdata to local and remote clients, condensing marketdata into a configurable cadence with an extensible set of windowed metrics.

It ingests real-time market data feeds, maintains lock-free rolling circular buffers with zero allocations in the critical path, computes immediate realtime metrics (NBBO, midprice, spread, last trade), projects tick streams into fixed-cadence metric bars (e.g., 1Hz), and fans out projections and real-time state to local processes and network clients.

---

## Architecture Overview

```
                        ┌───────────────────────────────┐
                        │   Upstream Market Data Feed   │
                        │    (Massive.com WebSocket)    │
                        └───────────────┬───────────────┘
                                        │ (Raw Frames)
                                        ▼
                        ┌───────────────────────────────┐
                        │    Stream Demux & Parsing     │
                        │     (Quotes / Trades)         │
                        └───────────────┬───────────────┘
                                        │
                ┌───────────────────────┴───────────────────────┐
                ▼                                               ▼
┌───────────────────────────────┐               ┌───────────────────────────────┐
│     Immediate Metric State    │               │      Zero-Alloc Arena         │
│   (Atomic / Lockless Cache)   │               │   (Per-Symbol Ring Buffer)    │
│  - NBBO (Bid/Ask Px & Size)   │               │  - Quotes & Trades Ring       │
│  - Midprice & Spread          │               │  - Bounded Memory Window      │
│  - Last Trade Px & Size       │               └───────────────┬───────────────┘
└───────────────┬───────────────┘                               │
                │                                               ▼
                │                               ┌───────────────────────────────┐
                │                               │   Cadence Projection Engine   │
                │                               │     (Configurable, e.g. 1Hz)  │
                │                               │  - Volume & Notional (Buy/Sell│
                │                               │  - Trade & Quote Counts       │
                │                               │  - Extensible Metric Blocks   │
                │                               └───────────────┬───────────────┘
                │                                               │
                └───────────────────────┬───────────────────────┘
                                        ▼
                        ┌───────────────────────────────┐
                        │         Fanout Hub            │
                        │  - Local IPC (UDS / SHM)      │
                        │  - Network (TCP / WebSocket)  │
                        └───────────────────────────────┘
```

---

## Core Principles

1. **Zero Heap Allocation in Hot Path**:
   Tick buffers, projection buckets, and dispatch envelopes are pre-allocated at startup into fixed-capacity arenas and ring buffers. Ingestion and projection run with zero steady-state garbage collection pressure.

2. **Decoupled Ingestion and Projection**:
   Network ingestion does not wait on metric calculations or downstream consumers. Ingestion writes into per-symbol ring buffers; independent projection workers derive periodic metrics at configured frequencies.

3. **Dual Metric Access**:
   - **Realtime Immediate Access**: Sub-microsecond access to instantaneous market state (NBBO, midprice, spread, last trade).
   - **Cadenced Projections**: Periodic (e.g. 1Hz) multi-dimensional time-bucket aggregates for feature engineering, alpha generation, and model scoring.

4. **Extensible Metric Pipeline**:
   Projection blocks are modular interfaces. New derived features (e.g., microstructural imbalance, order flow toxicity, tick rules) can be registered without modifying the feed ingestion core.

5. **Multi-Transport Fanout**:
   Distribute projections and realtime updates to co-located processes (via low-latency Unix domain sockets or shared memory) and remote clients (TCP / WebSockets).

---

## Metric Catalog

### Realtime Immediate Metrics
- **NBBO**: Best bid price (`BidPx`), best ask price (`AskPx`), bid size (`BidSz`), ask size (`AskSz`), and participant exchanges.
- **Midprice**: `(BidPx + AskPx) / 2`
- **Spread**: `AskPx - BidPx` (bps and ticks)
- **Last Trade**: Price (`LastPx`), size (`LastSz`), side classification, and exchange timestamp.

### Cadenced Projections (Default 1Hz)
- **Trade Flow**: Buy volume, sell volume, buy notional, sell notional, buy/sell trade counts.
- **Quote Dynamics**: Bid quote count, ask quote count, high/low bid/ask over interval.
- **Price Trajectory**: Interval high, low, volume-weighted summary, and standard deviation.
- **Microstructure**: Order book staleness, quote rates, and trade-to-quote alignment.

---

## Getting Started

### Prerequisites
- Go 1.23+

### Build
```bash
go build -o bin/tickhub ./cmd/tickhub
```

### Run Tests
```bash
go test -v -race ./...
```

---

## License
Apache License 2.0. See [LICENSE](LICENSE) for details.
