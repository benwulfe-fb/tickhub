# Playbook: TickHub Relay Setup & Local Resting Order Watcher

## 1. Overview & Architecture

The **TickHub Relay** replicates high-frequency market data frames and top-of-book SeqLock snapshots across machines over a low-latency TCP connection (`THRL` binary protocol).

This allows developers and operators to run diagnostic scripts, resting order watchers, shadow paper engines, or analytics **locally on WSL without touching or adding load to the live production VM** (`ccm-live-1`).

```
+--------------------------------------------------------+
| PROD VM (ccm-live-1: 100.71.0.9)                       |
|   /dev/shm/tickhub_live (Primary SHM)                  |
|   └─ tickhub relay-server -shm tickhub_live -addr 8765 |
+--------------------------------------------------------+
                           │ (TCP binary stream over Tailscale: 15ms latency)
                           ▼
+--------------------------------------------------------+
| LOCAL WORKSTATION (WSL: benwu-home)                    |
|   ~/.local/bin/tickhub relay-client                    |
|   └─ Replicates into local /dev/shm/tickhub_live       |
|                                                        |
|   [Consumer Tools]                                     |
|   ├─ TickHubReader in Python (sub-microsecond read)    |
|   └─ scripts/watch_resting_orders.py (20Hz watcher)    |
+--------------------------------------------------------+
```

---

## 2. Prerequisites

1. **Tailscale Connection**: Ensure local machine can reach `ccm-live-1`:
   ```bash
   nc -zv -w 3 100.71.0.9 8765
   ```
2. **TickHub Binary**: Located at `~/.local/bin/tickhub` or built from `/mnt/wc/tickhub`:
   ```bash
   cd /mnt/wc/tickhub
   CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o bin/tickhub ./cmd/tickhub
   cp bin/tickhub ~/.local/bin/tickhub
   ```
3. **Python Environment**: `TickHubReader` available via `/mnt/wc/tickhub/python`:
   ```bash
   /mnt/wc/miniconda3/envs/gpu_env/bin/python -c "import sys; sys.path.insert(0, '/mnt/wc/tickhub/python'); from tickhub import TickHubReader; print('TickHubReader OK')"
   ```

---

## 3. Starting the Local TickHub Relay Client

Run the relay client inside a detached tmux session on WSL:

```bash
tmux new-session -d -s tickhub-relay-wsl \
  "~/.local/bin/tickhub relay-client -addr 100.71.0.9:8765 -shm tickhub_live -no-unlink"
```

### Verification
Check replication progress via capture-pane:
```bash
tmux capture-pane -p -t tickhub-relay-wsl | tail -15
```
Expected output:
```text
[RELAY-CLI] Replicated anchor 1791216444750000000 to /dev/shm/tickhub_live (53 snapshots, jitter 15807µs, gaps=0, reconnects=0)
```

Inspect local shared memory status:
```bash
~/.local/bin/tickhub status -shm tickhub_live
```

---

## 4. Running the Resting Order Price Watcher

The watcher script tails remote order placement events over SSH and samples the local replicated TickHub SHM at 20Hz (50ms intervals) during every resting order's lifetime.

### Run in tmux
```bash
tmux new-session -d -s tickhub-resting-watcher \
  "/mnt/wc/miniconda3/envs/gpu_env/bin/python -u /mnt/wc/tickhub/scripts/watch_resting_orders.py"
```

### Live Inspection
```bash
tmux capture-pane -p -t tickhub-resting-watcher | tail -25
```

### Monitored Diagnostics
For every placed order, the watcher tracks:
1. **Bid Jump Detection**: Did `current_bid > limit_px`? How many cents above our limit did the away bid jump? How many seconds after placement did the first jump occur?
2. **Trade Throughs**: Did any print execute at or below our limit price (`last_trade_px <= limit_px`)?
3. **Midpoint & Spread Drift**: Trajectory of midpoint from placement to cancel/fill.

Sample output:
```text
================================================================================
[*] NEW ENTRY ORDER: B87 AAOI (Qty: 100) | Mid: $114.16 | Spread: $0.27
    Limit Price: $114.03
--------------------------------------------------------------------------------
[ORDER CANCELLED] B87 AAOI | Limit: $114.03 | Resting Duration: 11.2s
  • DID BID JUMP?: YES (Peak Bid: $114.09, +6.00c above limit, first jump at 0.8s)
  • TRADES <= LIMIT: 0 trades executed at or below limit
  • PLACEMENT QUOTE: Mid: $114.16 | Spread: $0.27
  • FINAL QUOTE:     Bid: $114.09 | Ask: $114.27 | Mid: $114.18
  • MID DRIFT:       +2.2 bps
--------------------------------------------------------------------------------
```

---

## 5. Teardown / Stop Procedures

When monitoring is complete:
```bash
# Terminate watcher
tmux kill-session -t tickhub-resting-watcher

# Terminate relay client
tmux kill-session -t tickhub-relay-wsl

# Clean up local SHM segment
rm -f /dev/shm/tickhub_live
```
