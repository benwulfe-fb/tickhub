# Playbook: SLO-05 — Relay Sequence Delivery

## 1. SLO Definition & Budget
- **Objective**: The TickHub Relay client must receive a strictly contiguous sequence of framed snapshot packets over TCP without drops or gaps.
- **Budget**: `SequenceGaps = 0`, `GapPackets = 0`.
- **Normal Range**: Strictly sequential packet sequence IDs ($P_{k+1} = P_k + 1$) across all frames.

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  rate(tickhub_relay_sequence_gaps_total[1m]) > 0
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] [RELAY-CLI] Sequence gap detected: expected %d, got %d (gap=%d packets)
  ```
- **Relay Client Metric**:
  Incremented in `pkg/relay/client.go` upon encountering non-contiguous sequence numbers (`seq > c.lastPacketSeq + 1`).

---

## 3. Severity & Business Impact
- **Severity**: **P0 (Critical / Severe)**
- **Impact**: Missing packets mean dropped symbol snapshots or missing 1Hz anchors on downstream remote nodes (e.g. WSL paper engine). Models consuming incomplete frames make trading decisions on corrupted feature matrices.

---

## 4. Diagnostics & Root Cause Analysis

1. **Check Relay Daemon Log (Producer/Server)**:
   ```bash
   journalctl -u tickhub-relay -n 50 --no-pager
   ```
   Check for TCP socket buffer overruns (`WSAENOBUFS` / `ENOBUFS`) or connection drops.
2. **Check Relay Client Log (Consumer)**:
   ```bash
   journalctl -u tickhub-relay-client -n 50 --no-pager
   ```
   Look for `[RELAY-CLI] Sequence gap detected` entries.
3. **Inspect Network Link Quality (Tailscale / LAN)**:
   Check packet loss, retransmits, and interface errors:
   ```bash
   netstat -s | grep -i retrans
   ping -c 20 <relay-server-ip>
   ```
4. **Inspect Relay Buffer Sizing**:
   Check if the relay TCP socket buffer size is too small to handle microbursts when 72 symbol snapshots are transmitted simultaneously at the top of the second.

---

## 5. Mitigation Procedures

1. **Verify Network Connectivity & WireGuard / Tailscale Tunnel**:
   ```bash
   tailscale status
   tailscale ping <relay-server-ip>
   ```
   If tunnel is stalled, restart Tailscale daemon on both ends:
   ```bash
   sudo systemctl restart tailscaled
   ```
2. **Increase Socket Read/Write Buffer Sizes**:
   Tune OS socket buffers on both sender and receiver:
   ```bash
   sysctl -w net.core.rmem_max=16777216
   sysctl -w net.core.wmem_max=16777216
   ```
3. **Restart Relay Client**:
   Force relay client to reconnect and resynchronize stream sequence:
   ```bash
   systemctl restart tickhub-relay-client
   ```
   Client re-establishes TCP session and receives clean contiguous stream from next anchor.

---

## 6. Verification
1. Inspect relay client logs: confirm `[RELAY-CLI] Reconnected and synchronized sequence` without gap warnings.
2. Verify sequence gap metric is 0:
   ```bash
   curl -s http://localhost:9090/metrics | grep tickhub_relay_sequence_gaps_total
   ```
