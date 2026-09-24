# Playbook: SLO-06 — Relay Replication Latency

## 1. SLO Definition & Budget
- **Objective**: Time elapsed from server-side phase finalization in VM `/dev/shm` to client-side receipt and replication in remote node `/dev/shm` must not exceed 10.0 milliseconds.
- **Budget**: $P_{99} \le 10.0\,\text{ms}$ ($10,000\,\mu\text{s}$).
- **Normal Range**: $1.5\,\text{ms} - 6.0\,\text{ms}$ over Tailscale / wire link.

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  tickhub_relay_replication_latency_microseconds{quantile="0.99"} > 10000
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] [RELAY-CLI] Replication latency exceeded 10ms budget: %dµs @ anchor %d
  ```
- **Relay Client Telemetry**:
  Logged in `pkg/relay/client.go` when `now - header.TimestampNS > 10ms`.

---

## 3. Severity & Business Impact
- **Severity**: **P1 (Urgent)**
- **Impact**: Increased latency between live VM and WSL paper engine impairs parity validation. In live-parallel or shadow-testing modes, late ticks skew paper execution pricing, leading to false divergence signals between VM and WSL execution logs.

---

## 4. Diagnostics & Root Cause Analysis

1. **Measure Network RTT**:
   ```bash
   ping -c 20 <relay-server-ip>
   ```
   Check if base round-trip time has degraded past 5ms.
2. **Inspect Tailscale Direct vs DERP Relay Connection**:
   ```bash
   tailscale status
   ```
   If connection is routed via DERP relay rather than direct WireGuard peer-to-peer, latency increases by $20 - 100\,\text{ms}$.
3. **Verify TCP_NODELAY (Nagle's Algorithm)**:
   Verify `pkg/relay/server.go` and `pkg/relay/client.go` enable `TCP_NODELAY` on accepted and dialed connections:
   ```go
   tcpConn.SetNoDelay(true)
   ```
4. **Check Sender / Receiver Serialization Overhead**:
   Inspect CPU load on sender and receiver nodes. High CPU usage may delay buffer flushing.

---

## 5. Mitigation Procedures

1. **Force Tailscale Direct Peer-to-Peer Connection**:
   If DERP relaying is active:
   ```bash
   tailscale ping <relay-server-ip>
   ```
   Check firewall UDP port 41641 to ensure direct NAT traversal is unobstructed.
2. **Prioritize Relay Network Traffic**:
   Apply QoS or traffic shaping to prioritize port `9190` (TickHub Relay) over bulk file transfer or web traffic.
3. **Enable TCP Low-Latency Settings**:
   Ensure `TCP_NODELAY` and `TCP_QUICKACK` are set on the socket.

---

## 6. Verification
1. Inspect relay client logs: confirm `Replication latency` reports $< 6000\mu\text{s}$.
2. Verify metric:
   ```bash
   curl -s http://localhost:9090/metrics | grep tickhub_relay_replication_latency_microseconds
   ```
