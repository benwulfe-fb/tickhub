# TickHub Operational Playbooks

This directory contains incident response, diagnostic, and remediation playbooks for all 9 enforced Service Level Objectives (SLOs) governing TickHub.

---

## SLO Incident & Alert Matrix

| SLO ID | Playbook | Budget | Severity | Primary Alert Trigger |
| :--- | :--- | :--- | :--- | :--- |
| **SLO-01** | [`slo_01_publish_latency.md`](./slo_01_publish_latency.md) | $P_{99} \le 15\,\text{ms}$ | P1 | `tickhub_publish_latency_nanoseconds > 15000000` or `[SLO-VIOLATION] 1Hz publish latency` |
| **SLO-02** | [`slo_02_seqlock_contention.md`](./slo_02_seqlock_contention.md) | $P_{99} \le 50\,\text{ns}$ | P2 | `SeqLock contention timeout reading snapshot` |
| **SLO-03** | [`slo_03_feature_matrix_load.md`](./slo_03_feature_matrix_load.md) | $P_{99} \le 2.0\,\mu\text{s}$ | P2 | `load_sync` returns non-empty `failed_symbols` |
| **SLO-04** | [`slo_04_local_shm_frame_loss.md`](./slo_04_local_shm_frame_loss.md) | $0$ frame loss ($100\%$) | P0 | `[SLO-VIOLATION] Frame lag / buffer overrun` or `LaggedAnchorError` |
| **SLO-05** | [`slo_05_relay_sequence_delivery.md`](./slo_05_relay_sequence_delivery.md) | $99.999\%$ delivery | P0 | `[SLO-VIOLATION] [RELAY-CLI] Sequence gap detected` |
| **SLO-06** | [`slo_06_relay_latency.md`](./slo_06_relay_latency.md) | WAN $\le 10\,\text{ms}$, Loopback $\le 500\,\mu\text{s}$ | P1 | `[SLO-VIOLATION] [RELAY-CLI] Replication latency exceeded 10ms` |
| **SLO-07** | [`slo_07_daemon_midday_recovery.md`](./slo_07_daemon_midday_recovery.md) | $T_{\text{recover}} \le 200\,\text{ms}$ | P1 | `[SLO-VIOLATION] Sub-cadence recovery downtime exceeded` |
| **SLO-08** | [`slo_08_cold_recovery_convergence.md`](./slo_08_cold_recovery_convergence.md) | $T_{\text{converge}} \le 15\,\text{s}$ | P1 | `FlagColdStart` remains set $> 15\,\text{s}$ or unexpected cold start |
| **SLO-09** | [`slo_09_heartbeat_staleness.md`](./slo_09_heartbeat_staleness.md) | Stale threshold: $> 3.0\,\text{s}$ | P0 | HTTP 503 on `/readyz` or `HeartbeatTimeoutError` in Python |

---

## Triage & Escalation Hierarchy

1. **P0 (Emergency - Capital Risk)**:
   - **Triggers**: SLO-04 (Frame Loss), SLO-05 (Relay Packet Sequence Gap), SLO-09 (Heartbeat Stalled $> 3\text{s}$).
   - **Immediate Action**: Inhibit new trading orders in downstream execution models.
   - **Resolution Goal**: Resync memory state or failover within 5 seconds.
2. **P1 (Urgent - Latency / Performance Regression)**:
   - **Triggers**: SLO-01 (1Hz Publishing Latency), SLO-06 (Relay Latency $> 10\text{ms}$), SLO-07 (Recovery Delay $> 200\text{ms}$), SLO-08 (Cold Start Warmup Active).
   - **Immediate Action**: Inspect CPU affinity, network socket buffers, and Massive WebSocket backlog.
3. **P2 (Operational Warning - Non-Blocking Contention)**:
   - **Triggers**: SLO-02 (SeqLock Retries), SLO-03 (Feature Load Delay).
   - **Immediate Action**: Monitor core contention and memory bus load.

---

## Core Operational Tools

- **Realtime Dashboard**:
  ```bash
  tickhub status --shm tickhub_live --watch
  ```
- **Prometheus Metrics**:
  ```bash
  curl -s http://localhost:9090/metrics | grep tickhub_
  ```
- **Supervisor Readiness**:
  ```bash
  curl -i http://localhost:9090/readyz
  ```
- **Daemon Logs**:
  ```bash
  journalctl -u tickhub -n 100 -f
  ```
