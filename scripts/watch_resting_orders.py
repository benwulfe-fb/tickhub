#!/usr/bin/env python3
"""watch_resting_orders.py — Real-time price and bid-jump watcher for resting orders.

Connects to local replicated TickHub SHM (/dev/shm/tickhub_live) on WSL and
tails remote engine order events from ccm-live-1 via SSH. For every resting order,
it continuously samples the top-of-book at 20Hz (50ms) to monitor:
1. Did the away bid jump over our limit price? (bid_jumped, jump amount, time-to-jump)
2. Did any trades print at or through our limit price? (last_trade_px <= limit_px)
3. Did the spread or midpoint shift during the resting window?
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, "/mnt/wc/tickhub/python")
from tickhub import TickHubReader

VM_HOST = "100.71.0.9"
SSH_KEY = os.path.expanduser("~/.ssh/google_compute_engine")
DEFAULT_SHM = "/dev/shm/tickhub_live"


class RestingOrderWatcher:
    def __init__(self, shm_path=DEFAULT_SHM, vm_host=VM_HOST, log_path="output/resting_orders.jsonl"):
        self.shm_path = shm_path
        self.vm_host = vm_host
        self.log_path = log_path
        self.reader = TickHubReader(shm_path)
        self.active_orders = {}  # oid -> order_dict
        self.lock = threading.Lock()
        self.running = True
        os.makedirs(os.path.dirname(os.path.abspath(log_path)), exist_ok=True)

    def start_polling_thread(self):
        t = threading.Thread(target=self._poll_resting_quotes, daemon=True)
        t.start()
        return t

    def _poll_resting_quotes(self):
        """Poll TickHub top-of-book snapshots at 20Hz for all currently resting orders."""
        last_log = {}
        while self.running:
            time.sleep(0.05)  # 50ms interval (20Hz)
            with self.lock:
                orders_snapshot = list(self.active_orders.values())

            now = time.time()
            for ord_data in orders_snapshot:
                oid = ord_data["order_id"]
                sym = ord_data["symbol"]
                limit_px = ord_data.get("limit_px")
                if limit_px is None or limit_px != limit_px:
                    continue

                try:
                    snap = self.reader.snapshot(sym)
                except Exception:
                    continue
                if not snap:
                    continue

                bid = float(snap.get("bid_px", 0.0))
                ask = float(snap.get("ask_px", 0.0))
                mid = float(snap.get("midprice", 0.0))
                spr = float(snap.get("spread", 0.0))
                trd_px = float(snap.get("last_trade_px", 0.0))
                trd_sz = float(snap.get("last_trade_sz", 0.0))

                # Track min/max
                ord_data["samples_count"] += 1
                ord_data["max_bid"] = max(ord_data["max_bid"], bid)
                ord_data["min_bid"] = min(ord_data["min_bid"], bid) if ord_data["min_bid"] > 0 else bid
                ord_data["max_ask"] = max(ord_data["max_ask"], ask)
                ord_data["min_ask"] = min(ord_data["min_ask"], ask) if ord_data["min_ask"] > 0 else ask
                ord_data["max_mid"] = max(ord_data["max_mid"], mid)
                ord_data["min_mid"] = min(ord_data["min_mid"], mid) if ord_data["min_mid"] > 0 else mid

                # 1. Did the bid jump above our limit?
                if bid > limit_px:
                    if not ord_data["bid_jumped"]:
                        ord_data["bid_jumped"] = True
                        ord_data["time_to_jump_s"] = round(now - ord_data["t_place"], 2)
                        ord_data["first_jump_bid"] = bid
                        print(f"🚨 [BID JUMPED] {oid} {sym} | Limit: ${limit_px:.2f} < Away Bid: ${bid:.2f} "
                              f"(+{round(bid - limit_px, 4):.2f}c) after {ord_data['time_to_jump_s']:.2f}s!", flush=True)

                    jump_amt = round(bid - limit_px, 4)
                    if jump_amt > ord_data["max_jump_cents"]:
                        ord_data["max_jump_cents"] = jump_amt

                # 2. Did any trade print <= limit price?
                if 0.0 < trd_px <= limit_px:
                    # Check if new trade sequence
                    trd_key = (trd_px, trd_sz, snap.get("sip_timestamp_ns"))
                    if trd_key not in ord_data["seen_trades"]:
                        ord_data["seen_trades"].add(trd_key)
                        ord_data["qualifying_trades"].append({
                            "px": trd_px, "sz": trd_sz,
                            "t_offset_s": round(now - ord_data["t_place"], 2)
                        })
                        print(f"⚡ [TRADE <= LIMIT] {oid} {sym} | Trd: {trd_sz}sh @ ${trd_px:.2f} (Limit: ${limit_px:.2f})", flush=True)

                # Record trajectory sample (up to 200 samples per order)
                if len(ord_data["samples"]) < 200:
                    ord_data["samples"].append({
                        "t_offset_s": round(now - ord_data["t_place"], 2),
                        "bid": bid, "ask": ask, "mid": mid, "spread": spr,
                        "trd_px": trd_px, "trd_sz": trd_sz
                    })

                # Periodic resting tick log (every ~1s or on big move)
                last_t = last_log.get(oid, 0)
                if now - last_t >= 1.0:
                    last_log[oid] = now
                    resting_s = round(now - ord_data["t_place"], 1)
                    jump_str = f"JUMPED (+{round(bid - limit_px, 2):.2f}$)" if bid > limit_px else "RESTING_AT_OR_BELOW"
                    print(f"  [{resting_s:4.1f}s] {oid} {sym} | Limit: ${limit_px:.2f} | Bid: ${bid:.2f} ({jump_str}) | Ask: ${ask:.2f} | Mid: ${mid:.2f} | Trd: ${trd_px:.2f}", flush=True)

    def tail_remote_orders(self):
        """Tail ccm-live-1 engine_orders JSONL stream via SSH."""
        cmd = [
            "ssh", "-o", "ConnectTimeout=5", "-i", SSH_KEY, f"benwu@{self.vm_host}",
            "tail -n 20 -F $(ls -t /opt/ccm/src/output/live/*/engine_orders_*.jsonl | head -n 1)"
        ]
        print(f"[*] Connecting to remote orders feed on {self.vm_host}...")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue

                evt = d.get("evt")
                if evt != "order":
                    continue

                status = d.get("status")
                oid = d.get("order_id", "")
                role = d.get("role", "")

                if status in ("placed", "held") and role == "entry" and oid.startswith("B"):
                    with self.lock:
                        if oid not in self.active_orders:
                            sym = d.get("symbol")
                            mid = float(d.get("mid", 0.0))
                            spread = float(d.get("spread", 0.0))
                            lmt = d.get("limit_px")
                            lmt_val = float(lmt) if (lmt is not None and str(lmt) != "nan") else None

                            self.active_orders[oid] = {
                                "order_id": oid,
                                "ibkr_id": d.get("ibkr_id"),
                                "symbol": sym,
                                "qty": d.get("qty"),
                                "mid_at_place": mid,
                                "spread_at_place": spread,
                                "limit_px": lmt_val,
                                "t_place": time.time(),
                                "samples_count": 0,
                                "max_bid": 0.0,
                                "min_bid": 0.0,
                                "max_ask": 0.0,
                                "min_ask": 0.0,
                                "max_mid": 0.0,
                                "min_mid": 0.0,
                                "bid_jumped": False,
                                "max_jump_cents": 0.0,
                                "first_jump_s": None,
                                "first_jump_bid": None,
                                "seen_trades": set(),
                                "qualifying_trades": [],
                                "samples": [],
                            }
                            print(f"\n================================================================================")
                            print(f"[*] NEW ENTRY ORDER: {oid} {sym} (Qty: {d.get('qty')}) | Mid: ${mid:.2f} | Spread: ${spread:.2f}")
                            if lmt_val:
                                print(f"    Limit Price: ${lmt_val:.2f}")
                            print(f"================================================================================", flush=True)

                        elif d.get("limit_px") is not None and str(d.get("limit_px")) != "nan":
                            self.active_orders[oid]["limit_px"] = float(d.get("limit_px"))
                            print(f"[*] {oid} Limit Price Updated -> ${float(d.get('limit_px')):.2f}", flush=True)

                elif status in ("cancelled", "filled", "expired") and oid in self.active_orders:
                    with self.lock:
                        ord_data = self.active_orders.pop(oid, None)

                    if ord_data:
                        self._report_completed_order(ord_data, status, d)
        finally:
            proc.terminate()

    def _report_completed_order(self, ord_data, status, last_event):
        oid = ord_data["order_id"]
        sym = ord_data["symbol"]
        limit_px = ord_data.get("limit_px", 0.0)
        dur = round(time.time() - ord_data["t_place"], 2)

        # Final quote snapshot from SHM
        snap = self.reader.snapshot(sym) or {}
        end_bid = float(snap.get("bid_px", 0.0))
        end_ask = float(snap.get("ask_px", 0.0))
        end_mid = float(snap.get("midprice", 0.0))

        jump_verdict = "YES" if ord_data["bid_jumped"] else "NO"
        jump_detail = (f"(Peak Bid: ${ord_data['max_bid']:.2f}, +{ord_data['max_jump_cents']:.2f}c above limit, first jump at {ord_data['first_jump_s']}s)"
                       if ord_data["bid_jumped"] else f"(Max Bid: ${ord_data['max_bid']:.2f} stayed <= limit)")

        print(f"\n--------------------------------------------------------------------------------")
        print(f"[ORDER {status.upper()}] {oid} {sym} | Limit: ${limit_px:.2f} | Resting Duration: {dur:.1f}s")
        print(f"  • DID BID JUMP?: {jump_verdict} {jump_detail}")
        print(f"  • TRADES <= LIMIT: {len(ord_data['qualifying_trades'])} trades executed at or below limit")
        for qtrd in ord_data["qualifying_trades"][:5]:
            print(f"      - {qtrd['sz']} sh @ ${qtrd['px']:.2f} (+{qtrd['t_offset_s']}s)")
        print(f"  • PLACEMENT QUOTE: Mid: ${ord_data['mid_at_place']:.2f} | Spread: ${ord_data['spread_at_place']:.2f}")
        print(f"  • FINAL QUOTE:     Bid: ${end_bid:.2f} | Ask: ${end_ask:.2f} | Mid: ${end_mid:.2f}")
        if ord_data["mid_at_place"] > 0 and end_mid > 0:
            mid_delta_bps = (end_mid - ord_data["mid_at_place"]) / ord_data["mid_at_place"] * 10000
            print(f"  • MID DRIFT:       {mid_delta_bps:+.1f} bps (Range: ${ord_data['min_mid']:.2f} - ${ord_data['max_mid']:.2f})")
        print(f"--------------------------------------------------------------------------------\n", flush=True)

        # Write to JSONL
        record = {
            "order_id": oid,
            "symbol": sym,
            "qty": ord_data["qty"],
            "limit_px": limit_px,
            "resting_duration_s": dur,
            "final_status": status,
            "bid_jumped": ord_data["bid_jumped"],
            "max_jump_cents": ord_data["max_jump_cents"],
            "max_bid": ord_data["max_bid"],
            "first_jump_s": ord_data["first_jump_s"],
            "qualifying_trades_count": len(ord_data["qualifying_trades"]),
            "qualifying_trades": ord_data["qualifying_trades"],
            "mid_at_place": ord_data["mid_at_place"],
            "final_mid": end_mid,
            "samples": ord_data["samples"],
            "ts": time.time(),
        }
        with open(self.log_path, "a") as f:
            f.write(json.dumps(record) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Watch resting orders against replicated TickHub SHM")
    parser.add_argument("--shm", default=DEFAULT_SHM, help="Path to local replicated SHM")
    parser.add_argument("--vm", default=VM_HOST, help="Remote VM host IP")
    parser.add_argument("--log", default="/mnt/wc/tickhub/output/resting_orders.jsonl", help="Output JSONL log path")
    args = parser.parse_args()

    print(f"[*] Starting RestingOrderWatcher...")
    print(f"    SHM: {args.shm}")
    print(f"    VM:  {args.vm}")
    print(f"    Log: {args.log}")

    watcher = RestingOrderWatcher(shm_path=args.shm, vm_host=args.vm, log_path=args.log)
    watcher.start_polling_thread()
    watcher.tail_remote_orders()


if __name__ == "__main__":
    main()
