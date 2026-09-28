#!/usr/bin/env python3
"""
flatfile_ws_server.py — Real-time 1x Massive.com WebSocket playback server.

Plays back a pre-filtered Massive flat file (quotes + trades) in uncompressed 1x real-time.
Fast-forwards to virtual "now" (wall_clock + time_offset_s) upon client subscription,
dropping older rows so the connected client immediately receives a live feed.

Wire Protocol:
  - Auth: {"action": "auth", "params": "..."} -> [{"ev": "status", "status": "auth_success"}]
  - Sub:  {"action": "subscribe", "params": "..."} -> [{"ev": "status", "status": "success"}]
  - Feed: JSON arrays of {"ev": "Q", ...} and {"ev": "T", ...} events.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import gzip
import json
import logging
import time
from pathlib import Path
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("flatfile_ws")


class FlatFileWSServer:
    def __init__(
        self,
        flatfile_path: str | Path,
        time_offset_s: float = 0.0,
        speed: float = 1.0,
        host: str = "0.0.0.0",
        port: int = 8765,
        virtual_start_ns: int | None = None,
    ):
        self.flatfile_path = Path(flatfile_path)
        self.time_offset_s = float(time_offset_s)
        self.speed = float(speed)
        self.host = host
        self.port = int(port)
        self.virtual_start_ns = virtual_start_ns
        self.running = True

        if not self.flatfile_path.exists():
            raise FileNotFoundError(f"Flatfile not found: {self.flatfile_path}")

    async def handle_connection(self, websocket):
        client_addr = websocket.remote_address
        logger.info(f"[WS-SERVER] Client connected from {client_addr}")

        authenticated = False
        subscribed = False
        target_symbols: set[str] | None = None

        # 1. Handshake Phase
        try:
            async for raw_msg in websocket:
                try:
                    payload = json.loads(raw_msg)
                except Exception as e:
                    logger.warning(f"[WS-SERVER] Invalid JSON received: {e}")
                    continue

                action = payload.get("action")
                if action == "auth":
                    authenticated = True
                    resp = [{"ev": "status", "status": "auth_success", "message": "authenticated"}]
                    await websocket.send(json.dumps(resp))
                    logger.info("[WS-SERVER] Client authenticated successfully")
                elif action == "subscribe":
                    if not authenticated:
                        err = [{"ev": "status", "status": "auth_failed", "message": "unauthenticated"}]
                        await websocket.send(json.dumps(err))
                        continue

                    params = payload.get("params", "")
                    if params:
                        # Extract subscribed symbols, e.g. "Q.AAPL,T.AAPL,Q.MSFT" -> {"AAPL", "MSFT"}
                        syms = set()
                        wildcard = False
                        for p in params.split(","):
                            p = p.strip()
                            if p == "*" or p.endswith(".*"):
                                wildcard = True
                                break
                            if "." in p:
                                syms.add(p.split(".", 1)[1])
                            elif p:
                                syms.add(p)
                        if not wildcard and syms:
                            target_symbols = syms
                            logger.info(f"[WS-SERVER] Subscribed to {len(syms)} specific symbols: {sorted(syms)[:10]}...")
                        else:
                            target_symbols = None
                            logger.info("[WS-SERVER] Subscribed to all symbols in flat file")

                    subscribed = True
                    resp = [{"ev": "status", "status": "success", "message": "subscribed"}]
                    await websocket.send(json.dumps(resp))
                    logger.info("[WS-SERVER] Client subscription active. Beginning playback stream...")
                    break
        except websockets.exceptions.ConnectionClosed:
            logger.info("[WS-SERVER] Client disconnected during handshake")
            return

        if not subscribed:
            return

        # 2. Fast-Forward and Streaming Phase
        open_fn = gzip.open if str(self.flatfile_path).endswith(".gz") else open
        stream_start_wall = time.time()
        if self.virtual_start_ns is not None:
            virtual_start_ns = int(self.virtual_start_ns)
        else:
            virtual_start_ns = int((stream_start_wall + self.time_offset_s) * 1e9)
        v_start_iso = datetime.datetime.fromtimestamp(
            virtual_start_ns / 1e9, tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

        logger.info(
            f"[WS-SERVER] Replaying at {self.speed:.2f}x real-time (offset: {self.time_offset_s:.1f}s, "
            f"virtual_start: {v_start_iso})"
        )

        dropped_count = 0
        emitted_count = 0
        last_telemetry = time.time()
        target_wall_time = stream_start_wall

        batch = []
        batch_flush_interval = 0.010  # 10ms batch window

        try:
            with open_fn(self.flatfile_path, "rt", encoding="utf-8") as f:
                header_line = f.readline()  # skip header
                t_seek_0 = time.time()

                # Fast-forward past past events
                for line in f:
                    parts = line.strip().split(",")
                    if len(parts) < 9:
                        continue
                    try:
                        t_ns = int(parts[2])
                    except ValueError:
                        continue

                    if t_ns < virtual_start_ns:
                        dropped_count += 1
                        continue

                    # First in-window record reached
                    seek_dt = time.time() - t_seek_0
                    logger.info(
                        f"[WS-SERVER] Fast-forwarded to virtual_start_ns={virtual_start_ns} "
                        f"(dropped {dropped_count:,} rows in {seek_dt:.2f}s)"
                    )
                    break
                else:
                    logger.warning("[WS-SERVER] Reached EOF during fast-forward (all rows prior to virtual start)")
                    return

                # Paced real-time streaming loop
                while line:
                    parts = line.strip().split(",")
                    if len(parts) >= 9:
                        ev_type = parts[0]
                        sym = parts[1]

                        if target_symbols is None or sym in target_symbols:
                            try:
                                t_ns = int(parts[2])
                                event_vtime_s = t_ns / 1e9
                                target_wall_time = event_vtime_s - self.time_offset_s
                                now = time.time()
                                sleep_duration = (target_wall_time - now) / self.speed

                                if sleep_duration > 0.0005:
                                    if batch:
                                        await websocket.send(json.dumps(batch))
                                        batch = []
                                    await asyncio.sleep(sleep_duration)

                                if ev_type == "Q":
                                    event = {
                                        "ev": "Q",
                                        "sym": sym,
                                        "bp": float(parts[3]),
                                        "bs": float(parts[4]),
                                        "ap": float(parts[5]),
                                        "as": float(parts[6]),
                                        "t": t_ns // 1_000_000,
                                    }
                                    batch.append(event)
                                    emitted_count += 1
                                elif ev_type == "T":
                                    event = {
                                        "ev": "T",
                                        "sym": sym,
                                        "p": float(parts[7]),
                                        "s": float(parts[8]),
                                        "t": t_ns // 1_000_000,
                                    }
                                    batch.append(event)
                                    emitted_count += 1

                                if len(batch) >= 50:
                                    await websocket.send(json.dumps(batch))
                                    batch = []

                            except (ValueError, IndexError):
                                pass

                    now = time.time()
                    if now - last_telemetry >= 10.0:
                        elapsed = now - stream_start_wall
                        rate = emitted_count / elapsed if elapsed > 0 else 0
                        lag_ms = max(0.0, (now - target_wall_time) * 1000.0)
                        iso_vtime = datetime.datetime.fromtimestamp(
                            t_ns / 1e9, tz=datetime.timezone.utc
                        ).strftime("%H:%M:%S UTC")
                        logger.info(
                            f"[WS-SERVER] emitted={emitted_count:,} rate={rate:.1f}/s "
                            f"lag_ms={lag_ms:.1f} current_vtime={iso_vtime}"
                        )
                        last_telemetry = now

                    line = f.readline()

                if batch:
                    await websocket.send(json.dumps(batch))

            logger.info(f"[WS-SERVER] Playback completed. Emitted {emitted_count:,} total events.")
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"[WS-SERVER] Client disconnected after {emitted_count:,} emitted events.")
        except Exception as e:
            logger.error(f"[WS-SERVER] Playback error: {e}", exc_info=True)

    async def run(self):
        logger.info(f"[WS-SERVER] Starting WebSocket server on ws://{self.host}:{self.port}...")
        async with websockets.serve(self.handle_connection, self.host, self.port):
            await asyncio.Future()  # run forever


def main():
    parser = argparse.ArgumentParser(description="Massive.com flat file real-time WebSocket playback server.")
    parser.add_argument("--flatfile", required=True, help="Path to filtered flat file (.csv or .csv.gz)")
    parser.add_argument("--time-offset-s", type=float, default=0.0, help="Time offset in seconds (virtual - wall)")
    parser.add_argument("--speed", type=float, default=1.0, help="Playback speed multiplier (default: 1.0)")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8765, help="Port to listen on (default: 8765)")
    parser.add_argument("--virtual-start-ns", type=int, default=None, help="Explicit virtual start nanoseconds")
    args = parser.parse_args()

    server = FlatFileWSServer(
        flatfile_path=args.flatfile,
        time_offset_s=args.time_offset_s,
        speed=args.speed,
        host=args.host,
        port=args.port,
        virtual_start_ns=args.virtual_start_ns,
    )
    asyncio.run(server.run())


if __name__ == "__main__":
    main()
