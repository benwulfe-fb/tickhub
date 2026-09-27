#!/usr/bin/env python3
"""
test_flatfile_ws_server.py — Integration test for flat file filter and real-time WS server.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import websockets
import yaml

from scripts.filter_massive_flatfile import merge_and_filter
from scripts.flatfile_ws_server import FlatFileWSServer


class TestFlatFileWSServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.dir_path = Path(self.test_dir)

        # 1. Config with 2 symbols
        self.config_path = self.dir_path / "test_config.yaml"
        with open(self.config_path, "w") as f:
            yaml.safe_dump({"unique_symbols": ["AAPL", "MSFT"]}, f)

        # Base timestamp: 1790345000000000000 (Friday ~13:36 UTC)
        base_ts = 1790345000000000000
        self.base_ts = base_ts

        # 2. Raw trades file (includes UNWANTED symbol GOOG to test filtering)
        self.raw_trades_path = self.dir_path / "trades.csv.gz"
        with gzip.open(self.raw_trades_path, "wt") as f:
            f.write("ticker,conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,trf_id,trf_timestamp\n")
            # Tie at base_ts + 1_000_000_000
            f.write(f"AAPL,12,0,4,101,{base_ts},150.25,1,{base_ts + 1_000_000_000},10.0,1,0,0\n")
            f.write(f"GOOG,12,0,4,102,{base_ts},2800.0,2,{base_ts + 1_500_000_000},5.0,1,0,0\n")
            f.write(f"MSFT,12,0,4,103,{base_ts},310.50,3,{base_ts + 2_000_000_000},20.0,1,0,0\n")

        # 3. Raw quotes file
        self.raw_quotes_path = self.dir_path / "quotes.csv.gz"
        with gzip.open(self.raw_quotes_path, "wt") as f:
            f.write("ticker,ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,conditions,indicators,participant_timestamp,sequence_number,sip_timestamp,tape,trf_timestamp\n")
            # Tie at base_ts + 1_000_000_000 with AAPL trade
            f.write(f"AAPL,4,150.30,100.0,4,150.20,200.0,1,1,{base_ts},10,{base_ts + 1_000_000_000},1,0\n")
            f.write(f"MSFT,4,310.60,50.0,4,310.40,60.0,1,1,{base_ts},11,{base_ts + 1_800_000_000},1,0\n")

        self.filtered_path = self.dir_path / "filtered.csv.gz"

    async def asyncTearDown(self):
        shutil.rmtree(self.test_dir)

    async def test_filter_ordering_and_universe_pruning(self):
        merge_and_filter(
            trades_path=self.raw_trades_path,
            quotes_path=self.raw_quotes_path,
            config_path=self.config_path,
            output_path=self.filtered_path,
        )

        with gzip.open(self.filtered_path, "rt") as f:
            lines = [l.strip() for l in f if l.strip()]

        # Header + 4 records (GOOG filtered out)
        self.assertEqual(len(lines), 5)
        self.assertTrue(lines[0].startswith("event_type,symbol"))

        records = [line.split(",") for line in lines[1:]]
        # Check Quote-before-Trade tie-breaking at base_ts + 1_000_000_000
        rec0 = records[0]  # Q, AAPL, base_ts + 1_000_000_000
        rec1 = records[1]  # T, AAPL, base_ts + 1_000_000_000
        self.assertEqual(rec0[0], "Q")
        self.assertEqual(rec0[1], "AAPL")
        self.assertEqual(int(rec0[2]), self.base_ts + 1_000_000_000)

        self.assertEqual(rec1[0], "T")
        self.assertEqual(rec1[1], "AAPL")
        self.assertEqual(int(rec1[2]), self.base_ts + 1_000_000_000)

        # Check subsequent records in chronological order
        rec2 = records[2]  # Q, MSFT, base_ts + 1_800_000_000
        self.assertEqual(rec2[0], "Q")
        self.assertEqual(rec2[1], "MSFT")

        rec3 = records[3]  # T, MSFT, base_ts + 2_000_000_000
        self.assertEqual(rec3[0], "T")
        self.assertEqual(rec3[1], "MSFT")

    async def test_ws_server_handshake_and_pacing(self):
        # 1. Prepare filtered file
        merge_and_filter(
            trades_path=self.raw_trades_path,
            quotes_path=self.raw_quotes_path,
            config_path=self.config_path,
            output_path=self.filtered_path,
        )

        # Target virtual start to be before the first event:
        # offset_s = (base_ts / 1e9) - wall_time
        offset_s = (self.base_ts / 1e9) - time.time()

        server = FlatFileWSServer(
            flatfile_path=self.filtered_path,
            time_offset_s=offset_s,
            speed=10.0,  # 10x speed for fast test execution
            host="127.0.0.1",
            port=0,  # ephemeral port
        )

        server_task = None
        listener = await websockets.serve(server.handle_connection, "127.0.0.1", 0)
        port = listener.sockets[0].getsockname()[1]

        try:
            uri = f"ws://127.0.0.1:{port}"
            async with websockets.connect(uri) as ws:
                # Handshake: auth
                await ws.send(json.dumps({"action": "auth", "params": "test_key"}))
                auth_resp = json.loads(await ws.recv())
                self.assertEqual(auth_resp[0]["status"], "auth_success")

                # Handshake: subscribe
                t0 = time.time()
                await ws.send(json.dumps({"action": "subscribe", "params": "Q.AAPL,T.AAPL,Q.MSFT,T.MSFT"}))
                sub_resp = json.loads(await ws.recv())
                self.assertEqual(sub_resp[0]["status"], "success")

                # Stream records
                received_events = []
                while len(received_events) < 4:
                    msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    events = json.loads(msg)
                    received_events.extend(events)

                elapsed = time.time() - t0
                # 4 records span 1.0 second (from +1.0s to +2.0s). At 10x speed, playback takes ~0.1s +/- 0.15s
                self.assertLess(elapsed, 0.6)
                self.assertEqual(len(received_events), 4)

                self.assertEqual(received_events[0]["ev"], "Q")
                self.assertEqual(received_events[0]["sym"], "AAPL")
                self.assertEqual(received_events[1]["ev"], "T")
                self.assertEqual(received_events[1]["sym"], "AAPL")
                self.assertEqual(received_events[2]["ev"], "Q")
                self.assertEqual(received_events[2]["sym"], "MSFT")
                self.assertEqual(received_events[3]["ev"], "T")
                self.assertEqual(received_events[3]["sym"], "MSFT")

        finally:
            listener.close()
            await listener.wait_closed()


if __name__ == "__main__":
    unittest.main()
