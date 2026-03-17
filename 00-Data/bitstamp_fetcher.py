"""
Bitstamp OHLC Data Fetcher
Fetches BTC/USD 1-minute candle data from the Bitstamp API
from the earliest available data (2011-08-18) to today.

API endpoint: https://www.bitstamp.net/api/v2/ohlc/btcusd
Parameters:
    step   - candle interval in seconds (60 = 1 minute)
    limit  - max candles per request (max 1000)
    start  - unix timestamp to start from

Usage:
    python bitstamp_fetcher.py                    # fetch all missing data
    python bitstamp_fetcher.py --from-scratch      # re-fetch everything
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import json

# ── Config ──────────────────────────────────────────────────────────────────

API_URL = "https://www.bitstamp.net/api/v2/ohlc/btcusd"
STEP = 60          # 1-minute candles
LIMIT = 1000       # max per request
OUTPUT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = OUTPUT_DIR / "BTCUSD_1m_Bitstamp.csv"

# Earliest known Bitstamp BTC/USD data
EARLIEST_TIMESTAMP = 1313625600  # 2011-08-18 00:00:00 UTC

# Rate limiting
REQUEST_DELAY = 0.25  # seconds between requests
MAX_RETRIES = 5
RETRY_BACKOFF = 2     # exponential backoff multiplier

CSV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


# ── API ─────────────────────────────────────────────────────────────────────

def fetch_batch(start_ts: int) -> list[dict]:
    """Fetch one batch of up to 1000 candles starting from start_ts."""
    url = f"{API_URL}?step={STEP}&limit={LIMIT}&start={start_ts}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(url, headers={"User-Agent": "bitstamp-fetcher/1.0"})
            with urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            return data.get("data", {}).get("ohlc", [])
        except (URLError, HTTPError, json.JSONDecodeError, TimeoutError) as e:
            if attempt == MAX_RETRIES:
                print(f"\n  FAILED after {MAX_RETRIES} attempts: {e}")
                raise
            wait = RETRY_BACKOFF ** attempt
            print(f"\n  Retry {attempt}/{MAX_RETRIES} in {wait}s ({e})")
            time.sleep(wait)


# ── Resume logic ────────────────────────────────────────────────────────────

def get_last_timestamp(filepath: Path) -> int | None:
    """Read the last timestamp from an existing CSV to enable resuming."""
    if not filepath.exists():
        return None
    last_ts = None
    with open(filepath, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            last_ts = int(row["timestamp"])
    return last_ts


# ── Main ────────────────────────────────────────────────────────────────────

def run(from_scratch: bool = False):
    now_ts = int(datetime.now(timezone.utc).timestamp())

    # Determine start point
    if from_scratch or not OUTPUT_FILE.exists():
        start_ts = EARLIEST_TIMESTAMP
        mode = "w"
        print(f"Fetching from scratch: {datetime.fromtimestamp(start_ts, tz=timezone.utc).date()}")
    else:
        last_ts = get_last_timestamp(OUTPUT_FILE)
        if last_ts is None:
            start_ts = EARLIEST_TIMESTAMP
            mode = "w"
            print(f"Empty CSV, fetching from scratch: {datetime.fromtimestamp(start_ts, tz=timezone.utc).date()}")
        else:
            start_ts = last_ts + STEP  # next candle after last saved
            mode = "a"
            print(f"Resuming from: {datetime.fromtimestamp(start_ts, tz=timezone.utc)}")

    if start_ts >= now_ts:
        print("Already up to date.")
        return

    # Estimate total batches for progress
    total_candles = (now_ts - start_ts) // STEP
    total_batches = (total_candles // LIMIT) + 1

    write_header = (mode == "w")
    total_rows = 0
    batch_num = 0
    current_ts = start_ts

    with open(OUTPUT_FILE, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()

        while current_ts < now_ts:
            batch_num += 1
            candles = fetch_batch(current_ts)

            if not candles:
                break

            # Filter: skip candles we already have and future candles
            rows = []
            for c in candles:
                ts = int(c["timestamp"])
                if ts < start_ts:
                    continue
                if ts > now_ts:
                    break
                rows.append({
                    "timestamp": ts,
                    "open": c["open"],
                    "high": c["high"],
                    "low": c["low"],
                    "close": c["close"],
                    "volume": c["volume"],
                })

            if rows:
                writer.writerows(rows)
                f.flush()
                total_rows += len(rows)

            # Progress
            last_ts_in_batch = int(candles[-1]["timestamp"])
            last_date = datetime.fromtimestamp(last_ts_in_batch, tz=timezone.utc)
            pct = min(100, (batch_num / total_batches) * 100)
            sys.stdout.write(
                f"\r  Batch {batch_num}/{total_batches} "
                f"({pct:.1f}%) | {last_date.date()} | "
                f"{total_rows:,} rows saved"
            )
            sys.stdout.flush()

            # Advance: start from the candle after the last one received
            # If the batch was sparse (gaps in early data), jump forward by
            # the full window size so we don't get stuck in empty ranges.
            expected_end = current_ts + (LIMIT * STEP)
            current_ts = max(last_ts_in_batch + STEP, expected_end)

            time.sleep(REQUEST_DELAY)

    print(f"\n\nDone. {total_rows:,} new rows written to {OUTPUT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Bitstamp BTC/USD 1m OHLC data")
    parser.add_argument("--from-scratch", action="store_true", help="Re-fetch all data from 2011")
    args = parser.parse_args()
    run(from_scratch=args.from_scratch)
