"""
Fetch BTC price and prediction data from Polymarket's BTC Up/Down 5m markets.

Polymarket BTC Up/Down 5m series (docs.polymarket.com):
- Event slug pattern: btc-updown-5m-{unix_timestamp}
  where timestamp is aligned to 5-minute boundaries (divisible by 300)
- Gamma API: GET /events?slug=btc-updown-5m-{ts}  -> event with market odds
- CLOB API:  GET /prices-history?market=<token_id>&interval=max&fidelity=60
             GET /midpoint?token_id=<token_id>
             GET /book?token_id=<token_id>
- Resolution source: Chainlink BTC/USD data stream
  (https://data.chain.link/streams/btc-usd)
- Outcomes: ["Up", "Down"] — resolves "Up" if close >= open for that 5-min window

Binance API (for OHLCV training data):
- GET /api/v3/klines?symbol=BTCUSDT&interval=5m&limit=1000
"""

import json
import time
import threading
import requests
import websocket
import pandas as pd
import numpy as np
import os

import config

WINDOW_SECONDS = 300  # 5 minutes


# ─── Chainlink BTC/USD via Polymarket RTDS WebSocket ────────────────────────

def fetch_chainlink_btc_price(timeout=10):
    """
    Get the current Chainlink BTC/USD price from Polymarket's RTDS WebSocket.
    This is the exact price Polymarket uses for BTC Up/Down 5m resolution.
    """
    result = {}
    done = threading.Event()

    def on_open(ws):
        ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [{
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": "",
            }]
        }))

    def on_message(ws, message):
        data = json.loads(message)
        if (data.get("topic") == "crypto_prices_chainlink"
                and data.get("payload", {}).get("symbol") == "btc/usd"):
            result["price"] = data["payload"]["value"]
            result["timestamp"] = data["payload"]["timestamp"]
            done.set()
            ws.close()

    def on_error(ws, error):
        pass  # Ignore non-fatal errors (e.g. empty pings)

    def on_close(ws, code, msg):
        done.set()

    ws = websocket.WebSocketApp(
        "wss://ws-live-data.polymarket.com",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    t = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 5})
    t.daemon = True
    t.start()

    done.wait(timeout=timeout)
    try:
        ws.close()
    except Exception:
        pass

    return result.get("price")


# ─── Polymarket BTC Up/Down 5m ──────────────────────────────────────────────

def get_current_window_timestamp():
    """Get the unix timestamp for the current 5-minute window (aligned to 300s)."""
    now = int(time.time())
    return (now // WINDOW_SECONDS) * WINDOW_SECONDS


def get_next_window_timestamp():
    """Get the unix timestamp for the next 5-minute window."""
    return get_current_window_timestamp() + WINDOW_SECONDS


def fetch_polymarket_5m_event(window_ts=None):
    """
    Fetch a specific BTC Up/Down 5m event from Polymarket Gamma API.
    Slug pattern: btc-updown-5m-{unix_timestamp}

    Returns dict with market data or None if not found.
    """
    if window_ts is None:
        window_ts = get_current_window_timestamp()

    slug = f"btc-updown-5m-{window_ts}"
    url = f"{config.POLYMARKET_GAMMA}/events"
    params = {"slug": slug}

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        print(f"[WARN] Polymarket event fetch failed for {slug}: {e}")
        return None

    if not events:
        return None

    event = events[0]
    market = event["markets"][0]

    outcome_prices = json.loads(market.get("outcomePrices", "[]")) if market.get("outcomePrices") else []
    clob_token_ids = json.loads(market.get("clobTokenIds", "[]")) if market.get("clobTokenIds") else []

    up_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0.5
    down_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.5

    # Extract Chainlink "Price to beat" from event metadata
    price_to_beat = None
    event_metadata = event.get("eventMetadata")
    if event_metadata:
        if isinstance(event_metadata, str):
            try:
                event_metadata = json.loads(event_metadata)
            except (json.JSONDecodeError, TypeError):
                event_metadata = {}
        if isinstance(event_metadata, dict):
            ptb = event_metadata.get("priceToBeat")
            if ptb is not None:
                price_to_beat = float(ptb)

    return {
        "slug": event["slug"],
        "title": event["title"],
        "window_ts": window_ts,
        "end_date": event.get("endDate"),
        "up_price": up_price,
        "down_price": down_price,
        "price_to_beat": price_to_beat,
        "volume": float(event.get("volume", 0) or 0),
        "liquidity": float(event.get("liquidity", 0) or 0),
        "active": event.get("active", False),
        "closed": event.get("closed", False),
        "token_id_up": clob_token_ids[0] if len(clob_token_ids) > 0 else None,
        "token_id_down": clob_token_ids[1] if len(clob_token_ids) > 1 else None,
        "condition_id": market.get("conditionId"),
    }


def fetch_polymarket_5m_current():
    """
    Fetch the current active BTC Up/Down 5m market.
    Tries current window first, then next window if current is closed.
    """
    current_ts = get_current_window_timestamp()
    event = fetch_polymarket_5m_event(current_ts)

    if event and event["active"] and not event["closed"]:
        return event

    # Try next window
    next_ts = get_next_window_timestamp()
    event = fetch_polymarket_5m_event(next_ts)
    if event:
        return event

    # Try previous window (might still be resolving)
    prev_ts = current_ts - WINDOW_SECONDS
    return fetch_polymarket_5m_event(prev_ts)


def fetch_polymarket_5m_history(n_windows=100):
    """
    Fetch historical BTC Up/Down 5m market data by iterating past windows.
    Returns a DataFrame with one row per 5-minute window.
    """
    current_ts = get_current_window_timestamp()
    records = []

    print(f"Fetching {n_windows} historical Polymarket 5m windows...")
    for i in range(n_windows):
        ts = current_ts - (i * WINDOW_SECONDS)
        event = fetch_polymarket_5m_event(ts)

        if event:
            records.append({
                "window_ts": ts,
                "window_time": pd.Timestamp(ts, unit="s"),
                "poly_up_price": event["up_price"],
                "poly_down_price": event["down_price"],
                "poly_volume": event["volume"],
                "poly_liquidity": event["liquidity"],
                "poly_closed": event["closed"],
            })

        if (i + 1) % 20 == 0:
            print(f"  Fetched {i + 1}/{n_windows} windows...")
            time.sleep(0.5)  # Respect rate limits
        else:
            time.sleep(0.05)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df.set_index("window_time", inplace=True)
    df.sort_index(inplace=True)
    print(f"Got {len(df)} Polymarket 5m windows")
    return df


def fetch_polymarket_orderbook(token_id):
    """
    Fetch CLOB order book for a Polymarket token.
    CLOB API: GET /book?token_id=<token_id>
    Returns bids, asks, and last trade price.
    """
    url = f"{config.POLYMARKET_CLOB}/book"
    params = {"token_id": token_id}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return {
            "last_trade_price": float(data.get("last_trade_price", 0)),
            "bids": data.get("bids", []),
            "asks": data.get("asks", []),
            "bid_depth": sum(float(b["size"]) for b in data.get("bids", [])),
            "ask_depth": sum(float(a["size"]) for a in data.get("asks", [])),
        }
    except Exception as e:
        print(f"[WARN] Orderbook fetch failed: {e}")
        return None


def fetch_polymarket_midpoint(token_id):
    """
    Fetch midpoint price for a Polymarket token.
    CLOB API: GET /midpoint?token_id=<token_id>
    """
    url = f"{config.POLYMARKET_CLOB}/midpoint"
    params = {"token_id": token_id}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return float(data.get("mid", 0.5))
    except Exception as e:
        print(f"[WARN] Midpoint fetch failed: {e}")
        return 0.5


# ─── Binance OHLCV (supplementary price data) ───────────────────────────────

def fetch_binance_klines(symbol="BTCUSDT", interval=config.INTERVAL,
                         limit=config.CANDLE_LIMIT, start_time=None):
    """Fetch OHLCV candlestick data from Binance."""
    url = f"{config.BINANCE_BASE}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time:
        params["startTime"] = start_time

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    raw = resp.json()

    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])

    for col in ["open", "high", "low", "close", "volume", "quote_volume",
                "taker_buy_base", "taker_buy_quote"]:
        df[col] = df[col].astype(float)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df["trades"] = df["trades"].astype(int)
    df.drop(columns=["ignore"], inplace=True)
    df.set_index("open_time", inplace=True)
    return df


def fetch_binance_klines_extended(symbol="BTCUSDT", interval=config.INTERVAL,
                                  total_candles=5000):
    """Fetch more than 1000 candles by paginating backwards."""
    all_dfs = []
    end_time = None

    while total_candles > 0:
        batch = min(total_candles, 1000)
        url = f"{config.BINANCE_BASE}/klines"
        params = {"symbol": symbol, "interval": interval, "limit": batch}
        if end_time:
            params["endTime"] = end_time

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        if not raw:
            break

        df = pd.DataFrame(raw, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ])
        all_dfs.append(df)

        end_time = int(raw[0][0]) - 1
        total_candles -= len(raw)

        if len(raw) < batch:
            break
        time.sleep(0.1)

    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs[::-1], ignore_index=True)
    combined.drop_duplicates(subset=["open_time"], inplace=True)

    for col in ["open", "high", "low", "close", "volume", "quote_volume",
                "taker_buy_base", "taker_buy_quote"]:
        combined[col] = combined[col].astype(float)

    combined["open_time"] = pd.to_datetime(combined["open_time"], unit="ms")
    combined["close_time"] = pd.to_datetime(combined["close_time"], unit="ms")
    combined["trades"] = combined["trades"].astype(int)
    combined.drop(columns=["ignore"], inplace=True)
    combined.set_index("open_time", inplace=True)
    combined.sort_index(inplace=True)
    return combined


# ─── Combined data collection ───────────────────────────────────────────────

def collect_data(total_candles=5000):
    """
    Collect BTC OHLCV data from Binance and merge with Polymarket 5m odds.
    The Polymarket Up/Down prices act as market sentiment features.
    """
    os.makedirs("data", exist_ok=True)

    # 1. Fetch Binance OHLCV
    print(f"Fetching {total_candles} 5-min candles from Binance...")
    df = fetch_binance_klines_extended(total_candles=total_candles)
    print(f"Got {len(df)} candles from {df.index[0]} to {df.index[-1]}")

    # 2. Fetch current Polymarket BTC Up/Down 5m market
    print("\nFetching Polymarket BTC Up/Down 5m market...")
    current_market = fetch_polymarket_5m_current()

    if current_market:
        print(f"  Active market: {current_market['title']}")
        print(f"  Up: {current_market['up_price']:.3f} | Down: {current_market['down_price']:.3f}")
        print(f"  Volume: ${current_market['volume']:,.2f}")
        print(f"  Liquidity: ${current_market['liquidity']:,.2f}")

        # Add current market odds as features
        df["poly_up_price"] = current_market["up_price"]
        df["poly_down_price"] = current_market["down_price"]
        df["poly_volume"] = current_market["volume"]
        df["poly_liquidity"] = current_market["liquidity"]

        # Fetch order book depth for Up token
        if current_market["token_id_up"]:
            book = fetch_polymarket_orderbook(current_market["token_id_up"])
            if book:
                df["poly_bid_depth"] = book["bid_depth"]
                df["poly_ask_depth"] = book["ask_depth"]
                df["poly_last_trade"] = book["last_trade_price"]
                df["poly_depth_ratio"] = (
                    book["bid_depth"] / (book["ask_depth"] + 1e-10)
                )
                print(f"  Order book — Bid depth: {book['bid_depth']:.1f}, "
                      f"Ask depth: {book['ask_depth']:.1f}")

        # Midpoint from CLOB
        if current_market["token_id_up"]:
            midpoint = fetch_polymarket_midpoint(current_market["token_id_up"])
            df["poly_midpoint"] = midpoint
            print(f"  CLOB midpoint: {midpoint:.4f}")
    else:
        print("  [WARN] No active Polymarket 5m market found, using defaults")
        df["poly_up_price"] = 0.5
        df["poly_down_price"] = 0.5
        df["poly_volume"] = 0.0
        df["poly_liquidity"] = 0.0
        df["poly_bid_depth"] = 0.0
        df["poly_ask_depth"] = 0.0
        df["poly_last_trade"] = 0.5
        df["poly_depth_ratio"] = 1.0
        df["poly_midpoint"] = 0.5

    df.to_csv(config.DATA_CACHE)
    print(f"\nData cached to {config.DATA_CACHE}")
    return df


if __name__ == "__main__":
    df = collect_data()
    print(f"\nDataset shape: {df.shape}")
    print(df.tail())
