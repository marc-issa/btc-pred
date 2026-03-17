"""
Automated BTC Up/Down 5m predictor — live terminal dashboard.

Streams Chainlink BTC/USD price in real-time via Polymarket RTDS WebSocket.
Updates prediction every second. Tracks trade history and session P&L.
Persists all trades to SQLite database.

Usage:
    python auto.py
"""

import sys
import os
import time
import pickle
import threading
import json
import sqlite3
import numpy as np
from datetime import datetime

# Sound notification (Windows-only)
try:
    import winsound
except ImportError:
    winsound = None

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    os.system("")  # Enable ANSI escape codes on Windows
import pandas as pd
import lightgbm as lgb
import requests
import websocket

import random

import config
from data_collector import (
    fetch_binance_klines, fetch_polymarket_5m_current, fetch_polymarket_5m_event,
    fetch_polymarket_orderbook,
)
from bot_logging import get_logger
from notifications import send_alert
from features import (
    add_technical_indicators,
    add_multi_timeframe_features,
    add_volume_features,
    add_time_features,
    add_streak_features,
    add_lookback_summary_features,
)

# ─── Color codes ─────────────────────────────────────────────────────────────

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

# ─── Constants ───────────────────────────────────────────────────────────────

PRICE_BUFFER_SIZE = 600
TRADE_HISTORY_SHOW = 7
STARTING_BALANCE = 100.0
MIN_BET = 5.0   # Minimum stake per trade (defaults, overridden by DB config)
MAX_BET = 10.0  # Maximum stake per trade (defaults, overridden by DB config)
DB_PATH = "data/trades.db"

log = get_logger("auto")

# ─── Dynamic config cache ────────────────────────────────────────────────────

_config_cache = {}
_config_cache_time = 0
_CONFIG_CACHE_TTL = 2  # seconds


def get_config(key, default=None, cast=float):
    """Read a config value from bot_config table with 2-second cache.

    Args:
        key: Config key name
        default: Default value if key not found
        cast: Type to cast the value to (float, int, str)
    """
    global _config_cache, _config_cache_time
    now = time.time()
    if now - _config_cache_time > _CONFIG_CACHE_TTL:
        try:
            conn = sqlite3.connect(DB_PATH)
            rows = conn.execute("SELECT key, value FROM bot_config").fetchall()
            conn.close()
            _config_cache = {r[0]: r[1] for r in rows}
            _config_cache_time = now
        except Exception:
            pass  # Use stale cache on DB error

    raw = _config_cache.get(key)
    if raw is None:
        return default
    try:
        return cast(raw) if cast else raw
    except (ValueError, TypeError):
        return default

# ─── Shared state ────────────────────────────────────────────────────────────

chainlink_state = {
    "value": None,
    "buffer": [],
    "lock": threading.Lock(),
    "last_update": 0,
}

binance_1m_state = {
    "vol_ratio": None,
    "btc_volume_24h": None,
    "btc_5m_volume": None,
    "lock": threading.Lock(),
}

# Prediction feed for live dashboard
_pred_feed = []
_pred_feed_lock = threading.Lock()


def bg_poll_binance_1m():
    """Background thread: fetch Binance 1m klines every 30s for volume ratio + 24h volume."""
    while True:
        try:
            df = fetch_binance_klines(interval="1m", limit=5)
            if df is not None and len(df) >= 2:
                vols = df["volume"].values
                avg_prev = np.mean(vols[:-1]) if len(vols) > 1 else vols[0]
                current_vol = vols[-1]
                ratio = current_vol / (avg_prev + 1e-10)
                with binance_1m_state["lock"]:
                    binance_1m_state["vol_ratio"] = float(ratio)
            # Fetch current 5m candle quote volume (USDT, accumulates in real-time)
            try:
                df5 = fetch_binance_klines(interval="5m", limit=1)
                if df5 is not None and len(df5) >= 1:
                    with binance_1m_state["lock"]:
                        binance_1m_state["btc_5m_volume"] = float(df5["quote_volume"].values[-1])
            except Exception:
                pass
            # Fetch 24h ticker for volume display
            try:
                import requests as _req
                resp = _req.get("https://api.binance.com/api/v3/ticker/24hr",
                                params={"symbol": "BTCUSDT"}, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                    with binance_1m_state["lock"]:
                        binance_1m_state["btc_volume_24h"] = float(data.get("quoteVolume", 0))
            except Exception:
                pass
        except Exception:
            pass
        time.sleep(30)


# ─── SQLite Database ─────────────────────────────────────────────────────────

def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_ts INTEGER UNIQUE,
            time_str TEXT,
            price_to_beat REAL,
            close_price REAL,
            action TEXT,
            direction TEXT,
            confidence REAL,
            edge_val REAL,
            buy_price REAL,
            sell_price REAL,
            bet_size REAL,
            actual TEXT,
            won INTEGER,
            pnl REAL,
            balance_after REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_ts INTEGER,
            elapsed_s INTEGER,
            direction TEXT,
            confidence REAL,
            prob_up REAL,
            edge_val REAL,
            poly_up REAL,
            poly_down REAL,
            chainlink_price REAL,
            ptb REAL,
            traded INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # New tables for production features
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bot_config (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
    """)
    # Insert default config rows (ignore if already exist)
    defaults = {
        "starting_balance": "100",
        "stop_loss_balance": "40",
        "min_bet": "5",
        "max_bet": "10",
        "daily_loss_limit": "20",
        "max_position_pct": "10",
        "slippage_enabled": "1",
        "slippage_factor": "0.005",
        "telegram_bot_token": "",
        "telegram_chat_id": "",
        "telegram_alerts_enabled": "0",
        "drawdown_alert_pct": "15",
        "early_exit_profit_pct": "0.50",
        "early_exit_window_min": "60",
        "early_exit_window_max": "270",
        "stop_loss_pct": "0.75",
        "consecutive_loss_limit": "3",
        "momentum_atr_threshold": "1.5",
        "calibration_min_trades": "50",
        "flip_loss_pct": "0.30",
        "flip_min_remaining": "90",
        "poly_momentum_entry": "0.80",
        "poly_slam_entry": "0.90",
        "poly_slam_min_elapsed": "30",
        "poly_momentum_max_buy": "0.92",
        "market_agree_hold": "0.50",
        "market_disagree_sell": "0.25",
        "low_volume_threshold": "500",
        "high_volume_threshold": "2000",
        "conviction_hold_threshold": "0.60",
        # Entry time windows
        "entry_after": "90",
        "entry_before": "240",
        # Entry phase thresholds
        "phase1_max_elapsed": "120",
        "phase1_min_confidence": "0.70",
        "phase1_min_edge": "0.05",
        "phase2_max_elapsed": "180",
        "phase2_min_confidence": "0.55",
        "phase2_min_edge": "0.03",
        "phase3_min_confidence": "0.70",
        "phase3_min_edge": "0.05",
        # Market strategy confidence gates
        "slam_min_confidence": "0.50",
        "slam_strong_disagree": "0.65",
        "momentum_strong_disagree": "0.60",
        # Bet sizing weights
        "bet_conf_weight": "0.6",
        "bet_edge_weight": "0.4",
        "bet_conf_base": "0.55",
        "bet_conf_range": "0.20",
        "bet_edge_base": "0.03",
        "bet_edge_range": "0.12",
        # Flip thresholds
        "flip_min_edge": "0.05",
        "flip_min_confidence": "0.60",
        # Take profit tuning
        "take_profit_conviction_bonus": "0.20",
        "take_profit_max": "0.70",
        # Resolution
        "resolution_threshold": "0.90",
        # Dashboard auth
        "dashboard_username": "",
        "dashboard_password": "",
    }
    now_str = datetime.now().isoformat()
    for k, v in defaults.items():
        conn.execute(
            "INSERT OR IGNORE INTO bot_config (key, value, updated_at) VALUES (?, ?, ?)",
            (k, v, now_str),
        )

    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            open_time INTEGER PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            quote_volume REAL,
            trades_count INTEGER,
            taker_buy_base REAL,
            taker_buy_quote REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT,
            message TEXT,
            sent_ok INTEGER,
            error_msg TEXT,
            created_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            date TEXT PRIMARY KEY,
            trades_count INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            pnl REAL DEFAULT 0,
            max_drawdown REAL DEFAULT 0,
            trading_halted INTEGER DEFAULT 0,
            updated_at TEXT
        )
    """)

    # Add new columns to trades table (idempotent via try/except)
    new_cols = [
        "entry_clob_up REAL", "entry_clob_down REAL",
        # Smart exit analytics
        "exit_reason TEXT",            # 'hold_to_resolution', 'stop_loss', 'take_profit', 'market_disagree', 'flip'
        "exit_market_ratio REAL",      # market_price_our_side / buy_price at exit
        "exit_volume REAL",            # Polymarket volume at exit time
        "exit_liquidity REAL",         # Polymarket liquidity at exit time
        "entry_conviction REAL",       # conviction score at entry (0-1)
        "exit_confidence REAL",        # model confidence at exit time
        "exit_edge REAL",              # model edge at exit time
        "min_market_ratio REAL",       # lowest market_ratio during the trade (worst drawdown)
        "max_market_ratio REAL",       # highest market_ratio during the trade (best moment)
        # Slippage tracking
        "intended_price REAL",
        "fill_price REAL",
        "slippage_pct REAL",
        "slippage_cost REAL",
    ]
    for col in new_cols:
        try:
            conn.execute(f"ALTER TABLE trades ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()
    conn.close()


def save_trade_to_db(trade, balance_after):
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO trades
            (window_ts, time_str, price_to_beat, close_price, action, direction,
             confidence, edge_val, buy_price, sell_price, bet_size, actual, won, pnl, balance_after,
             entry_clob_up, entry_clob_down,
             exit_reason, exit_market_ratio, exit_volume, exit_liquidity,
             entry_conviction, exit_confidence, exit_edge,
             min_market_ratio, max_market_ratio,
             intended_price, fill_price, slippage_pct, slippage_cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade.get("window_ts"),
            trade.get("time_str"),
            trade.get("price_to_beat"),
            trade.get("close_price"),
            trade.get("action"),
            trade.get("direction"),
            trade.get("confidence"),
            trade.get("edge_val"),
            trade.get("buy_price"),
            trade.get("sell_price"),
            trade.get("bet_size"),
            trade.get("actual"),
            1 if trade.get("won") is True else (0 if trade.get("won") is False else None),
            trade.get("pnl"),
            balance_after,
            trade.get("entry_clob_up"),
            trade.get("entry_clob_down"),
            trade.get("exit_reason"),
            trade.get("exit_market_ratio"),
            trade.get("exit_volume"),
            trade.get("exit_liquidity"),
            trade.get("entry_conviction"),
            trade.get("exit_confidence"),
            trade.get("exit_edge"),
            trade.get("min_market_ratio"),
            trade.get("max_market_ratio"),
            trade.get("intended_price"),
            trade.get("fill_price"),
            trade.get("slippage_pct"),
            trade.get("slippage_cost"),
        ))
        conn.commit()
    finally:
        conn.close()


def save_candles_to_db(df):
    """Save 5-min Binance candles to the candles table (skips duplicates)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        for idx, row in df.iterrows():
            open_time = int(idx.timestamp()) if hasattr(idx, 'timestamp') else int(idx)
            conn.execute("""
                INSERT OR IGNORE INTO candles
                (open_time, open, high, low, close, volume, quote_volume,
                 trades_count, taker_buy_base, taker_buy_quote)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                open_time,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                float(row.get("quote_volume", 0)),
                int(row.get("trades", 0)),
                float(row.get("taker_buy_base", 0)),
                float(row.get("taker_buy_quote", 0)),
            ))
        conn.commit()
    except Exception as e:
        log.warning(f"Failed to save candles to DB: {e}")
    finally:
        conn.close()


def save_prediction_to_db(prediction, market, chainlink_price, price_to_beat, elapsed_s, traded):
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT INTO predictions
            (window_ts, elapsed_s, direction, confidence, prob_up, edge_val,
             poly_up, poly_down, chainlink_price, ptb, traded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            market["window_ts"],
            elapsed_s,
            prediction.get("direction"),
            prediction.get("confidence"),
            prediction.get("prob_up"),
            prediction.get("edge_val", 0),
            market.get("up_price"),
            market.get("down_price"),
            chainlink_price,
            price_to_beat,
            1 if traded else 0,
        ))
        conn.commit()
    finally:
        conn.close()


def load_trades_from_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY window_ts ASC"
        ).fetchall()
        cols = [d[0] for d in conn.execute("SELECT * FROM trades LIMIT 0").description]
    finally:
        conn.close()

    trades = []
    needs_fix = False
    for row in rows:
        t = dict(zip(cols, row))
        # Convert won back to bool/None
        if t["won"] == 1:
            t["won"] = True
        elif t["won"] == 0:
            t["won"] = False
        else:
            t["won"] = None

        # Fix early-exit trades that were saved without P&L
        if (t["action"] not in ("skip", None)
                and t["won"] is None
                and t.get("buy_price") and t.get("sell_price")
                and 0 < t["sell_price"] < 1):
            buy_price = t["buy_price"]
            sell_price = t["sell_price"]
            bet_size = t.get("bet_size") or MIN_BET
            shares = bet_size / buy_price
            t["pnl"] = round(shares * (sell_price - buy_price), 2)
            t["won"] = t["pnl"] > 0
            needs_fix = True

        trades.append(t)

    # Persist fixed P&L back to DB and recalculate balances
    if needs_fix:
        _fix_trade_balances(trades)

    return trades


def _fix_trade_balances(trades):
    """Recalculate balances and persist fixed trades to DB."""
    balance = get_config("starting_balance", STARTING_BALANCE)
    conn = sqlite3.connect(DB_PATH)
    try:
        for t in trades:
            if t["action"] != "skip" and t.get("pnl") is not None:
                balance += t["pnl"]
            t["balance_after"] = round(balance, 2)
            conn.execute("""
                UPDATE trades SET pnl = ?, won = ?, balance_after = ?
                WHERE window_ts = ?
            """, (
                t["pnl"],
                1 if t.get("won") is True else (0 if t.get("won") is False else None),
                t["balance_after"],
                t["window_ts"],
            ))
        conn.commit()
    finally:
        conn.close()


def get_last_balance():
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            "SELECT balance_after FROM trades ORDER BY window_ts DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else get_config("starting_balance", STARTING_BALANCE)


# ─── Chainlink WebSocket ─────────────────────────────────────────────────────

def start_chainlink_stream():
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
        try:
            data = json.loads(message)
            if (data.get("topic") == "crypto_prices_chainlink"
                    and data.get("payload", {}).get("symbol") == "btc/usd"):
                price = data["payload"]["value"]
                ts_ms = data["payload"]["timestamp"]
                with chainlink_state["lock"]:
                    chainlink_state["value"] = price
                    chainlink_state["last_update"] = time.time()
                    buf = chainlink_state["buffer"]
                    buf.append((ts_ms, price))
                    if len(buf) > PRICE_BUFFER_SIZE:
                        chainlink_state["buffer"] = buf[-PRICE_BUFFER_SIZE:]
        except Exception:
            pass

    def on_error(ws, error):
        log.warning(f"Chainlink WS error: {error}")
        ws.close()

    def on_close(ws, code, msg):
        time.sleep(3)
        _connect()

    def _connect():
        ws = websocket.WebSocketApp(
            "wss://ws-live-data.polymarket.com",
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=5)

    t = threading.Thread(target=_connect, daemon=True)
    t.start()


CHAINLINK_STALE_SECONDS = 30

def get_chainlink_price():
    with chainlink_state["lock"]:
        if chainlink_state["value"] is not None and chainlink_state["last_update"] > 0:
            age = time.time() - chainlink_state["last_update"]
            if age > CHAINLINK_STALE_SECONDS:
                log.warning(f"Chainlink price stale ({age:.0f}s old), returning None")
                return None
        return chainlink_state["value"]


def get_chainlink_price_at(target_ts):
    target_ms = target_ts * 1000
    with chainlink_state["lock"]:
        if chainlink_state["last_update"] > 0:
            age = time.time() - chainlink_state["last_update"]
            if age > CHAINLINK_STALE_SECONDS:
                log.warning(f"Chainlink price stale ({age:.0f}s old), returning None")
                return None
        buf = chainlink_state["buffer"]
        if not buf:
            return chainlink_state["value"]
        best = None
        for ts_ms, price in buf:
            if ts_ms <= target_ms:
                best = price
            else:
                break
        if best is not None:
            return best
        return buf[0][1]


def get_chainlink_intracandle(window_ts):
    window_ms = window_ts * 1000
    with chainlink_state["lock"]:
        buf = chainlink_state["buffer"]
        if not buf:
            return None
        window_prices = [(ts, p) for ts, p in buf if ts >= window_ms]

    if len(window_prices) < 2:
        return None

    candle_open = window_prices[0][1]
    minutes = {}
    for ts_ms, price in window_prices:
        minute_idx = int((ts_ms - window_ms) / 60000)
        if minute_idx not in minutes:
            minutes[minute_idx] = []
        minutes[minute_idx].append(price)

    if 0 not in minutes:
        return None

    m0_prices = minutes[0]
    m0_close = m0_prices[-1]
    m0_high = max(m0_prices)
    m0_low = min(m0_prices)

    if 1 in minutes:
        m1_prices = minutes[1]
        m1_close = m1_prices[-1]
        m1_high = max(m1_prices)
        m1_low = min(m1_prices)
    else:
        m1_close = m0_close
        m1_high = m0_high
        m1_low = m0_low

    m2_high = max(m0_high, m1_high)
    m2_low = min(m0_low, m1_low)

    # Use Binance 1m volume ratio if available (scale-independent)
    with binance_1m_state["lock"]:
        vol_ratio = binance_1m_state["vol_ratio"] or 0.0

    return {
        "ic_ret_1m": (m0_close - candle_open) / (candle_open + 1e-10),
        "ic_range_1m": (m0_high - m0_low) / (candle_open + 1e-10),
        "ic_body_1m": (m0_close - candle_open) / (m0_high - m0_low + 1e-10),
        "ic_upper_wick_1m": (m0_high - max(m0_close, candle_open)) / (candle_open + 1e-10),
        "ic_lower_wick_1m": (min(m0_close, candle_open) - m0_low) / (candle_open + 1e-10),
        "ic_vol_1m": 0.0,
        "ic_ret_2m": (m1_close - candle_open) / (candle_open + 1e-10),
        "ic_range_2m": (m2_high - m2_low) / (candle_open + 1e-10),
        "ic_body_2m": (m1_close - candle_open) / (m2_high - m2_low + 1e-10),
        "ic_vol_2m": 0.0,
        "ic_momentum": (m1_close - m0_close) / (candle_open + 1e-10),
        "ic_accel": ((m1_close - m0_close) - (m0_close - candle_open)) / (candle_open + 1e-10),
        "ic_range_pos_2m": (m1_close - m2_low) / (m2_high - m2_low + 1e-10),
        "ic_vol_ratio": vol_ratio,
        "ic_close": m1_close,
    }


# ─── Live Polymarket prices via CLOB API ─────────────────────────────────────

def fetch_clob_prices(token_id_up, token_id_down):
    """Fetch live prices from CLOB midpoint — much more accurate than Gamma."""
    prices = {"up": None, "down": None}
    for label, tid in [("up", token_id_up), ("down", token_id_down)]:
        if not tid:
            continue
        try:
            resp = requests.get(
                f"{config.POLYMARKET_CLOB}/midpoint",
                params={"token_id": tid},
                timeout=5,
            )
            resp.raise_for_status()
            mid = resp.json().get("mid")
            if mid:
                prices[label] = float(mid)
        except Exception:
            pass
    return prices


# ─── Model ───────────────────────────────────────────────────────────────────

def load_model():
    model = lgb.Booster(model_file=config.MODEL_PATH)
    with open(config.SCALER_PATH, "rb") as f:
        feature_cols = pickle.load(f)
    return model, feature_cols


def run_prediction(model, feature_cols, price_to_beat, window_ts):
    try:
        df = fetch_binance_klines(limit=750)

        df_shifted = df.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df_shifted[col] = df[col].shift(1)
        df_shifted.dropna(inplace=True)

        df_shifted = add_technical_indicators(df_shifted)
        df_shifted = add_multi_timeframe_features(df_shifted)
        df_shifted = add_volume_features(df_shifted)
        df_shifted = add_time_features(df_shifted)
        df_shifted = add_streak_features(df_shifted)
        df_shifted = add_lookback_summary_features(df_shifted)
        df_shifted.dropna(inplace=True)

        if len(df_shifted) == 0:
            return {"error": "No data after feature build"}

        ic = get_chainlink_intracandle(window_ts)
        row = df_shifted.iloc[[-1]].copy()

        if ic:
            for key, val in ic.items():
                if key != "ic_close":
                    row[key] = val
            row["price_gap"] = (ic["ic_close"] - price_to_beat) / (row["atr"].values[0] + 1e-10)
            row["price_gap_pct"] = (ic["ic_close"] - price_to_beat) / (price_to_beat + 1e-10)
        else:
            for col in feature_cols:
                if col.startswith("ic_") and col not in row.columns:
                    row[col] = 0.0
            row["price_gap"] = 0.0
            row["price_gap_pct"] = 0.0

        for col in feature_cols:
            if col not in row.columns:
                row[col] = 0.0

        row[feature_cols] = row[feature_cols].replace([np.inf, -np.inf], np.nan)
        row[feature_cols] = row[feature_cols].fillna(0)

        X = row[feature_cols].values
        prob = model.predict(X)[0]

        direction = "UP" if prob > 0.5 else "DOWN"
        confidence = prob if prob > 0.5 else 1 - prob

        if confidence >= 0.70:
            signal = "STRONG"
        elif confidence >= 0.60:
            signal = "MODERATE"
        else:
            signal = "WEAK (skip)"

        atr_val = float(row["atr"].values[0]) if "atr" in row.columns else 0.0

        return {
            "direction": direction,
            "confidence": confidence,
            "prob_up": prob,
            "signal": signal,
            "atr": atr_val,
        }
    except Exception as e:
        return {"error": str(e)}


# ─── Trade Tracking ──────────────────────────────────────────────────────────

def compute_edge(prediction, market):
    """Compute edge for the model's predicted direction. Returns (side, edge_val, buy_price)."""
    if not prediction or "error" in prediction:
        return None, 0, 0

    prob_up = prediction["prob_up"]
    poly_up = market["up_price"]
    poly_down = market["down_price"]
    side = prediction["direction"]

    if side == "UP":
        edge_val = prob_up - poly_up
        buy_price = poly_up
    else:
        edge_val = (1 - prob_up) - poly_down
        buy_price = poly_down

    if edge_val > 0:
        return side, edge_val, buy_price
    return None, edge_val, 0


def should_trade(prediction, market, elapsed_s, chainlink_price, price_to_beat, cal_data=None):
    """
    Smart entry decision. Returns (side, edge_val, buy_price) or (None, ...).

    Three entry strategies:
    A) Edge-based: model disagrees with market (original logic)
    B) Market momentum (80c+): market strongly favors a side, model doesn't disagree
    C) Market slam (90c+): near-certain outcome, enter early (30s+) for quick profit
    """
    if not prediction or "error" in prediction:
        return None, 0, 0

    prob_up = prediction["prob_up"]
    confidence = prediction["confidence"]
    if cal_data is not None:
        confidence = calibrate_confidence(confidence, cal_data)
    poly_up = market["up_price"]
    poly_down = market["down_price"]
    atr = prediction.get("atr", 0)

    # Determine market-favored side
    market_side = "UP" if poly_up >= poly_down else "DOWN"
    market_price = poly_up if market_side == "UP" else poly_down

    # Model's predicted side and edge
    model_side = prediction["direction"]
    if model_side == "UP":
        edge_val = prob_up - poly_up
        model_buy_price = poly_up
    else:
        edge_val = (1 - prob_up) - poly_down
        model_buy_price = poly_down

    # ── Strategy C: Market slam (90c+) — enter early at 30s+ ──
    if (market_price >= get_config("poly_slam_entry", 0.90)
            and market_price <= get_config("poly_momentum_max_buy", 0.92)
            and elapsed_s >= get_config("poly_slam_min_elapsed", 30)
            and confidence >= get_config("slam_min_confidence", 0.50)):
        model_agrees = (model_side == market_side)
        model_strongly_disagrees = (not model_agrees and confidence >= get_config("slam_strong_disagree", 0.65))
        if not model_strongly_disagrees:
            buy_price = market_price
            mkt_edge = confidence - market_price if model_agrees else 0.01
            return market_side, mkt_edge, buy_price

    # ── Strategy B: Market momentum (80c+) — normal entry window ──
    if (market_price >= get_config("poly_momentum_entry", 0.80)
            and market_price <= get_config("poly_momentum_max_buy", 0.92)
            and elapsed_s >= get_config("entry_after", 90)):
        model_agrees = (model_side == market_side)
        model_strongly_disagrees = (not model_agrees and confidence >= get_config("momentum_strong_disagree", 0.60))
        if not model_strongly_disagrees:
            buy_price = market_price
            mkt_edge = confidence - market_price if model_agrees else 0.01
            return market_side, mkt_edge, buy_price

    # ── Strategy A: Edge-based (original logic) ──

    # Only consider model's side for edge-based entries
    side = model_side
    buy_price = model_buy_price

    # Momentum ATR protection
    if chainlink_price and price_to_beat and atr > 0:
        distance_atr = abs(chainlink_price - price_to_beat) / atr
        price_above = chainlink_price >= price_to_beat
        betting_against = (side == "DOWN" and price_above) or (side == "UP" and not price_above)
        if betting_against and distance_atr > get_config("momentum_atr_threshold", 1.5):
            return None, edge_val, 0

    # Check momentum: is Chainlink price moving in our direction?
    momentum_aligns = False
    if chainlink_price and price_to_beat:
        price_above = chainlink_price >= price_to_beat
        momentum_aligns = (side == "UP" and price_above) or (side == "DOWN" and not price_above)

    # Phase 1: early — Only strong setups with momentum
    if elapsed_s < get_config("phase1_max_elapsed", 120):
        if confidence >= get_config("phase1_min_confidence", 0.70) and edge_val > get_config("phase1_min_edge", 0.05) and momentum_aligns:
            return side, edge_val, buy_price
        return None, edge_val, 0

    # Phase 2: mid — Moderate setups OK
    if elapsed_s < get_config("phase2_max_elapsed", 180):
        if confidence >= get_config("phase2_min_confidence", 0.55) and edge_val > get_config("phase2_min_edge", 0.03):
            return side, edge_val, buy_price
        return None, edge_val, 0

    # Phase 3: late — Last chance, need strong conviction
    if elapsed_s <= get_config("entry_before", 240):
        if confidence >= get_config("phase3_min_confidence", 0.70) and edge_val > get_config("phase3_min_edge", 0.05):
            return side, edge_val, buy_price
        return None, edge_val, 0

    # After entry_before — too late
    return None, edge_val, 0


def calculate_bet_size(confidence, edge_val, balance, loss_streak=0, cal_data=None):
    """
    Dynamic bet sizing based on confidence and edge.
    Reads min_bet, max_bet, max_position_pct from DB config.
    Drops to min_bet after CONSECUTIVE_LOSS_LIMIT consecutive losses.
    """
    min_bet = get_config("min_bet", MIN_BET)
    max_bet = get_config("max_bet", MAX_BET)
    max_pct = get_config("max_position_pct", 10) / 100.0

    # Consecutive loss protection
    if loss_streak >= get_config("consecutive_loss_limit", 3, cast=int):
        return min_bet

    # Apply calibration if available
    if cal_data is not None:
        confidence = calibrate_confidence(confidence, cal_data)

    conf_base = get_config("bet_conf_base", 0.55)
    conf_range = get_config("bet_conf_range", 0.20)
    edge_base = get_config("bet_edge_base", 0.03)
    edge_range = get_config("bet_edge_range", 0.12)
    conf_score = max(0.0, min(1.0, (confidence - conf_base) / conf_range))
    edge_score = max(0.0, min(1.0, (edge_val - edge_base) / edge_range))
    score = conf_score * get_config("bet_conf_weight", 0.6) + edge_score * get_config("bet_edge_weight", 0.4)

    bet = min_bet + score * (max_bet - min_bet)

    # Cap at max_position_pct of balance
    bet = min(bet, balance * max_pct)

    return round(max(min_bet, bet), 2)


def fetch_polymarket_result(window_ts):
    """Fetch resolved outcome from Polymarket for a completed window.
    Returns 'UP', 'DOWN', or None if not yet resolved."""
    try:
        event = fetch_polymarket_5m_event(window_ts)
        if not event:
            return None
        # After resolution, outcome prices become 1.0/0.0
        up_price = event["up_price"]
        down_price = event["down_price"]
        # Resolved: one side is ~1.0, other is ~0.0
        resolution_threshold = get_config("resolution_threshold", 0.90)
        if up_price >= resolution_threshold:
            return "UP"
        elif down_price >= resolution_threshold:
            return "DOWN"
        return None  # Not yet resolved
    except Exception:
        return None


def resolve_trade(trade, close_price, poly_result=None):
    ptb = trade["price_to_beat"]
    # Use Polymarket resolved outcome if available, fallback to Chainlink
    if poly_result:
        actual = poly_result
    else:
        actual = "UP" if close_price >= ptb else "DOWN"
    trade["actual"] = actual
    trade["close_price"] = close_price

    if trade["action"] == "skip":
        trade["pnl"] = 0.0
        trade["sell_price"] = 0.0
        trade["won"] = None
    elif trade.get("_early_exited"):
        # exit_reason already set at exit time
        # Early exit: use the recorded exit price for P&L
        buy_price = trade["buy_price"]
        bet_size = trade.get("bet_size", MIN_BET)
        shares = bet_size / buy_price
        exit_price = trade["_exit_price"]
        trade["sell_price"] = exit_price
        trade["pnl"] = round(float(shares * (exit_price - buy_price)), 2)
        trade["won"] = bool(trade["pnl"] > 0)
    else:
        # Held to resolution
        trade["exit_reason"] = "hold_to_resolution"
        buy_side = trade["action"]
        buy_price = trade["buy_price"]  # Price per share (e.g., 0.44)
        bet_size = trade.get("bet_size", MIN_BET)
        shares = bet_size / buy_price  # Number of shares bought

        if actual == buy_side:
            trade["sell_price"] = 1.0
            trade["pnl"] = shares * (1.0 - buy_price)  # Profit per share * shares
            trade["won"] = True
        else:
            trade["sell_price"] = 0.0
            trade["pnl"] = -bet_size  # Lose entire stake
            trade["won"] = False

    return trade


# ─── Confidence Calibration ──────────────────────────────────────────────

def compute_calibration(min_trades=get_config("calibration_min_trades", config.CALIBRATION_MIN_TRADES, cast=int)):
    """Compute calibration factor from historical trades. Returns dict or None."""
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT confidence, won FROM trades WHERE action != 'skip' AND won IS NOT NULL"
        ).fetchall()
    finally:
        conn.close()

    if len(rows) < min_trades:
        return None

    confidences = [r[0] for r in rows]
    wins = [r[1] for r in rows]
    avg_conf = sum(confidences) / len(confidences)
    actual_win_rate = sum(wins) / len(wins)

    if abs(avg_conf - 0.5) < 0.001:
        return {"factor": 1.0, "n_trades": len(rows), "win_rate": actual_win_rate, "avg_conf": avg_conf}

    factor = (actual_win_rate - 0.5) / (avg_conf - 0.5)
    factor = max(0.5, min(1.5, factor))
    return {"factor": factor, "n_trades": len(rows), "win_rate": actual_win_rate, "avg_conf": avg_conf}


def calibrate_confidence(raw_conf, cal_data):
    """Apply calibration to raw confidence. Returns adjusted confidence."""
    if cal_data is None:
        return raw_conf
    return 0.5 + (raw_conf - 0.5) * cal_data["factor"]


# ─── Sound Notifications ────────────────────────────────────────────────

def play_entry_sound(direction):
    """Play beep on position entry. UP = high pitch, DOWN = low pitch."""
    if winsound is None:
        return
    try:
        freq = 800 if direction == "UP" else 400
        winsound.Beep(freq, 200)
    except Exception:
        pass


def play_early_exit_sound():
    """Play double beep on early exit."""
    if winsound is None:
        return
    try:
        winsound.Beep(600, 150)
        winsound.Beep(800, 150)
    except Exception:
        pass


def play_flip_sound():
    """Play 3 short beeps on position flip."""
    if winsound is None:
        return
    try:
        for _ in range(3):
            winsound.Beep(500, 100)
    except Exception:
        pass


# ─── Smart Exit: Market Agreement + Volume + Conviction ─────────────────

def compute_conviction(confidence, edge_val):
    """Compute entry conviction score (0.0 to 1.0) from confidence and edge."""
    conf_base = get_config("bet_conf_base", 0.55)
    conf_range = get_config("bet_conf_range", 0.20)
    edge_base = get_config("bet_edge_base", 0.03)
    edge_range = get_config("bet_edge_range", 0.12)
    conf_score = max(0.0, min(1.0, (confidence - conf_base) / conf_range))
    edge_score = max(0.0, min(1.0, (edge_val - edge_base) / edge_range))
    return conf_score * get_config("bet_conf_weight", 0.6) + edge_score * get_config("bet_edge_weight", 0.4)


def compute_market_ratio(current_trade, market):
    """How much does the market still support our side? Returns cur_price / buy_price."""
    side = current_trade["action"]
    buy_price = current_trade["buy_price"]
    if buy_price <= 0:
        return 0.0
    cur_price = market["up_price"] if side == "UP" else market["down_price"]
    return cur_price / buy_price


def check_early_exit(current_trade, market, remaining_s, prediction=None):
    """
    Smart exit using market agreement + volume + conviction gating.

    Returns (should_exit, exit_reason) where exit_reason is one of:
    'take_profit', 'market_disagree', 'stop_loss', or None.
    """
    side = current_trade["action"]
    if side not in ("UP", "DOWN"):
        return False, None

    buy_price = current_trade["buy_price"]
    bet_size = current_trade.get("bet_size", MIN_BET)
    shares = bet_size / buy_price

    # Current market price for our side
    cur_price = market["up_price"] if side == "UP" else market["down_price"]
    unrealized_pnl = shares * (cur_price - buy_price)

    # ── Market agreement ratio ──
    market_ratio = cur_price / buy_price if buy_price > 0 else 0.0

    # Track min/max market ratio during the trade
    prev_min = current_trade.get("min_market_ratio")
    prev_max = current_trade.get("max_market_ratio")
    current_trade["min_market_ratio"] = min(market_ratio, prev_min) if prev_min is not None else market_ratio
    current_trade["max_market_ratio"] = max(market_ratio, prev_max) if prev_max is not None else market_ratio

    # ── Volume signal ──
    volume = market.get("volume", 0) or 0
    liquidity = market.get("liquidity", 0) or 0
    low_volume = volume < get_config("low_volume_threshold", 500)
    high_volume = volume >= get_config("high_volume_threshold", 2000)

    # ── Entry conviction ──
    conviction = current_trade.get("entry_conviction", 0)

    # ── Store live exit metrics on the trade for dashboard/DB ──
    current_trade["_live_market_ratio"] = market_ratio
    current_trade["_live_volume"] = volume
    current_trade["_live_liquidity"] = liquidity

    # ── Take profit: only within early exit window ──
    exit_min, exit_max = (get_config("early_exit_window_min", 60), get_config("early_exit_window_max", 270))
    if exit_min <= remaining_s <= exit_max:
        max_profit = shares * (1.0 - buy_price)
        if max_profit > 0 and unrealized_pnl > 0:
            profit_ratio = unrealized_pnl / max_profit
            # High conviction → let profits run longer (need 70% of max vs 50%)
            take_profit_thresh = get_config("early_exit_profit_pct", 0.50)
            if conviction >= get_config("conviction_hold_threshold", 0.60):
                take_profit_thresh = min(get_config("take_profit_max", 0.70), take_profit_thresh + get_config("take_profit_conviction_bonus", 0.20))
            if profit_ratio >= take_profit_thresh:
                return True, "take_profit"

    # ── Market agrees (ratio >= 50% of entry) → HOLD ──
    if market_ratio >= get_config("market_agree_hold", 0.50):
        return False, None

    # ── Market disagrees (ratio < threshold) → check volume + conviction ──
    if market_ratio < get_config("market_disagree_sell", 0.25):
        # Market has collapsed against us
        if high_volume:
            # High volume = real money says we're wrong → SELL
            return True, "market_disagree"
        elif low_volume and conviction >= get_config("conviction_hold_threshold", 0.60):
            # Thin market + strong model conviction → HOLD (don't panic on noise)
            return False, None
        else:
            # Medium volume or low conviction → SELL
            return True, "market_disagree"

    # ── In the middle zone (25-50% of entry) → use old stop-loss as fallback ──
    if unrealized_pnl < 0 and abs(unrealized_pnl) >= bet_size * get_config("stop_loss_pct", 0.75):
        # But gate it: if conviction is high and volume is low, hold
        if low_volume and conviction >= get_config("conviction_hold_threshold", 0.60):
            return False, None
        return True, "stop_loss"

    return False, None


def check_position_flip(current_trade, prediction, market, remaining_s):
    """
    Check if we should cut loss and reverse position.
    Returns (should_flip, close_price, new_side, new_buy_price) or (False, ...).
    """
    if remaining_s < get_config("flip_min_remaining", 90):
        return False, 0, None, 0

    if not prediction or "error" in prediction:
        return False, 0, None, 0

    side = current_trade["action"]
    if side not in ("UP", "DOWN"):
        return False, 0, None, 0

    # Already flipped once this window
    if current_trade.get("_flipped_from"):
        return False, 0, None, 0

    buy_price = current_trade["buy_price"]
    bet_size = current_trade.get("bet_size", MIN_BET)

    # Current market price for our side
    cur_price = market["up_price"] if side == "UP" else market["down_price"]
    unrealized_pnl = (bet_size / buy_price) * (cur_price - buy_price)

    # Condition 1: Unrealized loss > FLIP_LOSS_PCT of stake
    if unrealized_pnl >= 0 or abs(unrealized_pnl) < bet_size * get_config("flip_loss_pct", 0.30):
        return False, 0, None, 0

    # Condition 2: Model prediction has FLIPPED to opposite side
    pred_dir = prediction["direction"]
    if pred_dir == side:
        return False, 0, None, 0

    # Condition 3: New edge > 5%
    new_side = pred_dir
    prob_up = prediction["prob_up"]
    poly_up = market["up_price"]
    poly_down = market["down_price"]
    if new_side == "UP":
        new_edge = prob_up - poly_up
        new_buy_price = poly_up
    else:
        new_edge = (1 - prob_up) - poly_down
        new_buy_price = poly_down

    if new_edge <= get_config("flip_min_edge", 0.05):
        return False, 0, None, 0

    # Condition 4: New confidence >= flip_min_confidence
    if prediction["confidence"] < get_config("flip_min_confidence", 0.60):
        return False, 0, None, 0

    # All conditions met — flip
    close_price = cur_price
    return True, close_price, new_side, new_buy_price


def count_consecutive_losses(trade_history):
    """Count trailing consecutive losses from trade history."""
    count = 0
    for t in reversed(trade_history):
        if t.get("action") == "skip":
            continue
        if t.get("won") is False:
            count += 1
        else:
            break
    return count


# ─── Slippage Simulation ─────────────────────────────────────────────────────

def simulate_slippage(intended_price, side, token_id, bet_size):
    """Simulate realistic slippage as a small random cost on top of CLOB price.

    Slippage models the small execution cost of crossing the spread and market
    impact. Typical real-world slippage on Polymarket is 0.1-0.5% for small bets.

    Args:
        intended_price: CLOB price we'd ideally buy/sell at
        side: 'UP' or 'DOWN'
        token_id: Polymarket token ID (unused now, kept for API compat)
        bet_size: Size of our bet in dollars

    Returns:
        (fill_price, slippage_pct, slippage_cost)
    """
    if not get_config("slippage_enabled", 1, cast=int):
        return intended_price, 0.0, 0.0

    slippage_factor = get_config("slippage_factor", 0.005)

    # Simple random slippage: 0 to slippage_factor (default 0-0.5%)
    # Buying: price goes up slightly; Selling: price goes down slightly
    slip = random.uniform(0, slippage_factor)
    fill_price = intended_price * (1 + slip)
    fill_price = min(fill_price, 0.99)  # Cap at 99c

    slippage_pct = slip * 100  # As percentage
    # Cost = fewer shares received due to higher price
    shares_ideal = bet_size / intended_price if intended_price > 0 else 0
    shares_actual = bet_size / fill_price if fill_price > 0 else 0
    slippage_cost = (shares_ideal - shares_actual) * 1.0

    return round(fill_price, 6), round(slippage_pct, 4), round(slippage_cost, 4)


# ─── Daily Stats ─────────────────────────────────────────────────────────────

def update_daily_stats(pnl, won):
    """Update daily_stats table with trade result."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    now_str = datetime.utcnow().isoformat()
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT * FROM daily_stats WHERE date = ?", (today,)).fetchone()
        if row:
            conn.execute("""
                UPDATE daily_stats
                SET trades_count = trades_count + 1,
                    wins = wins + ?,
                    losses = losses + ?,
                    pnl = pnl + ?,
                    updated_at = ?
                WHERE date = ?
            """, (1 if won else 0, 0 if won else 1, pnl, now_str, today))
        else:
            conn.execute("""
                INSERT INTO daily_stats (date, trades_count, wins, losses, pnl, max_drawdown, trading_halted, updated_at)
                VALUES (?, 1, ?, ?, ?, 0, 0, ?)
            """, (today, 1 if won else 0, 0 if won else 1, pnl, now_str))
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_daily_pnl():
    """Get today's cumulative P&L from daily_stats."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT pnl FROM daily_stats WHERE date = ?", (today,)).fetchone()
        conn.close()
        return row[0] if row else 0.0
    except Exception:
        return 0.0


# ─── Display ─────────────────────────────────────────────────────────────────

def get_display_lines():
    return 24 + 2 + TRADE_HISTORY_SHOW + 2 + 2  # +1 for volume line, +2 for config line


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def move_cursor_top():
    sys.stdout.write(f"\033[{get_display_lines()}A\r")
    sys.stdout.flush()


def fmt_time(ts):
    if sys.platform == "win32":
        return datetime.fromtimestamp(ts).strftime("%#I:%M%p").lower()
    return datetime.fromtimestamp(ts).strftime("%-I:%M%p").lower()


def render(market, price_to_beat, prediction, chainlink_now, trade_history, current_trade, balance,
           cal_data=None, consecutive_losses=0, trading_halted=False, halt_reason="",
           daily_pnl=0.0):
    now = int(time.time())
    event_ts = market["window_ts"]
    end_ts = event_ts + 300
    remaining = max(0, end_ts - now)
    mins, secs = divmod(remaining, 60)

    current_price = chainlink_now if chainlink_now else 0
    diff = current_price - price_to_beat if current_price and price_to_beat else 0

    diff_color = GREEN if diff >= 0 else RED
    diff_str = f"{'+' if diff >= 0 else ''}{diff:,.2f}"

    # Polymarket odds
    poly_up = market["up_price"]
    poly_down = market["down_price"]
    up_cents = f"{poly_up * 100:.1f}"
    down_cents = f"{poly_down * 100:.1f}"
    poly_trend = GREEN if poly_up > poly_down else (RED if poly_down > poly_up else DIM)
    poly_dir = "UP" if poly_up > poly_down else "DOWN"
    poly_line = (
        f"  {BOLD}Polymarket:{RESET}        "
        f"{GREEN}Up {up_cents}c{RESET} | {RED}Down {down_cents}c{RESET}"
        f"  {poly_trend}>> {poly_dir}{RESET}"
    )

    # Prediction + edge
    if prediction and "error" not in prediction:
        pred_dir = prediction["direction"]
        pred_conf = prediction["confidence"]
        pred_signal = prediction["signal"]
        pred_color = GREEN if pred_dir == "UP" else RED
        signal_color = GREEN if "STRONG" in pred_signal else (YELLOW if "MODERATE" in pred_signal else DIM)
        pred_line = f"  {BOLD}Prediction:{RESET}        {pred_color}{'ABOVE' if pred_dir == 'UP' else 'BELOW'} ${price_to_beat:,.2f}{RESET}"
        cal_str = ""
        if cal_data is not None:
            cal_conf = calibrate_confidence(pred_conf, cal_data)
            cal_str = f" {DIM}(cal: {cal_conf:.1%}){RESET}"
        conf_line = f"  {BOLD}Confidence:{RESET}        {signal_color}{pred_conf:.1%} - {pred_signal}{RESET}{cal_str}"

        edge_side, edge_val, _ = compute_edge(prediction, market)
        if edge_side:
            ec = GREEN if edge_side == "UP" else RED
            edge_line = f"  {BOLD}Edge:{RESET}              {ec}{BOLD}>> BUY {edge_side} ({edge_val:+.1%} vs market){RESET}"
        else:
            edge_line = f"  {BOLD}Edge:{RESET}              {DIM}NO EDGE (model ~ market){RESET}"
    elif prediction and "error" in prediction:
        pred_line = f"  {BOLD}Prediction:{RESET}        {RED}Error: {prediction['error'][:40]}{RESET}"
        conf_line = f"  {BOLD}Confidence:{RESET}        ---"
        edge_line = f"  {BOLD}Edge:{RESET}              {DIM}---{RESET}"
    else:
        pred_line = f"  {BOLD}Prediction:{RESET}        {DIM}Loading...{RESET}"
        conf_line = f"  {BOLD}Confidence:{RESET}        {DIM}---{RESET}"
        edge_line = f"  {BOLD}Edge:{RESET}              {DIM}---{RESET}"

    # ── Polymarket metrics (always visible) ──
    vol = market.get("volume", 0) or 0
    liq = market.get("liquidity", 0) or 0
    poly_max = max(poly_up, poly_down)

    # Volume label
    if vol >= get_config("high_volume_threshold", 2000):
        vol_label = f"{GREEN}HIGH{RESET}"
    elif vol >= get_config("low_volume_threshold", 500):
        vol_label = f"{YELLOW}MED{RESET}"
    else:
        vol_label = f"{DIM}LOW{RESET}"

    # Market conviction label
    if poly_max >= 0.90:
        mkt_conv_color = GREEN
        mkt_conv_label = "SLAM"
    elif poly_max >= 0.80:
        mkt_conv_color = GREEN
        mkt_conv_label = "STRONG"
    elif poly_max >= 0.65:
        mkt_conv_color = YELLOW
        mkt_conv_label = "MODERATE"
    else:
        mkt_conv_color = DIM
        mkt_conv_label = "SPLIT"

    poly_metrics_line = (
        f"  {BOLD}Market:{RESET}            "
        f"Signal: {mkt_conv_color}{mkt_conv_label} {poly_max:.0%}{RESET}"
        f"  Vol: ${vol:,.0f} {vol_label}"
        f"  Liq: ${liq:,.0f}"
    )

    # Current trade (live position with unrealized P&L + smart exit metrics)
    hold_line = ""
    if current_trade and current_trade["action"] not in ("skip",):
        side = current_trade["action"]
        bp = current_trade["buy_price"]
        bet = current_trade.get("bet_size", MIN_BET)
        shares = bet / bp
        side_color = GREEN if side == "UP" else RED

        if current_trade.get("_early_exited"):
            exit_p = current_trade["_exit_price"]
            realized_pnl = shares * (exit_p - bp)
            pnl_color = GREEN if realized_pnl >= 0 else RED
            reason = current_trade.get("exit_reason", "?")
            trade_line = (
                f"  {BOLD}Position:{RESET}          {YELLOW}SOLD {side} at {exit_p * 100:.1f}c ({reason}){RESET}"
                f"  {pnl_color}Realized: ${realized_pnl:+.2f}{RESET}"
            )
        elif current_trade.get("_flipped_from"):
            orig = current_trade["_flipped_from"]
            cur_price = poly_up if side == "UP" else poly_down
            cur_value = shares * cur_price
            unreal_pnl = cur_value - bet
            pnl_color = GREEN if unreal_pnl >= 0 else RED
            trade_line = (
                f"  {BOLD}Position:{RESET}          {YELLOW}FLIPPED: Sold {orig} -> Bought {side} at {bp * 100:.1f}c (${bet:.2f}){RESET}"
                f"  {pnl_color}P&L: ${unreal_pnl:+.2f}{RESET}"
            )
        else:
            # Current market price for our side
            cur_price = poly_up if side == "UP" else poly_down
            cur_value = shares * cur_price
            unreal_pnl = cur_value - bet

            pnl_color = GREEN if unreal_pnl >= 0 else RED
            trade_line = (
                f"  {BOLD}Position:{RESET}          {side_color}{BOLD}Bought {side} at {bp * 100:.1f}c{RESET}"
                f"  Stake: ${bet:.2f}  Value: ${cur_value:.2f}  {pnl_color}P&L: ${unreal_pnl:+.2f}{RESET}"
            )

        # Hold logic line (shown while position is open and not yet exited)
        if not current_trade.get("_early_exited"):
            mr = current_trade.get("_live_market_ratio", 0)
            conv = current_trade.get("entry_conviction", 0)

            # Market ratio color
            if mr >= get_config("market_agree_hold", 0.50):
                mr_color = GREEN
                mr_label = "AGREE"
            elif mr >= get_config("market_disagree_sell", 0.25):
                mr_color = YELLOW
                mr_label = "WEAK"
            else:
                mr_color = RED
                mr_label = "DISAGREE"

            conv_color = GREEN if conv >= get_config("conviction_hold_threshold", 0.60) else (YELLOW if conv >= 0.3 else DIM)

            hold_line = (
                f"  {BOLD}Hold Logic:{RESET}        "
                f"Mkt ratio: {mr_color}{mr:.0%} {mr_label}{RESET}"
                f"  Conv: {conv_color}{conv:.0%}{RESET}"
            )
    elif trading_halted:
        reason_display = halt_reason.replace("_", " ").title() if halt_reason else "Manual Pause"
        trade_line = f"  {BOLD}Position:{RESET}          {RED}{BOLD}PAUSED — {reason_display}{RESET}  {DIM}(resume from dashboard){RESET}"
    elif current_trade and current_trade.get("_observing"):
        trade_line = f"  {BOLD}Position:{RESET}          {YELLOW}OBSERVING (first window, no trade){RESET}"
    else:
        elapsed_s = 300 - remaining
        poly_max_now = max(poly_up, poly_down)
        if poly_max_now >= get_config("poly_slam_entry", 0.90) and elapsed_s >= get_config("poly_slam_min_elapsed", 30):
            slam_side = "UP" if poly_up >= poly_down else "DOWN"
            slam_color = GREEN if slam_side == "UP" else RED
            trade_line = (
                f"  {BOLD}Position:{RESET}          {slam_color}{BOLD}SLAM ENTRY ready — "
                f"{slam_side} at {poly_max_now * 100:.0f}c{RESET}"
            )
        elif elapsed_s < get_config("entry_after", 90):
            wait_s = get_config("entry_after", 90) - elapsed_s
            trade_line = f"  {BOLD}Position:{RESET}          {DIM}Waiting for entry window ({wait_s:.0f}s)...{RESET}"
        elif elapsed_s < get_config("phase1_max_elapsed", 120):
            trade_line = f"  {BOLD}Position:{RESET}          {YELLOW}Scanning... (strong setup only){RESET}"
        elif elapsed_s < get_config("phase2_max_elapsed", 180):
            trade_line = f"  {BOLD}Position:{RESET}          {YELLOW}Scanning... (moderate+ setup){RESET}"
        elif elapsed_s <= get_config("entry_before", 240):
            trade_line = f"  {BOLD}Position:{RESET}          {YELLOW}Last chance... (strong only){RESET}"
        else:
            trade_line = f"  {BOLD}Position:{RESET}          {DIM}Entry window closed — no trade{RESET}"

    # Time bar
    elapsed = min(300, 300 - remaining)
    bar_width = 30
    filled = int(bar_width * elapsed / 300)
    bar = f"{'#' * filled}{'-' * (bar_width - filled)}"
    time_color = RED if remaining < 30 else (YELLOW if remaining < 60 else CYAN)

    # BTC price direction
    if current_price and price_to_beat:
        if diff > 0:
            price_arrow = f"{GREEN}^ above PTB{RESET}"
        elif diff < 0:
            price_arrow = f"{RED}v below PTB{RESET}"
        else:
            price_arrow = f"{DIM}= at PTB{RESET}"
    else:
        price_arrow = ""

    # Session stats
    resolved = [t for t in trade_history if t.get("actual")]
    trades_taken = [t for t in resolved if t["action"] != "skip"]
    wins = sum(1 for t in trades_taken if t.get("won"))
    losses = sum(1 for t in trades_taken if t.get("won") is False)
    total_pnl = sum(t.get("pnl", 0) for t in trades_taken)
    skipped = sum(1 for t in resolved if t["action"] == "skip")

    starting = get_config("starting_balance", STARTING_BALANCE)
    bal_color = GREEN if balance >= starting else RED
    loss_warn = ""
    if consecutive_losses >= get_config("consecutive_loss_limit", 3, cast=int):
        loss_warn = f"  {RED}{BOLD}[{consecutive_losses}x LOSS STREAK — MIN BET]{RESET}"
    halt_warn = ""
    if trading_halted:
        halt_warn = f"  {RED}{BOLD}[HALTED: {halt_reason}]{RESET}"
    daily_str = f"  {BOLD}Daily:{RESET} {GREEN if daily_pnl >= 0 else RED}${daily_pnl:+.2f}{RESET}"

    if trades_taken:
        pnl_color = GREEN if total_pnl >= 0 else RED
        stats_line = (
            f"  {BOLD}Balance:{RESET}  {bal_color}${balance:.2f}{RESET}"
            f"  {BOLD}P&L:{RESET} {pnl_color}${total_pnl:+.2f}{RESET}"
            f"  ({GREEN}{wins}W{RESET} {RED}{losses}L{RESET})"
            f"  {DIM}Skipped: {skipped}{RESET}"
            f"{daily_str}{loss_warn}{halt_warn}"
        )
    else:
        stats_line = f"  {BOLD}Balance:{RESET}  {bal_color}${balance:.2f}{RESET}  {DIM}No trades yet{RESET}{daily_str}{loss_warn}{halt_warn}"

    # Trade history table
    history_lines = []
    show_trades = list(reversed(resolved[-TRADE_HISTORY_SHOW:]))
    if show_trades:
        history_lines.append(
            f"  {DIM}{'#':>3}  {'Time':<8} {'PTB':>10} {'Bought':>8} {'at':>6} {'Stake':>7} {'Sold':>6} {'Result':>6} {'P&L':>8} {'Exit':>10}{RESET}"
        )
        for i, t in enumerate(show_trades):
            idx = len(resolved) - i
            t_time = t.get("time_str", "")
            t_ptb = f"${t['price_to_beat']:,.0f}" if t.get("price_to_beat") else "---"
            t_actual = t.get("actual", "?")
            t_actual_color = GREEN if t_actual == "UP" else RED if t_actual == "DOWN" else DIM

            if t["action"] == "skip":
                history_lines.append(
                    f"  {idx:>3}  {t_time:<8} {t_ptb:>10} {DIM}{'---':>8} {'---':>6} {'---':>7} {'---':>6}{RESET}"
                    f" {t_actual_color}{t_actual:>6}{RESET} {DIM}{'skip':>8}{RESET}"
                )
            else:
                side = t["action"]
                bp = t.get("buy_price", 0)
                sp = t.get("sell_price", 0)
                bet = t.get("bet_size", MIN_BET)
                pnl_val = t.get("pnl", 0)
                side_color = GREEN if side == "UP" else RED
                exit_r = t.get("exit_reason", "held")
                # Shorten exit reason for display
                exit_short = {"hold_to_resolution": "held", "take_profit": "profit",
                              "market_disagree": "mkt_out", "stop_loss": "stop",
                              "flip": "flip"}.get(exit_r, exit_r or "held")

                if t.get("won"):
                    pnl_str = f"{GREEN}+${pnl_val:.2f}{RESET}"
                elif t.get("won") is False:
                    pnl_str = f"{RED}-${abs(pnl_val):.2f}{RESET}"
                else:
                    pnl_str = "---"

                history_lines.append(
                    f"  {idx:>3}  {t_time:<8} {t_ptb:>10}"
                    f" {side_color}{side:>8}{RESET}"
                    f" {bp * 100:>5.1f}c"
                    f" ${bet:>5.2f}"
                    f" {sp * 100:>5.0f}c"
                    f" {t_actual_color}{t_actual:>6}{RESET}"
                    f" {pnl_str:>8}"
                    f" {DIM}{exit_short:>10}{RESET}"
                )
    else:
        history_lines.append(f"  {DIM}No trades yet — only executed trades shown here{RESET}")

    # Config summary from DB (confirms bot is reading from dashboard settings)
    cfg_min = get_config("min_bet", MIN_BET)
    cfg_max = get_config("max_bet", MAX_BET)
    cfg_pos = get_config("max_position_pct", 10)
    cfg_daily = get_config("daily_loss_limit", 20)
    cfg_stop = get_config("stop_loss_balance", 40)
    cfg_slip = get_config("slippage_enabled", 1, cast=int)
    cfg_slip_f = get_config("slippage_factor", 0.005)
    cfg_tg = get_config("telegram_alerts_enabled", 0, cast=int)
    slip_str = f"{GREEN}ON{RESET} ({cfg_slip_f:.3f})" if cfg_slip else f"{DIM}OFF{RESET}"
    tg_str = f"{GREEN}ON{RESET}" if cfg_tg else f"{DIM}OFF{RESET}"
    config_line = (
        f"  {DIM}Config:{RESET} "
        f"Bet ${cfg_min:.0f}-${cfg_max:.0f}  "
        f"MaxPos {cfg_pos:.0f}%  "
        f"DailyLim ${cfg_daily:.0f}  "
        f"StopBal ${cfg_stop:.0f}  "
        f"Slip {slip_str}  "
        f"TG {tg_str}"
    )

    # Build output
    lines = [
        f"  {BOLD}{CYAN}BTC Up/Down 5m - Live Dashboard{RESET}",
        f"  {DIM}{market['title']}{RESET}",
        f"  {'=' * 75}",
        f"  {BOLD}Price to beat:{RESET}     ${price_to_beat:,.2f}  {DIM}(Chainlink){RESET}" if price_to_beat else f"  {BOLD}Price to beat:{RESET}     {DIM}Waiting...{RESET}",
        f"  {BOLD}BTC Price:{RESET}         {diff_color}${current_price:,.2f}{RESET} ({diff_color}{diff_str}{RESET})  {price_arrow}" if current_price else f"  {BOLD}BTC Price:{RESET}         {DIM}Connecting...{RESET}",
        f"  {BOLD}BTC Volume:{RESET}        5m: ${binance_1m_state.get('btc_5m_volume', 0) or 0:,.0f}  |  24h: ${binance_1m_state.get('btc_volume_24h', 0) or 0:,.0f}" if binance_1m_state.get("btc_volume_24h") else f"  {BOLD}BTC Volume:{RESET}        {DIM}Loading...{RESET}",
        f"  {BOLD}Time remaining:{RESET}    {time_color}{mins}m {secs:02d}s{RESET}  {bar}",
        f"  {'-' * 75}",
        pred_line,
        conf_line,
        poly_line,
        poly_metrics_line,
        f"  {'-' * 75}",
        edge_line,
        trade_line,
        hold_line if hold_line else f"  {DIM}{RESET}",
        f"  {'=' * 75}",
        stats_line,
        config_line,
        f"  {'-' * 75}",
    ]
    lines.extend(history_lines)

    # Pad to fixed height
    total_needed = get_display_lines()
    while len(lines) < total_needed - 1:
        lines.append("")
    lines.append(f"  {DIM}Ctrl+C to stop{RESET}")

    output = "\r\n".join(f"{line:<95}" for line in lines[:total_needed])
    sys.stdout.write(output)
    sys.stdout.flush()

    # Write live state JSON for dashboard
    try:
        # Build position info
        pos_info = None
        hold_info = None
        position_status = "waiting"
        if current_trade and current_trade["action"] not in ("skip",):
            side = current_trade["action"]
            bp = current_trade["buy_price"]
            bet = current_trade.get("bet_size", MIN_BET)
            shares = bet / bp if bp > 0 else 0
            if current_trade.get("_early_exited"):
                exit_p = current_trade["_exit_price"]
                realized = shares * (exit_p - bp)
                position_status = "sold"
                pos_info = {"side": side, "buy_price": bp, "bet_size": bet,
                            "exit_price": exit_p, "realized_pnl": round(realized, 2),
                            "exit_reason": current_trade.get("exit_reason", "?")}
            elif current_trade.get("_flipped_from"):
                cur_p = poly_up if side == "UP" else poly_down
                cur_val = shares * cur_p
                unreal = cur_val - bet
                position_status = "flipped"
                pos_info = {"side": side, "buy_price": bp, "bet_size": bet,
                            "current_value": round(cur_val, 2), "unrealized_pnl": round(unreal, 2),
                            "flipped_from": current_trade["_flipped_from"]}
            else:
                cur_p = poly_up if side == "UP" else poly_down
                cur_val = shares * cur_p
                unreal = cur_val - bet
                position_status = "open"
                pos_info = {"side": side, "buy_price": bp, "bet_size": bet,
                            "current_value": round(cur_val, 2), "unrealized_pnl": round(unreal, 2)}
            if not current_trade.get("_early_exited"):
                hold_info = {
                    "market_ratio": round(current_trade.get("_live_market_ratio", 0), 4),
                    "conviction": round(current_trade.get("entry_conviction", 0), 4),
                    "volume": current_trade.get("_live_volume", 0),
                }
        elif trading_halted:
            position_status = "paused"
        elif current_trade and current_trade.get("_observing"):
            position_status = "observing"
        else:
            elapsed_s = 300 - remaining
            if elapsed_s < get_config("entry_after", 90):
                position_status = "waiting"
            elif elapsed_s <= get_config("entry_before", 240):
                position_status = "scanning"
            else:
                position_status = "closed"

        # Prediction info
        pred_info = None
        edge_info = None
        if prediction and "error" not in prediction:
            pred_info = {"direction": prediction["direction"],
                         "confidence": round(prediction["confidence"], 4),
                         "signal": prediction["signal"]}
            if cal_data is not None:
                pred_info["calibrated"] = round(calibrate_confidence(prediction["confidence"], cal_data), 4)
            e_side, e_val, _ = compute_edge(prediction, market)
            edge_info = {"side": e_side, "value": round(e_val, 4) if e_val else 0}
        elif prediction and "error" in prediction:
            pred_info = {"error": prediction["error"]}

        # Volume label
        vol_label = "HIGH" if vol >= get_config("high_volume_threshold", 2000) else (
            "MED" if vol >= get_config("low_volume_threshold", 500) else "LOW")

        live_state = {
            "timestamp": now,
            "market_title": market.get("title", ""),
            "window_ts": event_ts,
            "price_to_beat": price_to_beat,
            "btc_price": current_price,
            "btc_diff": round(diff, 2),
            "time_remaining": remaining,
            "elapsed": 300 - remaining,
            "btc_volume_5m": binance_1m_state.get("btc_5m_volume"),
            "btc_volume_24h": binance_1m_state.get("btc_volume_24h"),
            "prediction": pred_info,
            "edge": edge_info,
            "polymarket": {
                "up_price": poly_up, "down_price": poly_down,
                "direction": poly_dir, "volume": vol, "liquidity": liq,
                "conviction": mkt_conv_label, "volume_label": vol_label,
            },
            "position_status": position_status,
            "position": pos_info,
            "hold_logic": hold_info,
            "stats": {
                "balance": round(balance, 2), "total_pnl": round(total_pnl, 2),
                "wins": wins, "losses": losses, "skipped": skipped,
                "daily_pnl": round(daily_pnl, 2),
                "consecutive_losses": consecutive_losses,
                "trading_halted": trading_halted, "halt_reason": halt_reason,
            },
            "config": {
                "min_bet": cfg_min, "max_bet": cfg_max,
                "max_position_pct": cfg_pos, "daily_loss_limit": cfg_daily,
                "stop_loss_balance": cfg_stop,
                "slippage_enabled": bool(cfg_slip), "slippage_factor": cfg_slip_f,
                "telegram_enabled": bool(cfg_tg),
            },
        }
        with _pred_feed_lock:
            live_state["pred_feed"] = list(_pred_feed)
        tmp_path = "data/live_state.json.tmp"
        final_path = "data/live_state.json"
        with open(tmp_path, "w") as f:
            json.dump(live_state, f)
        os.replace(tmp_path, final_path)
    except Exception:
        pass


# ─── Main loop ───────────────────────────────────────────────────────────────

def main():
    init_db()
    log.info("Bot starting")
    model, feature_cols = load_model()

    # Load previous trades and balance from DB
    trade_history = load_trades_from_db()
    balance = get_last_balance()

    # Initialize consecutive loss counter from DB history
    consecutive_losses = count_consecutive_losses(trade_history)

    # Confidence calibration
    cal_data = compute_calibration()
    cal_refresh_counter = 0

    # Daily loss tracking
    daily_pnl = get_daily_pnl()
    daily_date = datetime.utcnow().strftime("%Y-%m-%d")
    trading_halted = False
    halt_reason = ""

    # Check if previously halted (persisted in bot_config)
    if get_config("trading_halted", 0, cast=int):
        trading_halted = True
        halt_reason = get_config("halt_reason", "unknown", cast=str)

    if trade_history:
        trades_taken = [t for t in trade_history if t["action"] != "skip"]
        print(f"Loaded {len(trade_history)} trades from DB (balance: ${balance:.2f})")
        if cal_data:
            print(f"Calibration active: factor={cal_data['factor']:.2f} ({cal_data['n_trades']} trades)")
        if consecutive_losses >= get_config("consecutive_loss_limit", 3, cast=int):
            print(f"Loss streak: {consecutive_losses} — betting minimum")
        if trading_halted:
            print(f"TRADING HALTED: {halt_reason}")

    start_chainlink_stream()

    # Start Binance 1m volume polling thread
    threading.Thread(target=bg_poll_binance_1m, daemon=True).start()

    print("Connecting to Chainlink BTC/USD stream...")
    for _ in range(20):
        if get_chainlink_price() is not None:
            break
        time.sleep(0.5)
    else:
        print("[ERROR] Could not connect to Chainlink price stream")
        sys.exit(1)

    clear_screen()

    last_slug = None
    price_to_beat = None
    prediction = None
    market = None
    current_trade = None
    first_window = True  # Skip trading on the first window (joined mid-candle)
    position_locked = False  # Once we open a position, don't change it
    prediction_logged = False  # Track if we've logged a prediction for this window

    # Background workers
    pred_lock = threading.Lock()
    market_lock = threading.Lock()
    bg_prediction = {"value": None}
    bg_market = {"value": None}
    bg_pred_running = {"flag": False}
    bg_market_running = {"flag": False}

    # CLOB price state
    clob_lock = threading.Lock()
    clob_prices = {"up": None, "down": None}
    bg_clob_running = {"flag": False}

    def bg_run_prediction(mdl, fcols, ptb, wts):
        try:
            result = run_prediction(mdl, fcols, ptb, wts)
            with pred_lock:
                bg_prediction["value"] = result
            # Log to prediction feed
            cur_price = get_chainlink_price()
            vol_5m = binance_1m_state.get("btc_5m_volume")
            vol_24h = binance_1m_state.get("btc_volume_24h")
            if result and "error" not in result:
                entry = {
                    "t": int(time.time()),
                    "dir": result["direction"],
                    "conf": round(result["confidence"], 4),
                    "sig": result["signal"],
                    "ptb": ptb,
                    "btc": round(cur_price, 2) if cur_price else None,
                    "vol5m": round(vol_5m, 0) if vol_5m else None,
                    "vol24h": round(vol_24h, 0) if vol_24h else None,
                    "atr": round(result.get("atr", 0), 2),
                }
            else:
                entry = {"t": int(time.time()), "error": result.get("error", "?") if result else "exception"}
            with _pred_feed_lock:
                _pred_feed.append(entry)
                if len(_pred_feed) > 1:
                    _pred_feed.pop(0)
        except Exception:
            pass
        finally:
            with pred_lock:
                bg_pred_running["flag"] = False

    def bg_poll_market():
        try:
            m = fetch_polymarket_5m_current()
            if m:
                with market_lock:
                    bg_market["value"] = m
        except Exception:
            pass
        with market_lock:
            bg_market_running["flag"] = False

    def bg_poll_clob(tid_up, tid_down):
        try:
            p = fetch_clob_prices(tid_up, tid_down)
            with clob_lock:
                if p["up"] is not None:
                    clob_prices["up"] = p["up"]
                if p["down"] is not None:
                    clob_prices["down"] = p["down"]
        except Exception:
            pass
        finally:
            with clob_lock:
                bg_clob_running["flag"] = False

    last_pred_launch = 0
    last_market_launch = 0
    last_clob_launch = 0

    ENTRY_AFTER = get_config("entry_after", 90)
    ENTRY_BEFORE = get_config("entry_before", 240)

    try:
        while True:
            now = int(time.time())

            # Launch Polymarket Gamma poll every 3 seconds (just for metadata)
            if now - last_market_launch >= 3:
                with market_lock:
                    if not bg_market_running["flag"]:
                        bg_market_running["flag"] = True
                        last_market_launch = now
                        threading.Thread(target=bg_poll_market, daemon=True).start()

            # Launch CLOB price poll every 1 second (for accurate prices)
            if market and now - last_clob_launch >= 1:
                with clob_lock:
                    if not bg_clob_running["flag"]:
                        bg_clob_running["flag"] = True
                        last_clob_launch = now
                        threading.Thread(
                            target=bg_poll_clob,
                            args=(market.get("token_id_up"), market.get("token_id_down")),
                            daemon=True,
                        ).start()

            # Pick up market updates
            with market_lock:
                if bg_market["value"] is not None:
                    m = bg_market["value"]
                    if market is None or m["slug"] != last_slug:
                        # ── Window transition ──
                        # 1. Resolve previous trade (only store actual trades, not skips)
                        if current_trade and current_trade.get("price_to_beat"):
                            if current_trade["action"] != "skip" and not current_trade.get("_observing"):
                                # Get actual result from Polymarket (authoritative)
                                poly_result = fetch_polymarket_result(current_trade["window_ts"])
                                close_price = get_chainlink_price_at(
                                    current_trade["window_ts"] + 300
                                )
                                if close_price:
                                    current_trade = resolve_trade(
                                        current_trade, close_price,
                                        poly_result=poly_result,
                                    )
                                    balance += current_trade["pnl"]
                                    save_trade_to_db(current_trade, balance)
                                    trade_history.append(current_trade)

                                    pnl_val = current_trade["pnl"]
                                    won_val = current_trade.get("won")
                                    side_val = current_trade["action"]
                                    log.info("Trade resolved", extra={"data": {
                                        "side": side_val, "pnl": pnl_val,
                                        "won": won_val, "balance": balance,
                                    }})

                                    # Send trade exit alert
                                    result_str = f"Won +${pnl_val:.2f}" if won_val else f"Lost -${abs(pnl_val):.2f}"
                                    exit_r = current_trade.get("exit_reason", "held")
                                    send_alert("TRADE_EXIT", f"{result_str} on {side_val} ({exit_r}). Balance: ${balance:.2f}")

                                    # Update daily stats
                                    update_daily_stats(pnl_val, won_val)

                                    # Daily date rollover
                                    today = datetime.utcnow().strftime("%Y-%m-%d")
                                    if today != daily_date:
                                        daily_pnl = 0.0
                                        daily_date = today
                                    daily_pnl += pnl_val

                                    # Check daily loss limit
                                    daily_limit = get_config("daily_loss_limit", 20)
                                    if daily_pnl <= -daily_limit and not trading_halted:
                                        trading_halted = True
                                        halt_reason = "daily_loss"
                                        log.warning("Daily loss limit hit", extra={"data": {"daily_pnl": daily_pnl}})
                                        send_alert("DAILY_LOSS_LIMIT", f"Daily loss ${daily_pnl:+.2f} hit limit -${daily_limit}. Trading halted.")
                                        try:
                                            conn = sqlite3.connect(DB_PATH)
                                            conn.execute("INSERT OR REPLACE INTO bot_config (key, value, updated_at) VALUES (?, ?, ?)",
                                                         ("trading_halted", "1", datetime.utcnow().isoformat()))
                                            conn.execute("INSERT OR REPLACE INTO bot_config (key, value, updated_at) VALUES (?, ?, ?)",
                                                         ("halt_reason", "daily_loss", datetime.utcnow().isoformat()))
                                            conn.commit()
                                            conn.close()
                                        except Exception:
                                            pass

                                    # Check balance stop-loss
                                    stop_loss_bal = get_config("stop_loss_balance", 40)
                                    if balance <= stop_loss_bal and not trading_halted:
                                        trading_halted = True
                                        halt_reason = "balance_stop_loss"
                                        log.warning("Balance stop-loss hit", extra={"data": {"balance": balance}})
                                        send_alert("BALANCE_STOP_LOSS", f"Balance ${balance:.2f} <= stop-loss ${stop_loss_bal:.2f}. Trading halted.")
                                        try:
                                            conn = sqlite3.connect(DB_PATH)
                                            conn.execute("INSERT OR REPLACE INTO bot_config (key, value, updated_at) VALUES (?, ?, ?)",
                                                         ("trading_halted", "1", datetime.utcnow().isoformat()))
                                            conn.execute("INSERT OR REPLACE INTO bot_config (key, value, updated_at) VALUES (?, ?, ?)",
                                                         ("halt_reason", "balance_stop_loss", datetime.utcnow().isoformat()))
                                            conn.commit()
                                            conn.close()
                                        except Exception:
                                            pass

                                    # Check drawdown alert
                                    starting = get_config("starting_balance", STARTING_BALANCE)
                                    drawdown_pct = (starting - balance) / starting * 100 if starting > 0 else 0
                                    alert_pct = get_config("drawdown_alert_pct", 15)
                                    if drawdown_pct >= alert_pct:
                                        send_alert("DRAWDOWN", f"Drawdown at {drawdown_pct:.1f}%, exceeds {alert_pct:.0f}% threshold. Balance: ${balance:.2f}")

                                    # Update consecutive loss counter
                                    if current_trade.get("won") is False:
                                        consecutive_losses += 1
                                    elif current_trade.get("won") is True:
                                        consecutive_losses = 0

                                    # Check if halt was cleared from dashboard
                                    if trading_halted and not get_config("trading_halted", 0, cast=int):
                                        trading_halted = False
                                        halt_reason = ""

                        # Save latest Binance 5m candles to DB for retraining
                        try:
                            candle_df = fetch_binance_klines(limit=5)
                            if candle_df is not None and len(candle_df) > 0:
                                save_candles_to_db(candle_df)
                        except Exception as e:
                            log.warning(f"Candle save failed: {e}")

                        # Refresh calibration every 10 windows
                        cal_refresh_counter += 1
                        if cal_refresh_counter >= 10:
                            cal_data = compute_calibration()
                            cal_refresh_counter = 0

                        # 2. Start new window
                        last_slug = m["slug"]
                        event_ts = m["window_ts"]
                        price_to_beat = get_chainlink_price_at(event_ts)
                        prediction = None
                        last_pred_launch = 0
                        position_locked = False
                        prediction_logged = False

                        # 3. Initialize new trade record
                        current_trade = {
                            "window_ts": event_ts,
                            "time_str": fmt_time(event_ts),
                            "price_to_beat": price_to_beat,
                            "action": "skip",
                            "direction": None,
                            "confidence": 0,
                            "edge_val": 0,
                            "buy_price": 0,
                            "sell_price": 0,
                        }

                        # First window: always observe only (joined mid-candle)
                        if first_window:
                            current_trade["_observing"] = True
                            position_locked = True
                            first_window = False

                        clear_screen()

                    # Update market with CLOB prices if available
                    with clob_lock:
                        if clob_prices["up"] is not None:
                            m["up_price"] = clob_prices["up"]
                        if clob_prices["down"] is not None:
                            m["down_price"] = clob_prices["down"]

                    market = m

            if market is None:
                time.sleep(0.5)
                continue

            # Launch prediction every 1 second
            if price_to_beat and market and (now - last_pred_launch >= 1):
                with pred_lock:
                    if not bg_pred_running["flag"]:
                        bg_pred_running["flag"] = True
                        last_pred_launch = now
                        threading.Thread(
                            target=bg_run_prediction,
                            args=(model, feature_cols, price_to_beat, market["window_ts"]),
                            daemon=True,
                        ).start()

            # Pick up prediction updates
            with pred_lock:
                if bg_prediction["value"] is not None:
                    prediction = bg_prediction["value"]
                    bg_prediction["value"] = None

                    if current_trade and prediction and "error" not in prediction:
                        current_trade["direction"] = prediction["direction"]
                        current_trade["confidence"] = prediction["confidence"]

                        elapsed_in_window = now - market["window_ts"]

                        # Always update edge display
                        _, raw_edge, _ = compute_edge(prediction, market)
                        current_trade["edge_val"] = raw_edge

                        # Log prediction once per window (at entry decision time ~90-120s)
                        if not prediction_logged and ENTRY_AFTER <= elapsed_in_window <= ENTRY_AFTER + 30:
                            traded = current_trade["action"] not in ("skip",)
                            try:
                                save_prediction_to_db(
                                    prediction, market, get_chainlink_price(),
                                    price_to_beat, elapsed_in_window, traded,
                                )
                            except Exception:
                                pass
                            prediction_logged = True

                        # Check if halt was cleared from dashboard (each tick)
                        if trading_halted and not get_config("trading_halted", 0, cast=int):
                            trading_halted = False
                            halt_reason = ""
                            log.info("Trading resumed from dashboard")

                        # Try to enter if not locked and not halted
                        # Normal window: 90-240s. Slam trades (90c+): 30s+
                        poly_max = max(market["up_price"], market["down_price"])
                        early_ok = (poly_max >= get_config("poly_slam_entry", 0.90)
                                    and elapsed_in_window >= get_config("poly_slam_min_elapsed", 30))
                        normal_ok = ENTRY_AFTER <= elapsed_in_window <= ENTRY_BEFORE
                        if not position_locked and not trading_halted and (normal_ok or early_ok):
                            trade_side, edge_val, buy_price = should_trade(
                                prediction, market, elapsed_in_window,
                                get_chainlink_price(), price_to_beat,
                                cal_data=cal_data,
                            )
                            if trade_side:
                                bet = calculate_bet_size(
                                    prediction["confidence"], edge_val, balance,
                                    loss_streak=consecutive_losses,
                                    cal_data=cal_data,
                                )

                                # Slippage simulation
                                intended_price = buy_price
                                token_id = market.get("token_id_up") if trade_side == "UP" else market.get("token_id_down")
                                fill_price, slip_pct, slip_cost = simulate_slippage(
                                    intended_price, trade_side, token_id, bet,
                                )
                                current_trade["intended_price"] = intended_price
                                current_trade["fill_price"] = fill_price
                                current_trade["slippage_pct"] = slip_pct
                                current_trade["slippage_cost"] = slip_cost
                                buy_price = fill_price  # Use fill price for P&L

                                current_trade["action"] = trade_side
                                current_trade["buy_price"] = buy_price
                                current_trade["bet_size"] = bet
                                current_trade["edge_val"] = edge_val
                                current_trade["entry_conviction"] = compute_conviction(
                                    prediction["confidence"], edge_val,
                                )
                                # Record entry CLOB prices
                                current_trade["entry_clob_up"] = market.get("up_price")
                                current_trade["entry_clob_down"] = market.get("down_price")
                                position_locked = True

                                log.info("Trade entered", extra={"data": {
                                    "side": trade_side, "buy_price": buy_price,
                                    "bet": bet, "edge": edge_val,
                                    "slippage_pct": slip_pct,
                                }})
                                send_alert("TRADE_ENTRY",
                                    f"Bought {trade_side} at {buy_price*100:.1f}c, stake ${bet:.2f}"
                                    + (f" (slip: {slip_pct:.2f}%)" if slip_pct > 0 else ""))

                                # Sound notification
                                play_entry_sound(trade_side)

                                # Update prediction log as traded
                                if prediction_logged:
                                    try:
                                        save_prediction_to_db(
                                            prediction, market, get_chainlink_price(),
                                            price_to_beat, elapsed_in_window, True,
                                        )
                                    except Exception:
                                        pass

            # ── Early exit / position flip checks (each tick while position open) ──
            if (current_trade and current_trade["action"] not in ("skip",)
                    and not current_trade.get("_early_exited")
                    and not current_trade.get("_observing")
                    and market):
                remaining_s = max(0, (market["window_ts"] + 300) - now)

                # Smart exit check (market agreement + volume + conviction)
                should_exit, exit_reason = check_early_exit(
                    current_trade, market, remaining_s, prediction=prediction,
                )
                if should_exit:
                    side = current_trade["action"]
                    exit_price = market["up_price"] if side == "UP" else market["down_price"]
                    # Apply slippage on exit (selling at bid)
                    if get_config("slippage_enabled", 1, cast=int):
                        token_id = market.get("token_id_up") if side == "UP" else market.get("token_id_down")
                        slippage_factor = get_config("slippage_factor", 0.005)
                        exit_price *= (1 - random.uniform(0, slippage_factor))
                        exit_price = max(exit_price, 0.01)
                    current_trade["_early_exited"] = True
                    current_trade["_exit_price"] = exit_price
                    current_trade["exit_reason"] = exit_reason
                    current_trade["exit_market_ratio"] = current_trade.get("_live_market_ratio", 0)
                    current_trade["exit_volume"] = current_trade.get("_live_volume", 0)
                    current_trade["exit_liquidity"] = current_trade.get("_live_liquidity", 0)
                    if prediction and "error" not in prediction:
                        current_trade["exit_confidence"] = prediction.get("confidence", 0)
                        _, exit_edge, _ = compute_edge(prediction, market)
                        current_trade["exit_edge"] = exit_edge
                    play_early_exit_sound()

                # Position flip check (mutually exclusive with early exit)
                elif prediction and "error" not in prediction:
                    should_flip, close_price, new_side, new_buy_price = check_position_flip(
                        current_trade, prediction, market, remaining_s,
                    )
                    if should_flip:
                        # 1. Close current position at loss
                        old_side = current_trade["action"]
                        buy_price = current_trade["buy_price"]
                        bet_size = current_trade.get("bet_size", MIN_BET)
                        shares = bet_size / buy_price
                        current_trade["sell_price"] = close_price
                        current_trade["pnl"] = shares * (close_price - buy_price)
                        current_trade["won"] = current_trade["pnl"] > 0
                        current_trade["actual"] = "FLIP"
                        current_trade["close_price"] = get_chainlink_price() or 0
                        current_trade["exit_reason"] = "flip"
                        current_trade["exit_market_ratio"] = current_trade.get("_live_market_ratio", 0)
                        current_trade["exit_volume"] = current_trade.get("_live_volume", 0)
                        current_trade["exit_liquidity"] = current_trade.get("_live_liquidity", 0)
                        current_trade["exit_confidence"] = prediction.get("confidence", 0)
                        _, exit_edge, _ = compute_edge(prediction, market)
                        current_trade["exit_edge"] = exit_edge
                        balance += current_trade["pnl"]
                        save_trade_to_db(current_trade, balance)
                        trade_history.append(current_trade)

                        # Update consecutive losses
                        if current_trade["pnl"] < 0:
                            consecutive_losses += 1
                        else:
                            consecutive_losses = 0

                        # 2. Open new position in opposite direction
                        new_conviction = compute_conviction(prediction["confidence"], exit_edge)
                        current_trade = {
                            "window_ts": market["window_ts"],
                            "time_str": fmt_time(market["window_ts"]),
                            "price_to_beat": price_to_beat,
                            "action": new_side,
                            "direction": new_side,
                            "confidence": prediction["confidence"],
                            "edge_val": current_trade.get("edge_val", 0),
                            "buy_price": new_buy_price,
                            "sell_price": 0,
                            "bet_size": MIN_BET,  # Conservative — already down
                            "entry_conviction": new_conviction,
                            "entry_clob_up": market.get("up_price"),
                            "entry_clob_down": market.get("down_price"),
                            "_flipped_from": old_side,
                        }
                        position_locked = True  # No further flips
                        play_flip_sound()

            # Render
            move_cursor_top()
            render(market, price_to_beat, prediction, get_chainlink_price(),
                   trade_history, current_trade, balance,
                   cal_data=cal_data, consecutive_losses=consecutive_losses,
                   trading_halted=trading_halted, halt_reason=halt_reason,
                   daily_pnl=daily_pnl)

            # Check if window ended
            end_ts = market["window_ts"] + 300
            if now >= end_ts:
                last_slug = None
                last_market_launch = 0

            time.sleep(0.5)

    except KeyboardInterrupt:
        # Resolve current trade on exit (only if we have a real position)
        if current_trade and current_trade["action"] != "skip" and not current_trade.get("_observing"):
            poly_result = fetch_polymarket_result(current_trade["window_ts"])
            close_price = get_chainlink_price()
            if close_price:
                current_trade = resolve_trade(current_trade, close_price, poly_result=poly_result)
                balance += current_trade["pnl"]
                save_trade_to_db(current_trade, balance)
                trade_history.append(current_trade)

        trades_taken = [t for t in trade_history if t["action"] != "skip"]
        total_pnl = sum(t.get("pnl", 0) for t in trades_taken)
        wins = sum(1 for t in trades_taken if t.get("won"))
        losses = sum(1 for t in trades_taken if t.get("won") is False)

        print(f"\n\033[0m")
        print(f"  Session Summary")
        starting = get_config("starting_balance", STARTING_BALANCE)
        print(f"  Balance: ${balance:.2f} (started at ${starting:.2f})")
        print(f"  {len(trade_history)} windows, {len(trades_taken)} trades")
        if trades_taken:
            print(f"  P&L: ${total_pnl:+.2f} | {wins}W {losses}L")
        print(f"  Trades saved to {DB_PATH}")
        print("  Stopped.")


if __name__ == "__main__":
    main()
