"""Feature engineering: technical indicators, multi-timeframe, volume, time, lookback summaries."""

import numpy as np
import pandas as pd
import ta

import config


def add_technical_indicators(df):
    """Add technical analysis indicators to OHLCV DataFrame."""
    df["rsi"] = ta.momentum.RSIIndicator(
        df["close"], window=config.RSI_PERIOD
    ).rsi()

    df["ema_short"] = ta.trend.EMAIndicator(
        df["close"], window=config.EMA_SHORT
    ).ema_indicator()
    df["ema_long"] = ta.trend.EMAIndicator(
        df["close"], window=config.EMA_LONG
    ).ema_indicator()
    df["ema_crossover"] = (df["ema_short"] - df["ema_long"]) / df["close"]

    macd = ta.trend.MACD(
        df["close"],
        window_slow=config.MACD_SLOW,
        window_fast=config.MACD_FAST,
        window_sign=config.MACD_SIGNAL,
    )
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()

    bb = ta.volatility.BollingerBands(df["close"], window=config.BB_PERIOD)
    df["bb_width"] = (bb.bollinger_hband() - bb.bollinger_lband()) / df["close"]
    df["bb_pct"] = bb.bollinger_pband()

    df["atr"] = ta.volatility.AverageTrueRange(
        df["high"], df["low"], df["close"], window=config.ATR_PERIOD
    ).average_true_range()
    df["atr_pct"] = df["atr"] / df["close"]

    df["volume_sma"] = df["volume"].rolling(window=20).mean()
    df["volume_ratio"] = df["volume"] / df["volume_sma"]

    df["returns_1"] = df["close"].pct_change(1)
    df["returns_3"] = df["close"].pct_change(3)
    df["returns_5"] = df["close"].pct_change(5)
    df["returns_10"] = df["close"].pct_change(10)
    df["returns_20"] = df["close"].pct_change(20)

    df["hl_range"] = (df["high"] - df["low"]) / df["close"]
    df["body_ratio"] = (df["close"] - df["open"]) / (df["high"] - df["low"] + 1e-10)

    return df


def add_multi_timeframe_features(df):
    """Add features from higher timeframes (15m, 1h, 4h)."""
    for period, label in [(3, "15m"), (12, "1h"), (48, "4h")]:
        htf_close = df["close"].rolling(window=period).mean()
        htf_high = df["high"].rolling(window=period).max()
        htf_low = df["low"].rolling(window=period).min()
        htf_volume = df["volume"].rolling(window=period).sum()

        df[f"{label}_trend"] = (df["close"] - htf_close) / (htf_close + 1e-10)
        df[f"{label}_range_pos"] = (df["close"] - htf_low) / (htf_high - htf_low + 1e-10)

        df[f"{label}_rsi"] = ta.momentum.RSIIndicator(
            df["close"], window=config.RSI_PERIOD * period
        ).rsi()

        df[f"{label}_momentum"] = df["close"].pct_change(period)

        htf_vol_avg = htf_volume / period
        df[f"{label}_vol_ratio"] = df["volume"] / (htf_vol_avg + 1e-10)

    return df


def add_volume_features(df):
    """Advanced volume analysis features."""
    df["volume_delta"] = df["volume"].pct_change(1)

    vol_mean = df["volume"].rolling(window=50).mean()
    vol_std = df["volume"].rolling(window=50).std()
    df["volume_zscore"] = (df["volume"] - vol_mean) / (vol_std + 1e-10)

    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_tp_vol = (typical_price * df["volume"]).rolling(window=20).sum()
    cumulative_vol = df["volume"].rolling(window=20).sum()
    vwap = cumulative_tp_vol / (cumulative_vol + 1e-10)
    df["vwap_dev"] = (df["close"] - vwap) / (vwap + 1e-10)

    df["buy_pressure"] = (df["close"] - df["low"]) / (df["high"] - df["low"] + 1e-10)
    df["vol_price_mom"] = df["returns_1"] * df["volume_ratio"]

    return df


def add_time_features(df):
    """Time-based features with cyclical encoding."""
    idx = df.index
    if not isinstance(idx, pd.DatetimeIndex):
        return df

    hour = idx.hour + idx.minute / 60.0
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)

    dow = idx.dayofweek
    df["dow_sin"] = np.sin(2 * np.pi * dow / 7)
    df["dow_cos"] = np.cos(2 * np.pi * dow / 7)

    return df


def add_streak_features(df):
    """Streak and pattern features."""
    up = (df["close"] >= df["open"]).astype(int)

    streak = up.copy()
    for i in range(1, len(streak)):
        if up.iloc[i] == up.iloc[i - 1]:
            streak.iloc[i] = streak.iloc[i - 1] + 1
        else:
            streak.iloc[i] = 1
    df["streak_len"] = streak
    df["streak_dir"] = up * 2 - 1
    df["streak_signed"] = df["streak_len"] * df["streak_dir"]

    for w in [5, 10, 20]:
        df[f"win_rate_{w}"] = up.rolling(window=w).mean()

    df["momentum_accel"] = df["returns_1"].diff()

    for w in [10, 30, 60]:
        rolling_mean = df["close"].rolling(window=w).mean()
        df[f"mean_rev_{w}"] = (df["close"] - rolling_mean) / (rolling_mean + 1e-10)

    vol_short = df["returns_1"].rolling(window=10).std()
    vol_long = df["returns_1"].rolling(window=50).std()
    df["vol_regime"] = vol_short / (vol_long + 1e-10)

    df["dist_from_high_20"] = (df["close"] - df["high"].rolling(20).max()) / df["close"]
    df["dist_from_low_20"] = (df["close"] - df["low"].rolling(20).min()) / df["close"]

    return df


def add_lookback_summary_features(df):
    """Summarize lookback window patterns as flat features for tree models."""
    for w in [5, 10, 30]:
        df[f"ret_mean_{w}"] = df["returns_1"].rolling(w).mean()
        df[f"ret_std_{w}"] = df["returns_1"].rolling(w).std()
        df[f"ret_skew_{w}"] = df["returns_1"].rolling(w).skew()
        df[f"ret_min_{w}"] = df["returns_1"].rolling(w).min()
        df[f"ret_max_{w}"] = df["returns_1"].rolling(w).max()

        df[f"vol_mean_{w}"] = df["volume"].rolling(w).mean()
        df[f"vol_std_{w}"] = df["volume"].rolling(w).std()

        df[f"rsi_change_{w}"] = df["rsi"].diff(w)
        df[f"hl_range_mean_{w}"] = df["hl_range"].rolling(w).mean()

    return df


def create_target(df):
    """
    Target: will the NEXT candle close >= its open?
    Uses current candle's features to predict next candle's direction.

    The next candle's open is the "Price to beat" on Polymarket.
    """
    next_close = df["close"].shift(-1)
    next_open = df["open"].shift(-1)

    # price_gap: how far current close is from the price to beat (next open)
    # Normalized by ATR so it's scale-independent across BTC price history
    df["price_gap"] = (df["close"] - next_open) / (df["atr"] + 1e-10)
    # Also as a simple percentage
    df["price_gap_pct"] = (df["close"] - next_open) / (next_open + 1e-10)

    df["target"] = (next_close >= next_open).astype(float)

    # Drop last row (no next candle)
    df = df.iloc[:-1].copy()
    df["target"] = df["target"].astype(int)
    return df


def create_target_intracandle(df):
    """
    Target: will THIS candle close >= its open?
    This is used with intra-candle features — we predict the current
    candle's outcome using partial data from the first 1-2 minutes.

    This matches how we actually trade: we see the open (price to beat)
    and the first minute(s) of price action, then predict the close.
    """
    df = df.copy()
    df["target"] = (df["close"] >= df["open"]).astype(int)

    # price_gap: how far the intra-candle price is from the open (price to beat)
    # ic_close is the price at minute 1-2 (set by build_features_1m)
    if "ic_close" in df.columns:
        df["price_gap"] = (df["ic_close"] - df["open"]) / (df["atr"] + 1e-10)
        df["price_gap_pct"] = (df["ic_close"] - df["open"]) / (df["open"] + 1e-10)
    else:
        df["price_gap"] = 0.0
        df["price_gap_pct"] = 0.0

    return df


def add_intracandle_features(df_5m, df_1m):
    """
    Add intra-candle features from 1-minute data (vectorized).
    For each 5-min candle, extracts what happened in the first 1-2 minutes.

    This simulates the information available when you decide to trade
    at minute 1-2 of the Polymarket 5-min window.
    """
    # Assign each 1-min candle to its 5-min window
    df_1m = df_1m.copy()
    df_1m["window"] = df_1m.index.floor("5min")
    df_1m["minute_in_window"] = ((df_1m.index - df_1m["window"]).dt.total_seconds() / 60).astype(int)

    # Extract minute 0, minute 1, and minute 2 data
    m0 = df_1m[df_1m["minute_in_window"] == 0].set_index("window")
    m1 = df_1m[df_1m["minute_in_window"] == 1].set_index("window")
    m2 = df_1m[df_1m["minute_in_window"] == 2].set_index("window")

    # Only keep windows where minutes 0, 1, and 2 all exist
    common = m0.index.intersection(m1.index).intersection(m2.index).intersection(df_5m.index)

    candle_open = m0.loc[common, "open"]  # = price to beat

    # Minute 1 features (after first 60s)
    m0_close = m0.loc[common, "close"]
    m0_high = m0.loc[common, "high"]
    m0_low = m0.loc[common, "low"]
    m0_vol = m0.loc[common, "volume"]

    # Minute 2 features (after first 120s)
    m1_close = m1.loc[common, "close"]
    m1_high = m1.loc[common, "high"]
    m1_low = m1.loc[common, "low"]
    m1_vol = m1.loc[common, "volume"]

    # Minute 3 features (after first 180s)
    m2_close = m2.loc[common, "close"]
    m2_high = m2.loc[common, "high"]
    m2_low = m2.loc[common, "low"]
    m2_vol = m2.loc[common, "volume"]

    # Combined high/low/vol for first 2 minutes
    first2_high = pd.concat([m0_high, m1_high], axis=1).max(axis=1)
    first2_low = pd.concat([m0_low, m1_low], axis=1).min(axis=1)
    first2_vol = m0_vol + m1_vol

    # Combined high/low/vol for first 3 minutes
    first3_high = pd.concat([m0_high, m1_high, m2_high], axis=1).max(axis=1)
    first3_low = pd.concat([m0_low, m1_low, m2_low], axis=1).min(axis=1)
    first3_vol = m0_vol + m1_vol + m2_vol

    ic = pd.DataFrame(index=common)

    # Minute 1 features
    ic["ic_ret_1m"] = (m0_close - candle_open) / (candle_open + 1e-10)
    ic["ic_range_1m"] = (m0_high - m0_low) / (candle_open + 1e-10)
    ic["ic_body_1m"] = (m0_close - candle_open) / (m0_high - m0_low + 1e-10)
    ic["ic_upper_wick_1m"] = (m0_high - pd.concat([m0_close, candle_open], axis=1).max(axis=1)) / (candle_open + 1e-10)
    ic["ic_lower_wick_1m"] = (pd.concat([m0_close, candle_open], axis=1).min(axis=1) - m0_low) / (candle_open + 1e-10)
    ic["ic_vol_1m"] = m0_vol

    # Minute 2 features (cumulative first 2 minutes)
    ic["ic_ret_2m"] = (m1_close - candle_open) / (candle_open + 1e-10)
    ic["ic_range_2m"] = (first2_high - first2_low) / (candle_open + 1e-10)
    ic["ic_body_2m"] = (m1_close - candle_open) / (first2_high - first2_low + 1e-10)
    ic["ic_vol_2m"] = first2_vol

    # Minute 3 features (cumulative first 3 minutes)
    ic["ic_ret_3m"] = (m2_close - candle_open) / (candle_open + 1e-10)
    ic["ic_range_3m"] = (first3_high - first3_low) / (candle_open + 1e-10)
    ic["ic_body_3m"] = (m2_close - candle_open) / (first3_high - first3_low + 1e-10)
    ic["ic_vol_3m"] = first3_vol
    ic["ic_upper_wick_3m"] = (first3_high - pd.concat([m2_close, candle_open], axis=1).max(axis=1)) / (candle_open + 1e-10)
    ic["ic_lower_wick_3m"] = (pd.concat([m2_close, candle_open], axis=1).min(axis=1) - first3_low) / (candle_open + 1e-10)

    # Momentum within candle (per-minute changes)
    ic["ic_momentum_1to2"] = (m1_close - m0_close) / (candle_open + 1e-10)
    ic["ic_momentum_2to3"] = (m2_close - m1_close) / (candle_open + 1e-10)
    ic["ic_accel"] = ((m1_close - m0_close) - (m0_close - candle_open)) / (candle_open + 1e-10)
    ic["ic_accel_late"] = ((m2_close - m1_close) - (m1_close - m0_close)) / (candle_open + 1e-10)

    # Where is price vs the range so far
    ic["ic_range_pos_2m"] = (m1_close - first2_low) / (first2_high - first2_low + 1e-10)
    ic["ic_range_pos_3m"] = (m2_close - first3_low) / (first3_high - first3_low + 1e-10)

    # Volume trends
    ic["ic_vol_ratio_1to2"] = m1_vol / (m0_vol + 1e-10)
    ic["ic_vol_ratio_2to3"] = m2_vol / (m1_vol + 1e-10)

    # Is the move continuing or reversing by minute 3?
    ic["ic_reversal_3m"] = np.sign(m1_close - m0_close) != np.sign(m2_close - m1_close)
    ic["ic_reversal_3m"] = ic["ic_reversal_3m"].astype(int)

    # Reference close for price_gap calculation (latest available minute)
    ic["ic_close"] = m2_close

    # Join to 5-min dataframe
    return df_5m.join(ic, how="left")


def build_features_1m(df_5m, df_1m):
    """
    Full feature engineering pipeline using both 5m and 1m data.

    IMPORTANT: To avoid data leakage, technical indicators are computed on
    PREVIOUS candles only (shifted by 1). The current candle's close/high/low
    are unknown at prediction time — we only have the first 1-2 minutes
    via intra-candle features.
    """
    # Save current candle's open and close for target
    candle_open = df_5m["open"].copy()
    candle_close = df_5m["close"].copy()

    # Build a shifted version: each row's OHLCV = previous candle's values
    # This ensures technical indicators only use past data
    df_prev = df_5m.copy()
    df_prev["open"] = df_5m["open"].shift(1)
    df_prev["high"] = df_5m["high"].shift(1)
    df_prev["low"] = df_5m["low"].shift(1)
    df_prev["close"] = df_5m["close"].shift(1)
    df_prev["volume"] = df_5m["volume"].shift(1)
    df_prev.dropna(inplace=True)

    # Compute all technical indicators on previous candle data
    df_prev = add_technical_indicators(df_prev)
    df_prev = add_multi_timeframe_features(df_prev)
    df_prev = add_volume_features(df_prev)
    df_prev = add_time_features(df_prev)
    df_prev = add_streak_features(df_prev)
    df_prev = add_lookback_summary_features(df_prev)

    # Restore actual open/close for target creation
    df_prev["open"] = candle_open
    df_prev["close"] = candle_close

    # Add intra-candle features from 1-min data (current candle's first 1-2 min)
    df_prev = add_intracandle_features(df_prev, df_1m)

    # Create target: does THIS candle close >= open?
    df_prev = create_target_intracandle(df_prev)
    df_prev.dropna(inplace=True)
    return df_prev


def get_feature_columns(df):
    """Return list of feature column names (only model input features)."""
    exclude = {"open", "high", "low", "close", "volume", "close_time",
                "quote_volume", "trades", "taker_buy_base", "taker_buy_quote",
                "target", "volume_sma", "price_vs_target", "ic_close"}
    return [c for c in df.columns if c not in exclude and not c.startswith("poly_")]


def build_features(df):
    """Full feature engineering pipeline."""
    df = add_technical_indicators(df)
    df = add_multi_timeframe_features(df)
    df = add_volume_features(df)
    df = add_time_features(df)
    df = add_streak_features(df)
    df = add_lookback_summary_features(df)
    df = create_target(df)
    df.dropna(inplace=True)
    return df
