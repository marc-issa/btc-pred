"""
Live prediction: will BTC close the 5-minute candle above or below your price?

Usage:
    python predict.py 71499.66
    python predict.py 71,499.66

Enter the "Price to beat" from Polymarket's BTC Up/Down 5m market:
https://polymarket.com/event/btc-updown-5m-{timestamp}
"""

import sys
import os
import pickle
import numpy as np
import pandas as pd
import lightgbm as lgb

import config
from data_collector import fetch_binance_klines, fetch_polymarket_5m_current
from features import (
    add_technical_indicators,
    add_multi_timeframe_features,
    add_volume_features,
    add_time_features,
    add_streak_features,
    add_lookback_summary_features,
)


def load_model():
    """Load trained LightGBM model and feature columns."""
    model = lgb.Booster(model_file=config.MODEL_PATH)
    with open(config.SCALER_PATH, "rb") as f:
        feature_cols = pickle.load(f)
    return model, feature_cols


def predict(price_to_beat):
    """
    Given a "Price to beat" from Polymarket, predict whether BTC
    will close the 5-min candle above or below that price.
    """
    model, feature_cols = load_model()

    # 1. Fetch Polymarket odds (display only)
    print("Fetching Polymarket BTC Up/Down 5m market...")
    market = fetch_polymarket_5m_current()
    if market:
        print(f"  Market: {market['title']}")
        print(f"  Polymarket odds — Up: {market['up_price']:.0%} | Down: {market['down_price']:.0%}")

    # 2. Fetch recent candles (need 700+ for 4h RSI which uses 14*48=672 periods)
    needed = 750
    print(f"Fetching {needed} recent 5-min candles...")
    df = fetch_binance_klines(limit=needed)
    current_price = df["close"].iloc[-1]

    # 3. Build features (without create_target — we ARE the prediction target)
    df = add_technical_indicators(df)
    df = add_multi_timeframe_features(df)
    df = add_volume_features(df)
    df = add_time_features(df)
    df = add_streak_features(df)
    df = add_lookback_summary_features(df)

    # 4. Add price_gap: how far current price is from the price to beat
    #    This is the key signal — matches what the model learned in training
    df["price_gap"] = (df["close"] - price_to_beat) / (df["atr"] + 1e-10)
    df["price_gap_pct"] = (df["close"] - price_to_beat) / (price_to_beat + 1e-10)
    df.dropna(inplace=True)

    # 5. Replace inf
    df[feature_cols] = df[feature_cols].replace([np.inf, -np.inf], np.nan)
    df[feature_cols] = df[feature_cols].fillna(0)

    # 6. Predict on last row
    X = df[feature_cols].iloc[[-1]].values
    prob = model.predict(X)[0]

    direction = "UP" if prob > 0.5 else "DOWN"
    confidence = prob if prob > 0.5 else 1 - prob
    diff = current_price - price_to_beat

    # Confidence rating
    if confidence >= 0.60:
        signal = "STRONG"
    elif confidence >= 0.55:
        signal = "MODERATE"
    else:
        signal = "WEAK (consider skipping)"

    print(f"\n{'='*60}")
    print(f"  Price to beat:      ${price_to_beat:,.2f}")
    print(f"  BTC Current Price:  ${current_price:,.2f} ({'+' if diff >= 0 else ''}{diff:,.2f})")
    print(f"  Prediction:         Closes {'ABOVE' if direction == 'UP' else 'BELOW'} ${price_to_beat:,.2f}")
    print(f"  Confidence:         {confidence:.1%} — {signal}")
    if market:
        print(f"  Polymarket says:    Up {market['up_price']:.0%} | Down {market['down_price']:.0%}")
        print(f"  Market:             https://polymarket.com/event/{market['slug']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    if not os.path.exists(config.MODEL_PATH):
        print("No trained model found. Run train.py first.")
        sys.exit(1)

    if len(sys.argv) < 2:
        try:
            raw = input("Enter Price to beat from Polymarket: $")
            price = float(raw.replace(",", ""))
        except (ValueError, EOFError):
            print("Usage: python predict.py <price_to_beat>")
            print("Example: python predict.py 71499.66")
            sys.exit(1)
    else:
        price = float(" ".join(sys.argv[1:]).replace(",", ""))

    predict(price)
