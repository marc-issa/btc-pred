"""Train the late management model using LightGBM (GPU-accelerated).

Same prediction target as the early model: will the current 5-min candle
close above or below its open?

Difference: uses intra-candle features from minutes 0-3 (first 240s) instead
of minutes 0-2. This gives the model more price action to work with, simulating
what the bot sees at the 180-240s mark when managing an open position.

Minute 4's close = candle close = target, so it is excluded to avoid leakage.
"""

import json
import os
import pickle
from datetime import date, datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

import config
from features import build_features_1m_late, get_feature_columns


def load_1m_data():
    """Load 1-minute Bitstamp data."""
    path = config.HISTORICAL_1M_CSV
    if not os.path.exists(path):
        raise FileNotFoundError(f"1-min data not found at {path}")

    print(f"Loading 1-minute data from {path}...")
    df = pd.read_csv(path)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df.set_index("datetime", inplace=True)
    df.drop(columns=["timestamp"], inplace=True)
    df = df[["open", "high", "low", "close", "volume"]].astype(float)

    print(f"Loaded {len(df):,} 1-min candles from {df.index[0]} to {df.index[-1]}")
    return df


def resample_to_5m(df_1m):
    """Resample 1-minute candles to 5-minute candles."""
    df_5m = df_1m.resample("5min").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna()
    print(f"Resampled to {len(df_5m):,} 5-min candles")
    return df_5m


def train():
    print("=== Training LATE MANAGEMENT model with minute 0-3 intra-candle features ===\n")

    # 1. Load data
    df_1m = load_1m_data()
    df_5m = resample_to_5m(df_1m)

    # 2. Feature engineering with late intra-candle data (m0-m3)
    print("Building features (5m + late intra-candle from 1m, minutes 0-3)...")
    df = build_features_1m_late(df_5m, df_1m)

    feature_cols = get_feature_columns(df)
    print(f"Features ({len(feature_cols)}): {feature_cols}")
    print(f"Target distribution:\n{df['target'].value_counts(normalize=True)}")

    # 3. Replace inf values
    df[feature_cols] = df[feature_cols].replace([np.inf, -np.inf], np.nan)
    df.dropna(subset=feature_cols, inplace=True)

    X = df[feature_cols].values
    y = df["target"].values
    print(f"Dataset: {X.shape[0]:,} samples, {X.shape[1]} features")

    # 4. Time-series train/val/test split
    train_end = int(len(X) * 0.7)
    val_end = int(len(X) * 0.85)

    X_train, y_train = X[:train_end], y[:train_end]
    X_val, y_val = X[train_end:val_end], y[train_end:val_end]
    X_test, y_test = X[val_end:], y[val_end:]
    print(f"Train: {len(X_train):,}, Val: {len(X_val):,}, Test: {len(X_test):,}")

    # 5. LightGBM datasets
    train_data = lgb.Dataset(X_train, label=y_train, feature_name=feature_cols)
    val_data = lgb.Dataset(X_val, label=y_val, feature_name=feature_cols, reference=train_data)

    # 6. LightGBM parameters — same as early model
    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "boosting_type": "gbdt",
        "learning_rate": 0.01,
        "num_leaves": 63,
        "max_depth": 7,
        "min_child_samples": 500,
        "feature_fraction": 0.6,
        "bagging_fraction": 0.7,
        "bagging_freq": 1,
        "lambda_l1": 1.0,
        "lambda_l2": 5.0,
        "min_gain_to_split": 0.01,
        "verbose": -1,
        "device": "gpu",
        "gpu_use_dp": False,
    }

    # 7. Train
    print("\nTraining LightGBM (late management model)...")
    callbacks = [
        lgb.log_evaluation(period=100),
        lgb.early_stopping(stopping_rounds=100),
    ]

    model = lgb.train(
        params,
        train_data,
        num_boost_round=5000,
        valid_sets=[train_data, val_data],
        valid_names=["train", "val"],
        callbacks=callbacks,
    )

    # 8. Evaluate
    print(f"\n=== Test Set Evaluation (unseen data) ===")
    print(f"Best iteration: {model.best_iteration}")

    y_pred_prob = model.predict(X_test, num_iteration=model.best_iteration)
    y_pred = (y_pred_prob > 0.5).astype(int)

    acc = accuracy_score(y_test, y_pred)
    print(f"  Overall Accuracy: {acc:.4f}")
    print(f"\nClassification Report:")
    print(classification_report(y_test, y_pred, target_names=["DOWN", "UP"]))
    print("Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    # 9. Confidence analysis
    print(f"\n=== Confidence Analysis ===")
    print(f"{'Threshold':>10} {'Accuracy':>10} {'Samples':>10} {'% of Total':>10}")
    print("-" * 45)
    for thresh in [0.50, 0.52, 0.54, 0.56, 0.58, 0.60, 0.65, 0.70]:
        confident = np.abs(y_pred_prob - 0.5) >= (thresh - 0.5)
        if confident.sum() > 0:
            conf_acc = accuracy_score(y_test[confident], (y_pred_prob[confident] > 0.5).astype(int))
            pct = confident.sum() / len(y_test) * 100
            print(f"  >= {thresh:.0%}    {conf_acc:>8.1%}    {confident.sum():>8,}    {pct:>8.1f}%")

    # 10. Simulated P&L
    print(f"\n=== Simulated P&L (bet $1 per trade) ===")
    print(f"{'Strategy':>30} {'Trades':>8} {'Win%':>8} {'P&L':>10} {'ROI':>8}")
    print("-" * 70)
    for thresh in [0.50, 0.55, 0.58, 0.60, 0.65]:
        confident = np.abs(y_pred_prob - 0.5) >= (thresh - 0.5)
        if confident.sum() == 0:
            continue

        preds = (y_pred_prob[confident] > 0.5).astype(int)
        actuals = y_test[confident]

        wins = (preds == actuals).sum()
        losses = len(preds) - wins
        pnl = wins * 0.50 - losses * 0.50
        roi = pnl / (len(preds) * 0.50) * 100
        win_pct = wins / len(preds) * 100

        label = f"Confidence >= {thresh:.0%}"
        print(f"  {label:>28} {len(preds):>8,} {win_pct:>7.1f}% ${pnl:>9,.2f} {roi:>7.1f}%")

    # 11. Feature importance
    importance = model.feature_importance(importance_type="gain")
    feat_imp = sorted(zip(feature_cols, importance), key=lambda x: -x[1])
    print(f"\nTop 25 Features (by gain):")
    for name, imp in feat_imp[:25]:
        print(f"  {name:30s} {imp:,.0f}")

    # 12. Save to registry
    version = config.next_version("late_management")
    model_path, scaler_path = config.registry_paths("late_management", version)

    model.save_model(model_path)
    with open(scaler_path, "wb") as f:
        pickle.dump(feature_cols, f)

    # Save metadata
    metadata = {
        "model_type": "late_management",
        "version": version,
        "algorithm": "LightGBM",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "train_rows": int(len(X_train)),
        "features_count": len(feature_cols),
        "target": "5m candle direction (close >= open)",
        "intracandle_minutes": "0-3 (first 240s of 300s window)",
        "validation_accuracy": float(acc),
        "best_iteration": int(model.best_iteration),
        "notes": "Late management model — same target as early model but with minute 0-3 intra-candle features.",
    }
    metadata_path = Path(model_path).parent / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")

    # Update active pointer
    config.update_active_pointer("late_management", version)

    print(f"\nModel saved to {model_path}")
    print(f"Feature cols saved to {scaler_path}")
    print(f"Metadata saved to {metadata_path}")
    print(f"Active pointer updated: late_management -> {version}")

    return model


if __name__ == "__main__":
    train()
