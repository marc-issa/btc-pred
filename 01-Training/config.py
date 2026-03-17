"""Training configuration for the BTC 5-minute prediction model."""

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = PROJECT_DIR / "00-Data"
MODELS_DIR = PROJECT_DIR / "02-Models"


# === Feature Engineering ===
RSI_PERIOD = 14
EMA_SHORT = 9
EMA_LONG = 21
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
ATR_PERIOD = 14


# === Training Paths ===
MODEL_PATH = str(MODELS_DIR / "early_btc_5m_predictor.lgb")
SCALER_PATH = str(MODELS_DIR / "early_scaler.pkl")
HISTORICAL_CSV = str(DATA_DIR / "BTCUSDT_5m_2017-09-01_to_2025-09-23.csv")
HISTORICAL_1M_CSV = str(DATA_DIR / "BTCUSD_1m_Bitstamp.csv")
