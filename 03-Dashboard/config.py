"""Runtime configuration for the BTC 5-minute prediction bot.

Runtime trading parameters (min_bet, max_bet, starting_balance, etc.)
are stored in the bot_config DB table and editable via the dashboard.
Static runtime/model/API config remains here.
"""

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = PROJECT_DIR / "00-Data"
MODELS_DIR = PROJECT_DIR / "02-Models"
RUNTIME_DATA_DIR = BASE_DIR / "data"

# === Data Collection ===
BINANCE_BASE = "https://api.binance.com/api/v3"
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB = "https://clob.polymarket.com"

# Binance kline interval
INTERVAL = "5m"
# How many 5-min candles to fetch (1000 = ~3.5 days)
CANDLE_LIMIT = 1000

# Polymarket Bitcoin market search query
POLY_SEARCH_QUERY = "bitcoin"

# === Feature Engineering ===
# Lookback window (number of 5-min candles the model sees)
LOOKBACK = 30

# Technical indicator periods
RSI_PERIOD = 14
EMA_SHORT = 9
EMA_LONG = 21
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
BB_PERIOD = 20
ATR_PERIOD = 14

# Minimum % move to include candle in training (filters noise)
# e.g., 0.03 = candles where |close-open|/open < 0.03% are excluded
MIN_MOVE_PCT = 0.03

# === Model ===
LSTM_UNITS_1 = 128
LSTM_UNITS_2 = 64
DENSE_UNITS = 32
DROPOUT = 0.3
LEARNING_RATE = 1e-3
BATCH_SIZE = 4096
EPOCHS = 50
VALIDATION_SPLIT = 0.2

# === Paths ===
MODEL_PATH = str(MODELS_DIR / "early_btc_5m_predictor.lgb")
SCALER_PATH = str(MODELS_DIR / "early_scaler.pkl")
DATA_CACHE = str(DATA_DIR / "btc_5m.csv")
HISTORICAL_CSV = str(DATA_DIR / "BTCUSDT_5m_2017-09-01_to_2025-09-23.csv")
HISTORICAL_1M_CSV = str(DATA_DIR / "BTCUSD_1m_Bitstamp.csv")

# === Trading Bot ===
# All trading parameters are stored in the bot_config DB table and editable
# via the dashboard. Only calibration_min_trades is kept here as a code-level default.
CALIBRATION_MIN_TRADES = 50
