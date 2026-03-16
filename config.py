"""Configuration for BTC 5-minute prediction model.

Note: Runtime trading parameters (min_bet, max_bet, starting_balance, etc.)
are now stored in the bot_config DB table and editable via the dashboard.
Static model/feature/API config remains here.
"""

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
MODEL_PATH = "model/btc_5m_predictor.lgb"
SCALER_PATH = "model/scaler.pkl"
DATA_CACHE = "data/btc_5m.csv"
HISTORICAL_CSV = "data/BTCUSDT_5m_2017-09-01_to_2025-09-23.csv"
HISTORICAL_1M_CSV = "data/BTCUSD_1m_Bitstamp.csv"

# === Trading Bot Improvements ===
# Consecutive loss protection: drop to MIN_BET after this many consecutive losses
CONSECUTIVE_LOSS_LIMIT = 3

# Momentum protection: skip trade if price is this many ATRs against our direction
MOMENTUM_ATR_THRESHOLD = 1.5

# Confidence calibration: minimum trades needed before applying calibration
CALIBRATION_MIN_TRADES = 50

# Early exit: sell position if unrealized profit >= this % of max possible profit
EARLY_EXIT_PROFIT_PCT = 0.50
EARLY_EXIT_WINDOW = (60, 270)  # Only consider selling between these remaining seconds

# Stop-loss: sell position when unrealized loss exceeds this % of stake
STOP_LOSS_PCT = 0.75

# Position flip: cut loss and reverse when losing heavily and model flips
FLIP_LOSS_PCT = 0.30       # Unrealized loss > this % of stake triggers consideration
FLIP_MIN_REMAINING = 90    # Need at least this many seconds remaining to flip

# Market momentum entry: buy when Polymarket strongly favors one side
POLY_MOMENTUM_ENTRY = 0.80    # Market prices side at 80c+ → buy if model doesn't disagree
POLY_SLAM_ENTRY = 0.90        # Market prices side at 90c+ → early entry (30s+)
POLY_SLAM_MIN_ELAPSED = 30    # Earliest entry for 90c+ slam trades
POLY_MOMENTUM_MAX_BUY = 0.92  # Don't buy above 92c (payout too thin)

# Smart exit: market agreement + volume + conviction gating
MARKET_AGREE_HOLD = 0.50     # Market prices our side >= 50% of entry price → hold
MARKET_DISAGREE_SELL = 0.25  # Market prices our side < 25% of entry price → sell
LOW_VOLUME_THRESHOLD = 500   # Below this $ volume, market signal is thin/unreliable
HIGH_VOLUME_THRESHOLD = 2000 # Above this $ volume, market signal is strong/reliable
CONVICTION_HOLD_THRESHOLD = 0.60  # High-conviction score: trust model over thin market
