"""Training configuration for the BTC 5-minute prediction model."""

import json
from datetime import date
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = PROJECT_DIR / "00-Data"
MODELS_DIR = PROJECT_DIR / "02-Models"
REGISTRY_DIR = MODELS_DIR / "model_registry"


def next_version(model_type: str) -> str:
    """Return the next version tag (e.g. 'v002_2026-03-18') for *model_type*."""
    type_dir = REGISTRY_DIR / model_type
    existing = sorted(type_dir.glob("v???_*")) if type_dir.exists() else []
    seq = len(existing) + 1
    return f"v{seq:03d}_{date.today().isoformat()}"


def registry_paths(model_type: str, version: str | None = None):
    """Return (model_path, scaler_path) inside the registry.

    If *version* is None a new version folder is created.
    """
    if version is None:
        version = next_version(model_type)
    version_dir = REGISTRY_DIR / model_type / version
    version_dir.mkdir(parents=True, exist_ok=True)
    return str(version_dir / "model.lgb"), str(version_dir / "feature_cols.pkl")


def update_active_pointer(model_type: str, version: str) -> None:
    """Set *version* as the active version for *model_type* in model_active.json."""
    pointer_path = MODELS_DIR / "model_active.json"
    data = json.loads(pointer_path.read_text()) if pointer_path.exists() else {}
    data[model_type] = version
    pointer_path.write_text(json.dumps(data, indent=2) + "\n")


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
MODEL_PATH, SCALER_PATH = registry_paths("early_entry")
HISTORICAL_CSV = str(DATA_DIR / "BTCUSDT_5m_2017-09-01_to_2025-09-23.csv")
HISTORICAL_1M_CSV = str(DATA_DIR / "BTCUSD_1m_Bitstamp.csv")
