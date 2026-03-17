"""
Lightweight analysis dashboard for versioned trades databases.

Usage:
    python analysis_dashboard.py
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template_string, request


BASE_DIR = Path(__file__).resolve().parent
LOCAL_DATA_DIR = BASE_DIR / "data"
DEFAULT_PORT = 5051

app = Flask(__name__)


HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC Model Validation Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  :root {
    --bg:#f4f7fb; --surface:rgba(255,255,255,.96); --panel:#ffffff; --ink:#101828;
    --muted:#475467; --border:#d9e2ec; --accent:#155eef; --accent2:#0f766e;
    --green:#087443; --red:#b42318; --yellow:#b54708; --shadow:0 18px 42px rgba(16,24,40,.08);
  }
  * { box-sizing:border-box; }
  body {
    margin:0; color:var(--ink); font-family:"Segoe UI", Inter, system-ui, sans-serif;
    background:
      radial-gradient(circle at top left, rgba(21,94,239,.10), transparent 34%),
      radial-gradient(circle at top right, rgba(15,118,110,.10), transparent 28%),
      linear-gradient(180deg, #f8fbff 0%, #eef4fa 100%);
  }
  .shell { width:min(1400px, calc(100vw - 24px)); margin:18px auto 28px; }
  .hero, .section { background:var(--surface); border:1px solid var(--border); border-radius:24px; box-shadow:var(--shadow); }
  .hero { overflow:hidden; }
  .hero-grid { display:grid; grid-template-columns:1.2fr .9fr; }
  .hero-copy, .hero-side, .section { padding:22px; }
  .hero-side { border-left:1px solid var(--border); background:linear-gradient(135deg, rgba(21,94,239,.06), rgba(15,118,110,.08)); }
  .eyebrow { display:inline-block; padding:6px 11px; border-radius:999px; background:rgba(0,0,0,.05); color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }
  h1 { margin:14px 0 10px; font-size:clamp(30px, 6vw, 56px); line-height:.95; letter-spacing:-.04em; }
  h2 { margin:0 0 8px; font-size:26px; letter-spacing:-.03em; }
  p, .muted { color:var(--muted); line-height:1.6; }
  .controls { display:grid; gap:10px; }
  label { font-size:11px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; color:var(--muted); }
  .row { display:flex; gap:10px; }
  select, button { border-radius:14px; border:1px solid var(--border); font:inherit; }
  select { flex:1; background:#fff; padding:12px 14px; color:var(--ink); }
  button { background:linear-gradient(135deg, var(--accent), #1d4ed8); color:#f8fbff; padding:12px 18px; font-weight:700; cursor:pointer; }
  .pills { display:flex; flex-wrap:wrap; gap:8px; margin-top:10px; }
  .pill { padding:6px 11px; border-radius:999px; background:rgba(0,0,0,.05); color:var(--muted); font-size:12px; }
  .warns { display:grid; gap:10px; padding:16px 22px 22px; border-top:1px solid var(--border); background:rgba(181,71,8,.05); }
  .warn { padding:11px 13px; border:1px solid rgba(181,71,8,.16); border-radius:14px; background:rgba(255,244,237,.82); color:#8a2c0d; }
  .section { margin-top:18px; }
  .cards { display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:12px; margin-top:14px; }
  .card { min-height:124px; padding:16px; border:1px solid var(--border); border-radius:18px; background:var(--panel); }
  .card.green { background:linear-gradient(160deg, rgba(8,116,67,.08), #fff); }
  .card.red { background:linear-gradient(160deg, rgba(180,35,24,.08), #fff); }
  .card.yellow { background:linear-gradient(160deg, rgba(181,71,8,.09), #fff); }
  .card.blue { background:linear-gradient(160deg, rgba(21,94,239,.08), #fff); }
  .label { color:var(--muted); font-size:11px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; }
  .value { margin:10px 0 4px; font-size:clamp(24px, 3vw, 38px); line-height:1; letter-spacing:-.05em; }
  .sub { color:var(--muted); font-size:13px; line-height:1.5; }
  .tables { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-top:16px; }
  .table-card { border:1px solid var(--border); border-radius:18px; background:#fff; overflow:hidden; }
  .table-head { padding:13px 15px; border-bottom:1px solid var(--border); background:rgba(21,94,239,.05); }
  .table-head h3 { margin:0 0 4px; font-size:16px; }
  .table-head p { margin:0; font-size:13px; }
  .table-wrap { overflow:auto; }
  table { width:100%; min-width:420px; border-collapse:collapse; }
  th, td { padding:11px 13px; border-bottom:1px solid var(--border); text-align:left; vertical-align:top; }
  th { background:#f8fafc; color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.08em; }
  tr.row-green td { background:rgba(8,116,67,.05); }
  tr.row-yellow td { background:rgba(181,71,8,.06); }
  tr.row-red td { background:rgba(180,35,24,.06); }
  .status-pill { display:inline-flex; align-items:center; gap:6px; padding:4px 9px; border-radius:999px; font-size:11px; font-weight:700; letter-spacing:.04em; }
  .status-pass { color:#065f46; background:rgba(8,116,67,.12); }
  .status-warn { color:#92400e; background:rgba(181,71,8,.14); }
  .status-fail { color:#991b1b; background:rgba(180,35,24,.12); }
  .status-na { color:#475467; background:rgba(71,84,103,.10); }
  .chart-grid { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-top:16px; }
  .chart-card { border:1px solid var(--border); border-radius:18px; background:#fff; overflow:hidden; }
  .chart-head { padding:13px 15px; border-bottom:1px solid var(--border); background:rgba(15,118,110,.05); }
  .chart-head h3 { margin:0 0 4px; font-size:16px; }
  .chart-head p { margin:0; color:var(--muted); font-size:13px; }
  .chart-body { padding:14px; }
  .chart-canvas { position:relative; height:280px; }
  tr:last-child td { border-bottom:none; }
  .empty { padding:16px; color:var(--muted); font-style:italic; }
  .footer { margin-top:16px; text-align:center; color:var(--muted); font-size:13px; }
  @media (max-width: 1180px) { .hero-grid, .cards, .tables, .chart-grid { grid-template-columns:1fr; } .hero-side { border-left:none; border-top:1px solid var(--border); } }
  @media (max-width: 720px) { .shell { width:min(100vw - 12px, 1400px); margin:8px auto 18px; } .hero-copy, .hero-side, .section { padding:18px; } .row { flex-direction:column; } }
</style>
</head>
<body>
  <div class="shell">
    <div class="hero">
      <div class="hero-grid">
        <div class="hero-copy">
          <div class="eyebrow">Data Review Studio</div>
          <h1>Model validation dashboard</h1>
          <p>Switch between database versions, refresh the selected file, and inspect live model performance the way you would in a validation notebook: scorecards, calibration slices, cohort analysis, and data-coverage checks based on the schema in <code>03-Dashboard/auto.py</code>.</p>
        </div>
        <div class="hero-side">
          <form method="get" class="controls">
            <label for="db">Database version</label>
            <div class="row">
              <select id="db" name="db">
                {% for db in databases %}
                <option value="{{ db.key }}" {% if selected and db.key == selected.key %}selected{% endif %}>{{ db.option_label }}</option>
                {% endfor %}
              </select>
              <button type="submit">Refresh</button>
            </div>
          </form>
          <div class="pills">
            <div class="pill">Scan root: 04-Analysis/data</div>
          </div>
          {% if selected %}
          <div class="muted">Selected: <strong>{{ selected.name }}</strong><br>Source: {{ selected.source_label }}<br>Path: {{ selected.path_display }}</div>
          {% else %}
          <div class="muted">No databases found yet. Add <code>.db</code> files to <code>04-Analysis/data</code>.</div>
          {% endif %}
        </div>
      </div>
      {% if warnings %}
      <div class="warns">
        {% for warning in warnings %}
        <div class="warn">{{ warning }}</div>
        {% endfor %}
      </div>
      {% endif %}
    </div>

    {% if report %}
    <section class="section">
      <h2>Overview</h2>
      <div class="muted">Top-level validation summary for the selected database version.</div>
      <div class="cards">
        {% for card in report.overview_cards %}
        <div class="card {{ card.tone }}">
          <div class="label">{{ card.label }}</div>
          <div class="value">{{ card.value }}</div>
          <div class="sub">{{ card.sub }}</div>
        </div>
        {% endfor %}
      </div>
    </section>

    {% if report.chart_panels %}
    <section class="section">
      <h2>Notebook Graphs</h2>
      <div class="muted">Core validation visuals you would usually inspect in a notebook before trusting a model live.</div>
      <div class="chart-grid">
        {% for chart in report.chart_panels %}
        <div class="chart-card">
          <div class="chart-head">
            <h3>{{ chart.title }}</h3>
            <p>{{ chart.description }}</p>
          </div>
          <div class="chart-body">
            <div class="chart-canvas"><canvas id="{{ chart.id }}"></canvas></div>
          </div>
        </div>
        {% endfor %}
      </div>
    </section>
    {% endif %}

    {% for section in report.sections %}
    <section class="section">
      <h2>{{ section.title }}</h2>
      <div class="muted">{{ section.description }}</div>
      {% if section.cards %}
      <div class="cards">
        {% for card in section.cards %}
        <div class="card {{ card.tone }}">
          <div class="label">{{ card.label }}</div>
          <div class="value">{{ card.value }}</div>
          <div class="sub">{{ card.sub }}</div>
        </div>
        {% endfor %}
      </div>
      {% endif %}
      <div class="tables">
        {% for table in section.tables %}
        <div class="table-card">
          <div class="table-head">
            <h3>{{ table.title }}</h3>
            <p>{{ table.description }}</p>
          </div>
          {% if table.rows %}
          <div class="table-wrap">
            <table>
              <thead><tr>{% for col in table.columns %}<th>{{ col }}</th>{% endfor %}</tr></thead>
              <tbody>
                {% for row in table.rows %}
                <tr class="row-{{ table.row_tones[loop.index0] if table.row_tones else '' }}">
                  {% for value in row %}
                  <td>
                    {% if value in ['PASS','WARN','FAIL','N/A'] %}
                    <span class="status-pill status-{{ value.lower() if value != 'N/A' else 'na' }}">{{ value }}</span>
                    {% else %}
                    {{ value }}
                    {% endif %}
                  </td>
                  {% endfor %}
                </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
          {% else %}
          <div class="empty">{{ table.empty_message }}</div>
          {% endif %}
        </div>
        {% endfor %}
      </div>
    </section>
    {% endfor %}
    {% endif %}

    <div class="footer">Generated at {{ generated_at }}.</div>
  </div>
  {% if report and report.chart_panels %}
  <script>
    const chartPanels = {{ report.chart_panels | tojson }};
    const commonOptions = {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { labels: { color: '#475467' } } },
      scales: {
        x: { ticks: { color: '#475467' }, grid: { color: '#e6edf4' } },
        y: { ticks: { color: '#475467' }, grid: { color: '#e6edf4' } }
      }
    };

    for (const panel of chartPanels) {
      const canvas = document.getElementById(panel.id);
      if (!canvas) continue;
      new Chart(canvas, {
        type: panel.type,
        data: panel.data,
        options: Object.assign({}, commonOptions, panel.options || {})
      });
    }
  </script>
  {% endif %}
</body>
</html>
"""


def ensure_data_dir() -> None:
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)


def fmt_num(value, digits: int = 2, default: str = "n/a") -> str:
    if value is None:
        return default
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def fmt_int(value, default: str = "0") -> str:
    if value is None:
        return default
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def fmt_pct(value, digits: int = 2, default: str = "n/a") -> str:
    if value is None:
        return default
    try:
        return f"{float(value):.{digits}f}%"
    except (TypeError, ValueError):
        return str(value)


def fmt_money(value, digits: int = 2, default: str = "n/a") -> str:
    if value is None:
        return default
    try:
        number = float(value)
        sign = "+" if number > 0 else ""
        return f"{sign}${number:,.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def fmt_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    return f"{num_bytes / (1024 * 1024):.2f} MB"


def fmt_stamp(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def make_card(label: str, value: str, sub: str, tone: str = "blue") -> dict:
    return {"label": label, "value": value, "sub": sub, "tone": tone}


def make_table(title: str, description: str, columns: list[str], rows: list[list[str]], empty_message: str) -> dict:
    return {
        "title": title,
        "description": description,
        "columns": columns,
        "rows": rows,
        "empty_message": empty_message,
        "row_tones": [],
    }


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def query_one(conn: sqlite3.Connection, sql: str) -> dict:
    row = conn.execute(sql).fetchone()
    return dict(row) if row else {}


def query_all(conn: sqlite3.Connection, sql: str) -> list[dict]:
    return [dict(row) for row in conn.execute(sql).fetchall()]


def get_schema(conn: sqlite3.Connection) -> dict[str, list[str]]:
    schema: dict[str, list[str]] = {}
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    for row in rows:
        table = row["name"]
        schema[table] = [col["name"] for col in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return schema


def discover_databases() -> list[dict]:
    ensure_data_dir()
    found: list[dict] = []
    for path in LOCAL_DATA_DIR.glob("*.db"):
        stat = path.stat()
        rel = path.relative_to(BASE_DIR.parent)
        found.append(
            {
                "key": path.name,
                "name": path.name,
                "path": path,
                "path_display": str(rel).replace("\\", "/"),
                "source_key": "analysis",
                "source_label": "Analysis Data",
                "mtime": stat.st_mtime,
                "size": stat.st_size,
                "option_label": f"{path.name} | Analysis Data | {fmt_stamp(stat.st_mtime)} | {fmt_size(stat.st_size)}",
            }
        )
    found.sort(key=lambda item: (-item["mtime"], item["name"].lower()))
    return found


def select_database(databases: list[dict], selected_key: str | None) -> dict | None:
    if not databases:
        return None
    if selected_key:
        for db in databases:
            if db["key"] == selected_key:
                return db
    return databases[0]


def table_exists(schema: dict[str, list[str]], table: str) -> bool:
    return table in schema


def row_counts(conn: sqlite3.Connection, schema: dict[str, list[str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in schema:
        counts[table] = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
    return counts


def status_tone(status: str) -> str:
    return {
        "pass": "green",
        "warn": "yellow",
        "fail": "red",
    }.get(status, "blue")


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lower = int(pos)
    upper = min(lower + 1, len(ordered) - 1)
    weight = pos - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def row_tone_from_status(status: str) -> str:
    return {
        "pass": "green",
        "warn": "yellow",
        "fail": "red",
    }.get(status, "")


def validation_status(value: float | None, pass_min: float | None = None, warn_min: float | None = None,
                      pass_max: float | None = None, warn_max: float | None = None) -> str:
    if value is None:
        return "na"
    if pass_min is not None:
        if value >= pass_min:
            return "pass"
        if warn_min is not None and value >= warn_min:
            return "warn"
        return "fail"
    if pass_max is not None:
        if value <= pass_max:
            return "pass"
        if warn_max is not None and value <= warn_max:
            return "warn"
        return "fail"
    return "na"


def build_validation_metrics(conn: sqlite3.Connection, schema: dict[str, list[str]]) -> tuple[list[dict], dict]:
    if not table_exists(schema, "trades"):
        return [], {}

    trades = query_all(conn, """
        SELECT * FROM trades
        WHERE action != 'skip' AND won IS NOT NULL
        ORDER BY id
    """)
    preds = query_all(conn, """
        SELECT * FROM predictions
        WHERE traded = 1
        ORDER BY id
    """) if table_exists(schema, "predictions") else []

    n = len(trades)
    if n == 0:
        return [], {}

    wins = [t for t in trades if t.get("won") == 1]
    losses = [t for t in trades if t.get("won") == 0]
    pnls = [float(t.get("pnl") or 0) for t in trades]
    win_pnls = [float(t.get("pnl") or 0) for t in wins]
    loss_pnls = [float(t.get("pnl") or 0) for t in losses]
    actual_wr = len(wins) / n if n else 0
    total_pnl = sum(pnls)
    ev = total_pnl / n if n else 0
    avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0
    avg_loss = abs(sum(loss_pnls) / len(loss_pnls)) if loss_pnls else 0
    wl_ratio = avg_win / avg_loss if avg_loss > 0 else 0
    gross_profit = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p < 0))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (99.0 if gross_profit > 0 else 0.0)

    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnls:
        cum += pnl
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    starting_balance = 100.0
    if table_exists(schema, "bot_config"):
        row = conn.execute("SELECT value FROM bot_config WHERE key='starting_balance'").fetchone()
        if row and row["value"] is not None:
            try:
                starting_balance = float(row["value"])
            except (TypeError, ValueError):
                pass
    dd_pct = (max_dd / starting_balance * 100) if starting_balance > 0 else 0

    avg_conf = None
    if "confidence" in schema.get("trades", []):
        conf_values = [float(t.get("confidence")) for t in trades if t.get("confidence") is not None]
        avg_conf = sum(conf_values) / len(conf_values) if conf_values else None
    cal_error = abs(actual_wr - avg_conf) if avg_conf is not None else None

    if n >= 2:
        mean_pnl = total_pnl / n
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / (n - 1)
        std_pnl = variance ** 0.5
        sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0
    else:
        sharpe = 0

    rolling_window = min(50, max(10, n // 3))
    min_rolling_wr = actual_wr * 100
    if n >= rolling_window:
        min_rolling_wr = 100.0
        for idx in range(rolling_window, n + 1):
            window = trades[idx - rolling_window:idx]
            wr = sum(1 for t in window if t.get("won") == 1) / len(window) * 100
            min_rolling_wr = min(min_rolling_wr, wr)

    prediction_match_rate = None
    avg_prediction_conf = None
    precision_up = None
    recall_up = None
    f1_up = None
    specificity = None
    balanced_accuracy = None
    confusion = {}
    if table_exists(schema, "predictions"):
        joined = query_one(conn, """
            SELECT
                COUNT(*) AS matched_windows,
                ROUND(100.0 * SUM(CASE WHEN p.direction = t.actual THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS direction_hit_rate,
                ROUND(AVG(p.confidence), 3) AS avg_pred_conf
            FROM predictions p
            JOIN trades t ON p.window_ts = t.window_ts
            WHERE p.direction IS NOT NULL AND t.action != 'skip' AND t.actual IN ('UP', 'DOWN')
        """)
        prediction_match_rate = joined.get("direction_hit_rate")
        avg_prediction_conf = joined.get("avg_pred_conf")
        confusion = query_one(conn, """
            SELECT
                SUM(CASE WHEN p.direction = 'UP' AND t.actual = 'UP' THEN 1 ELSE 0 END) AS tp,
                SUM(CASE WHEN p.direction = 'UP' AND t.actual = 'DOWN' THEN 1 ELSE 0 END) AS fp,
                SUM(CASE WHEN p.direction = 'DOWN' AND t.actual = 'UP' THEN 1 ELSE 0 END) AS fn,
                SUM(CASE WHEN p.direction = 'DOWN' AND t.actual = 'DOWN' THEN 1 ELSE 0 END) AS tn
            FROM predictions p
            JOIN trades t ON p.window_ts = t.window_ts
            WHERE p.direction IN ('UP', 'DOWN') AND t.action != 'skip' AND t.actual IN ('UP', 'DOWN')
        """)
        tp = int(confusion.get("tp") or 0)
        fp = int(confusion.get("fp") or 0)
        fn = int(confusion.get("fn") or 0)
        tn = int(confusion.get("tn") or 0)
        precision_up = tp / (tp + fp) if (tp + fp) else None
        recall_up = tp / (tp + fn) if (tp + fn) else None
        specificity = tn / (tn + fp) if (tn + fp) else None
        if precision_up is not None and recall_up is not None and (precision_up + recall_up) > 0:
            f1_up = 2 * precision_up * recall_up / (precision_up + recall_up)
        if recall_up is not None and specificity is not None:
            balanced_accuracy = (recall_up + specificity) / 2

    pnl_q1 = percentile(pnls, 0.25)
    pnl_q3 = percentile(pnls, 0.75)
    pnl_iqr = (pnl_q3 - pnl_q1) if pnl_q1 is not None and pnl_q3 is not None else None
    low_fence = (pnl_q1 - 1.5 * pnl_iqr) if pnl_iqr is not None else None
    high_fence = (pnl_q3 + 1.5 * pnl_iqr) if pnl_iqr is not None else None
    pnl_outliers = [p for p in pnls if low_fence is not None and high_fence is not None and (p < low_fence or p > high_fence)]
    outlier_rate = len(pnl_outliers) / n if n else None

    metrics = [
        {
            "name": "Sample Size",
            "display": fmt_int(n),
            "threshold": "Prefer >= 500 live trades",
            "detail": f"{max(0, 500 - n)} more trades needed for a stronger live sample." if n < 500 else "Live sample is large enough for stronger confidence.",
            "status": "pass" if n >= 500 else "warn" if n >= 200 else "fail",
        },
        {
            "name": "Win Rate",
            "display": fmt_pct(actual_wr * 100),
            "threshold": "Accept >= 55%",
            "detail": f"{len(wins)} wins and {len(losses)} losses.",
            "status": "pass" if actual_wr >= 0.55 else "warn" if actual_wr >= 0.50 else "fail",
        },
        {
            "name": "Expected Value",
            "display": fmt_money(ev, 3),
            "threshold": "Expect positive average trade PnL",
            "detail": f"Total realized PnL {fmt_money(total_pnl)} across {fmt_int(n)} trades.",
            "status": "pass" if ev > 0 else "warn" if ev > -0.2 else "fail",
        },
        {
            "name": "Profit Factor",
            "display": fmt_num(profit_factor, 2),
            "threshold": "Accept >= 1.30",
            "detail": f"Gross profit {fmt_money(gross_profit)} vs gross loss {fmt_money(-gross_loss)}.",
            "status": "pass" if profit_factor >= 1.3 else "warn" if profit_factor >= 1.0 else "fail",
        },
        {
            "name": "Win/Loss Ratio",
            "display": fmt_num(wl_ratio, 2),
            "threshold": "Accept >= 1.00",
            "detail": f"Average win {fmt_money(avg_win, 3)} and average loss {fmt_money(-avg_loss, 3)}.",
            "status": "pass" if wl_ratio >= 1.0 else "warn" if wl_ratio >= 0.8 else "fail",
        },
        {
            "name": "Max Drawdown",
            "display": f"{fmt_money(max_dd)} / {fmt_pct(dd_pct)}",
            "threshold": "Prefer <= 25% of starting balance",
            "detail": f"Starting balance reference {fmt_money(starting_balance)}.",
            "status": "pass" if dd_pct <= 25 else "warn" if dd_pct <= 35 else "fail",
        },
        {
            "name": "Calibration Error",
            "display": fmt_pct((cal_error or 0) * 100) if cal_error is not None else "n/a",
            "threshold": "Prefer < 5% gap",
            "detail": "Average trade confidence vs actual win rate." if cal_error is not None else "Confidence data is unavailable in this DB version.",
            "status": "pass" if cal_error is not None and cal_error < 0.05 else "warn" if cal_error is not None and cal_error < 0.10 else "fail" if cal_error is not None else "na",
        },
        {
            "name": "Rolling Stability",
            "display": fmt_pct(min_rolling_wr),
            "threshold": f"No drop below 50% over rolling {rolling_window} trades",
            "detail": f"Worst rolling window size used: {rolling_window}.",
            "status": "pass" if min_rolling_wr >= 50 else "warn" if min_rolling_wr >= 45 else "fail",
        },
        {
            "name": "Prediction Match",
            "display": fmt_pct(prediction_match_rate) if prediction_match_rate is not None else "n/a",
            "threshold": "Direction should beat 50%",
            "detail": f"Joined prediction confidence {fmt_num(avg_prediction_conf, 3)}." if prediction_match_rate is not None else "Predictions table is missing or cannot be joined to trades.",
            "status": "pass" if prediction_match_rate is not None and prediction_match_rate >= 55 else "warn" if prediction_match_rate is not None and prediction_match_rate >= 50 else "fail" if prediction_match_rate is not None else "na",
        },
        {
            "name": "Precision (UP)",
            "display": fmt_pct(precision_up * 100) if precision_up is not None else "n/a",
            "threshold": "Accept >= 55%",
            "detail": "Of the trades predicted UP, how many were actually UP.",
            "status": "pass" if precision_up is not None and precision_up >= 0.55 else "warn" if precision_up is not None and precision_up >= 0.50 else "fail" if precision_up is not None else "na",
        },
        {
            "name": "Recall (UP)",
            "display": fmt_pct(recall_up * 100) if recall_up is not None else "n/a",
            "threshold": "Accept >= 55%",
            "detail": "Of the true UP outcomes, how many the model captured as UP.",
            "status": "pass" if recall_up is not None and recall_up >= 0.55 else "warn" if recall_up is not None and recall_up >= 0.50 else "fail" if recall_up is not None else "na",
        },
        {
            "name": "F1 Score (UP)",
            "display": fmt_num(f1_up, 3) if f1_up is not None else "n/a",
            "threshold": "Accept >= 0.55",
            "detail": "Harmonic mean of precision and recall for the UP class.",
            "status": "pass" if f1_up is not None and f1_up >= 0.55 else "warn" if f1_up is not None and f1_up >= 0.50 else "fail" if f1_up is not None else "na",
        },
        {
            "name": "Balanced Accuracy",
            "display": fmt_pct(balanced_accuracy * 100) if balanced_accuracy is not None else "n/a",
            "threshold": "Accept >= 55%",
            "detail": "Average of UP recall and DOWN recall to reduce class imbalance bias.",
            "status": "pass" if balanced_accuracy is not None and balanced_accuracy >= 0.55 else "warn" if balanced_accuracy is not None and balanced_accuracy >= 0.50 else "fail" if balanced_accuracy is not None else "na",
        },
        {
            "name": "PnL Outlier Rate",
            "display": fmt_pct((outlier_rate or 0) * 100) if outlier_rate is not None else "n/a",
            "threshold": "Prefer <= 5%",
            "detail": "Share of trades outside Tukey IQR fences. High rates suggest unstable tails or execution anomalies.",
            "status": "pass" if outlier_rate is not None and outlier_rate <= 0.05 else "warn" if outlier_rate is not None and outlier_rate <= 0.10 else "fail" if outlier_rate is not None else "na",
        },
    ]

    summary = {
        "trade_count": n,
        "win_rate_pct": actual_wr * 100,
        "total_pnl": total_pnl,
        "ev": ev,
        "profit_factor": profit_factor,
        "drawdown_pct": dd_pct,
        "calibration_error_pct": (cal_error * 100) if cal_error is not None else None,
        "rolling_min_pct": min_rolling_wr,
        "prediction_match_rate": prediction_match_rate,
        "avg_prediction_conf": avg_prediction_conf,
        "sharpe": sharpe,
        "precision_up_pct": precision_up * 100 if precision_up is not None else None,
        "recall_up_pct": recall_up * 100 if recall_up is not None else None,
        "f1_up": f1_up,
        "balanced_accuracy_pct": balanced_accuracy * 100 if balanced_accuracy is not None else None,
        "outlier_rate_pct": outlier_rate * 100 if outlier_rate is not None else None,
        "confusion": confusion,
        "pnl_low_fence": low_fence,
        "pnl_high_fence": high_fence,
    }
    return metrics, summary


def build_validation_section(conn: sqlite3.Connection, schema: dict[str, list[str]]) -> dict:
    metrics, summary = build_validation_metrics(conn, schema)
    if not metrics:
        return {
            "title": "Validation Scorecard",
            "description": "Notebook-style live validation metrics for the currently selected model database.",
            "cards": [],
            "tables": [make_table("Validation", "No executed trades are available yet.", [], [], "No executed trade data yet.")],
        }

    pass_count = sum(1 for metric in metrics if metric["status"] == "pass")
    warn_count = sum(1 for metric in metrics if metric["status"] == "warn")
    fail_count = sum(1 for metric in metrics if metric["status"] == "fail")
    if pass_count >= 6 and fail_count == 0:
        verdict = ("Validation Verdict", "Healthy", "Most core live metrics are meeting the current bar.", "green")
    elif pass_count >= 4 and fail_count <= 2:
        verdict = ("Validation Verdict", "Mixed", "The model shows edge, but some metrics still need work.", "yellow")
    else:
        verdict = ("Validation Verdict", "Weak", "The live evidence is not strong enough yet.", "red")

    cards = [
        make_card(verdict[0], verdict[1], verdict[2], verdict[3]),
        make_card("Live Sample", fmt_int(summary.get("trade_count")), "Executed trades used for the scorecard.", status_tone(metrics[0]["status"])),
        make_card("Win Rate", fmt_pct(summary.get("win_rate_pct")), "Headline directional accuracy after execution.", status_tone(metrics[1]["status"])),
        make_card("Expected Value", fmt_money(summary.get("ev"), 3), "Average realized PnL per executed trade.", status_tone(metrics[2]["status"])),
        make_card("Profit Factor", fmt_num(summary.get("profit_factor"), 2), "Gross profit divided by gross loss.", status_tone(metrics[3]["status"])),
        make_card("Drawdown", fmt_pct(summary.get("drawdown_pct")), "Peak-to-trough drawdown from realized equity.", status_tone(metrics[5]["status"])),
        make_card("Calibration Error", fmt_pct(summary.get("calibration_error_pct")), "Gap between confidence and realized win rate.", status_tone(metrics[6]["status"]) if summary.get("calibration_error_pct") is not None else "blue"),
        make_card("Rolling Floor", fmt_pct(summary.get("rolling_min_pct")), "Worst rolling win rate across the sample.", status_tone(metrics[7]["status"])),
        make_card("F1 Score", fmt_num(summary.get("f1_up"), 3), "Precision/recall balance for UP predictions.", status_tone(metrics[11]["status"]) if summary.get("f1_up") is not None else "blue"),
        make_card("Outlier Rate", fmt_pct(summary.get("outlier_rate_pct")), "Share of PnL tail events outside IQR fences.", status_tone(metrics[13]["status"]) if summary.get("outlier_rate_pct") is not None else "blue"),
    ]

    metric_table = make_table(
        "Validation Metrics",
        "Core metrics you would usually inspect in a notebook before trusting a live strategy.",
        ["Metric", "Value", "Threshold", "Interpretation", "Status"],
        [[metric["name"], metric["display"], metric["threshold"], metric["detail"], metric["status"].upper()] for metric in metrics],
        "No validation metrics available.",
    )
    metric_table["row_tones"] = [row_tone_from_status(metric["status"]) for metric in metrics]

    summary_table = make_table(
        "Score Summary",
        "A compact view of how many metrics passed, warned, or failed.",
        ["Outcome", "Count"],
        [["Pass", fmt_int(pass_count)], ["Warn", fmt_int(warn_count)], ["Fail", fmt_int(fail_count)]],
        "No summary available.",
    )
    summary_table["row_tones"] = ["green", "yellow", "red"]

    tables = [metric_table, summary_table]

    return {
        "title": "Validation Scorecard",
        "description": "Notebook-style live validation metrics for the currently selected model database.",
        "cards": cards,
        "tables": tables,
    }


def build_notebook_diagnostics_section(conn: sqlite3.Connection, schema: dict[str, list[str]]) -> dict:
    tables: list[dict] = []
    cards: list[dict] = []

    if not table_exists(schema, "trades"):
        return {
            "title": "Notebook Diagnostics",
            "description": "Performance slices that help explain why the model is or is not working.",
            "cards": [],
            "tables": [make_table("Diagnostics", "This database has no trades table.", [], [], "No trade diagnostics available.")],
        }

    trade_cols = set(schema.get("trades", []))
    winner_gap = query_one(conn, f"""
        SELECT
            ROUND(AVG(CASE WHEN won = 1 THEN confidence END), 3) AS win_conf,
            ROUND(AVG(CASE WHEN won = 0 THEN confidence END), 3) AS loss_conf,
            ROUND(AVG(CASE WHEN won = 1 THEN edge_val END), 4) AS win_edge,
            ROUND(AVG(CASE WHEN won = 0 THEN edge_val END), 4) AS loss_edge
        FROM trades
        WHERE action != 'skip'
    """) if "confidence" in trade_cols and "edge_val" in trade_cols else {}

    if winner_gap:
        conf_gap = None
        edge_gap = None
        if winner_gap.get("win_conf") is not None and winner_gap.get("loss_conf") is not None:
            conf_gap = winner_gap["win_conf"] - winner_gap["loss_conf"]
        if winner_gap.get("win_edge") is not None and winner_gap.get("loss_edge") is not None:
            edge_gap = winner_gap["win_edge"] - winner_gap["loss_edge"]
        cards.extend([
            make_card("Confidence Separation", fmt_num(conf_gap, 3), "Positive means winning trades had stronger confidence than losing trades.", "green" if conf_gap and conf_gap > 0 else "red" if conf_gap is not None else "blue"),
            make_card("Edge Separation", fmt_num(edge_gap, 4), "Positive means winners were entered with a better edge estimate.", "green" if edge_gap and edge_gap > 0 else "red" if edge_gap is not None else "blue"),
        ])

    edge_rows = query_all(conn, """
        SELECT
            CASE
                WHEN edge_val < 0.03 THEN '< 0.03'
                WHEN edge_val < 0.05 THEN '0.03 - 0.05'
                WHEN edge_val < 0.10 THEN '0.05 - 0.10'
                WHEN edge_val < 0.15 THEN '0.10 - 0.15'
                ELSE '0.15+'
            END AS bucket,
            COUNT(*) AS trades,
            ROUND(100.0 * SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS win_rate_pct,
            ROUND(AVG(pnl), 3) AS avg_pnl
        FROM trades
        WHERE action != 'skip' AND edge_val IS NOT NULL
        GROUP BY bucket
        ORDER BY MIN(edge_val)
    """) if "edge_val" in trade_cols else []
    tables.append(make_table(
        "Edge Cohorts",
        "Validation-style slice to check whether higher estimated edge really leads to better realized outcomes.",
        ["Edge Bucket", "Trades", "Win Rate", "Avg PnL"],
        [[row["bucket"], fmt_int(row["trades"]), fmt_pct(row["win_rate_pct"]), fmt_money(row["avg_pnl"], 3)] for row in edge_rows],
        "No edge data is available in this DB version.",
    ))
    tables[-1]["row_tones"] = [row_tone_from_status(validation_status(row["win_rate_pct"], pass_min=55, warn_min=50)) for row in edge_rows]

    confidence_rows = query_all(conn, """
        SELECT
            CASE
                WHEN confidence < 0.55 THEN '< 0.55'
                WHEN confidence < 0.60 THEN '0.55 - 0.60'
                WHEN confidence < 0.65 THEN '0.60 - 0.65'
                WHEN confidence < 0.70 THEN '0.65 - 0.70'
                WHEN confidence < 0.75 THEN '0.70 - 0.75'
                ELSE '0.75+'
            END AS bucket,
            COUNT(*) AS trades,
            ROUND(100.0 * SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS win_rate_pct,
            ROUND(AVG(pnl), 3) AS avg_pnl
        FROM trades
        WHERE action != 'skip' AND confidence IS NOT NULL
        GROUP BY bucket
        ORDER BY MIN(confidence)
    """) if "confidence" in trade_cols else []
    tables.append(make_table(
        "Confidence Cohorts",
        "Notebook-style confidence calibration slice: stronger confidence should hold more edge, not less.",
        ["Confidence Bucket", "Trades", "Win Rate", "Avg PnL"],
        [[row["bucket"], fmt_int(row["trades"]), fmt_pct(row["win_rate_pct"]), fmt_money(row["avg_pnl"], 3)] for row in confidence_rows],
        "No confidence data is available in this DB version.",
    ))
    tables[-1]["row_tones"] = [row_tone_from_status(validation_status(row["win_rate_pct"], pass_min=55, warn_min=50)) for row in confidence_rows]

    exit_rows = query_all(conn, """
        SELECT
            COALESCE(exit_reason, '(none)') AS exit_reason,
            COUNT(*) AS trades,
            ROUND(100.0 * SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS win_rate_pct,
            ROUND(SUM(COALESCE(pnl, 0)), 2) AS total_pnl
        FROM trades
        WHERE action != 'skip'
        GROUP BY COALESCE(exit_reason, '(none)')
        ORDER BY trades DESC, exit_reason
    """) if "exit_reason" in trade_cols else []
    tables.append(make_table(
        "Exit Path Attribution",
        "How much of the model's performance is coming from each management path.",
        ["Exit Reason", "Trades", "Win Rate", "Total PnL"],
        [[row["exit_reason"], fmt_int(row["trades"]), fmt_pct(row["win_rate_pct"]), fmt_money(row["total_pnl"])] for row in exit_rows],
        "No exit_reason data is available in this DB version.",
    ))
    tables[-1]["row_tones"] = [row_tone_from_status(validation_status(row["win_rate_pct"], pass_min=55, warn_min=50)) for row in exit_rows]

    if table_exists(schema, "predictions"):
        pred_rows = query_all(conn, """
            SELECT
                COALESCE(direction, '(none)') AS direction,
                COUNT(*) AS rows_count,
                ROUND(AVG(confidence), 3) AS avg_confidence,
                ROUND(AVG(prob_up), 3) AS avg_prob_up,
                ROUND(AVG(edge_val), 4) AS avg_edge
            FROM predictions
            GROUP BY COALESCE(direction, '(none)')
            ORDER BY rows_count DESC, direction
        """)
        tables.append(make_table(
            "Prediction Distribution",
            "Direction mix and average confidence from the predictions table.",
            ["Direction", "Rows", "Avg Confidence", "Avg Prob Up", "Avg Edge"],
            [[row["direction"], fmt_int(row["rows_count"]), fmt_num(row["avg_confidence"], 3), fmt_num(row["avg_prob_up"], 3), fmt_num(row["avg_edge"], 4)] for row in pred_rows],
            "No prediction rows are available yet.",
        ))
        tables[-1]["row_tones"] = [row_tone_from_status(validation_status(row["avg_confidence"], pass_min=0.60, warn_min=0.55)) for row in pred_rows]

    return {
        "title": "Notebook Diagnostics",
        "description": "Performance slices that help explain why the model is or is not working.",
        "cards": cards,
        "tables": tables,
    }


def build_data_scientist_section(conn: sqlite3.Connection, schema: dict[str, list[str]]) -> dict:
    metrics, summary = build_validation_metrics(conn, schema)
    if not metrics or not table_exists(schema, "trades"):
        return {
            "title": "Data Scientist Review",
            "description": "Classification quality, tail-risk checks, and explicit acceptability thresholds.",
            "cards": [],
            "tables": [make_table("Review", "No executed trade sample is available yet.", [], [], "No review data available.")],
        }

    trade_cols = set(schema.get("trades", []))
    metric_lookup = {metric["name"]: metric for metric in metrics}
    cards = [
        make_card("Precision (UP)", fmt_pct(summary.get("precision_up_pct")), "Acceptable >= 55%. Lower values mean too many false UP calls.", status_tone(metric_lookup["Precision (UP)"]["status"]) if summary.get("precision_up_pct") is not None else "blue"),
        make_card("Recall (UP)", fmt_pct(summary.get("recall_up_pct")), "Acceptable >= 55%. Lower values mean missed UP opportunities.", status_tone(metric_lookup["Recall (UP)"]["status"]) if summary.get("recall_up_pct") is not None else "blue"),
        make_card("F1 Score", fmt_num(summary.get("f1_up"), 3), "Acceptable >= 0.55. Balances precision and recall.", status_tone(metric_lookup["F1 Score (UP)"]["status"]) if summary.get("f1_up") is not None else "blue"),
        make_card("Balanced Accuracy", fmt_pct(summary.get("balanced_accuracy_pct")), "Acceptable >= 55%. Better than raw accuracy under class imbalance.", status_tone(metric_lookup["Balanced Accuracy"]["status"]) if summary.get("balanced_accuracy_pct") is not None else "blue"),
        make_card("PnL Outlier Rate", fmt_pct(summary.get("outlier_rate_pct")), "Prefer <= 5%. Higher rates often mean unstable execution or strategy tails.", status_tone(metric_lookup["PnL Outlier Rate"]["status"]) if summary.get("outlier_rate_pct") is not None else "blue"),
    ]

    confusion = summary.get("confusion") or {}
    confusion_rows = [
        ["Predicted UP / Actual UP", fmt_int(confusion.get("tp")), "Correct UP calls"],
        ["Predicted UP / Actual DOWN", fmt_int(confusion.get("fp")), "False positives"],
        ["Predicted DOWN / Actual UP", fmt_int(confusion.get("fn")), "False negatives"],
        ["Predicted DOWN / Actual DOWN", fmt_int(confusion.get("tn")), "Correct DOWN calls"],
    ]
    confusion_table = make_table(
        "Confusion Breakdown",
        "Binary classification view using UP as the positive class.",
        ["Bucket", "Count", "Meaning"],
        confusion_rows,
        "Predictions cannot be joined to trades in this DB version.",
    )
    confusion_table["row_tones"] = ["green", "red", "yellow", "green"] if confusion else []

    time_expr = "COALESCE(time_str, created_at, '(unknown)')" if "created_at" in trade_cols else "COALESCE(time_str, '(unknown)')"
    conf_expr = "ROUND(confidence, 3)" if "confidence" in trade_cols else "NULL"
    edge_expr = "ROUND(edge_val, 4)" if "edge_val" in trade_cols else "NULL"
    exit_expr = "COALESCE(exit_reason, '(none)')" if "exit_reason" in trade_cols else "'(n/a)'"
    outlier_rows = query_all(conn, f"""
        SELECT
            id,
            {time_expr} AS time_label,
            COALESCE(action, '(none)') AS action,
            ROUND(pnl, 3) AS pnl,
            {conf_expr} AS confidence,
            {edge_expr} AS edge_val,
            {exit_expr} AS exit_reason
        FROM trades
        WHERE action != 'skip' AND pnl IS NOT NULL
        ORDER BY ABS(pnl) DESC
        LIMIT 10
    """)
    outlier_table = make_table(
        "Largest Absolute PnL Moves",
        "These are the most extreme realized trades. Compare them to the IQR fences to judge if the tails are acceptable.",
        ["Trade", "Time", "Action", "PnL", "Confidence", "Edge", "Exit Reason"],
        [[fmt_int(row["id"]), row["time_label"], row["action"], fmt_money(row["pnl"], 3), fmt_num(row["confidence"], 3), fmt_num(row["edge_val"], 4), row["exit_reason"]] for row in outlier_rows],
        "No PnL rows available.",
    )
    low_fence = summary.get("pnl_low_fence")
    high_fence = summary.get("pnl_high_fence")
    outlier_table["description"] = (
        f"Lower fence {fmt_money(low_fence, 3)} and upper fence {fmt_money(high_fence, 3)} from Tukey IQR outlier testing."
        if low_fence is not None and high_fence is not None else
        outlier_table["description"]
    )
    outlier_table["row_tones"] = [
        "red" if row.get("pnl") is not None and low_fence is not None and high_fence is not None and
        (float(row["pnl"]) < low_fence or float(row["pnl"]) > high_fence) else ""
        for row in outlier_rows
    ]

    acceptable_rows = [
        ["Precision / Recall / F1", ">= 55%", "Below 50% is weak. Between 50% and 55% is marginal.", metric_lookup["F1 Score (UP)"]["status"].upper() if summary.get("f1_up") is not None else "N/A"],
        ["Balanced Accuracy", ">= 55%", "Protects against misleading accuracy under class imbalance.", metric_lookup["Balanced Accuracy"]["status"].upper() if summary.get("balanced_accuracy_pct") is not None else "N/A"],
        ["Calibration Error", "< 5%", "5-10% is usable but noisy. Above 10% means confidence is overstated or understated.", metric_lookup["Calibration Error"]["status"].upper() if summary.get("calibration_error_pct") is not None else "N/A"],
        ["PnL Outlier Rate", "<= 5%", "5-10% is marginal. Above 10% means unstable payoff tails.", metric_lookup["PnL Outlier Rate"]["status"].upper() if summary.get("outlier_rate_pct") is not None else "N/A"],
    ]
    acceptable_table = make_table(
        "Acceptability Guide",
        "A compact interpretation layer for the main classification and tail-risk checks.",
        ["Check", "Target", "How To Read It", "Status"],
        acceptable_rows,
        "No acceptability guide available.",
    )
    acceptable_table["row_tones"] = [row_tone_from_status(row[-1].lower()) if row[-1] != "N/A" else "" for row in acceptable_rows]

    return {
        "title": "Data Scientist Review",
        "description": "Classification quality, tail-risk checks, and explicit acceptability thresholds.",
        "cards": cards,
        "tables": [confusion_table, acceptable_table, outlier_table],
    }


def build_dataset_coverage_section(conn: sqlite3.Connection, schema: dict[str, list[str]], counts: dict[str, int]) -> dict:
    cards: list[dict] = []
    tables: list[dict] = []

    if table_exists(schema, "window_snapshots"):
        snapshot_summary = query_one(conn, """
            SELECT COUNT(*) AS snapshot_rows,
                   COUNT(DISTINCT window_ts) AS snapshot_windows,
                   ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT window_ts), 0), 2) AS avg_snapshots_per_window,
                   SUM(CASE WHEN early_model_prob_up IS NOT NULL THEN 1 ELSE 0 END) AS early_rows,
                   SUM(CASE WHEN late_model_signal IS NOT NULL THEN 1 ELSE 0 END) AS late_rows
            FROM window_snapshots
        """)
        cards.extend([
            make_card("Snapshot Rows", fmt_int(snapshot_summary.get("snapshot_rows")), f"{fmt_int(snapshot_summary.get('snapshot_windows'))} windows with {fmt_num(snapshot_summary.get('avg_snapshots_per_window'), 2)} snapshots each on average.", "blue"),
            make_card("Early vs Late Coverage", f"{fmt_int(snapshot_summary.get('early_rows'))} / {fmt_int(snapshot_summary.get('late_rows'))}", "Rows with early-model fields vs rows with late-model fields.", "yellow"),
        ])

    if table_exists(schema, "bot_decisions"):
        decision_summary = query_one(conn, """
            SELECT COUNT(*) AS decision_rows,
                   SUM(CASE WHEN executed = 1 THEN 1 ELSE 0 END) AS executed_rows,
                   COUNT(DISTINCT window_ts) AS decision_windows
            FROM bot_decisions
        """)
        cards.append(
            make_card("Decision Audit Trail", fmt_int(decision_summary.get("decision_rows")), f"{fmt_int(decision_summary.get('executed_rows'))} executed decision rows across {fmt_int(decision_summary.get('decision_windows'))} windows.", "green")
        )

    if table_exists(schema, "windows"):
        window_rows = query_all(conn, """
            SELECT
                COALESCE(final_result_side, '(none)') AS result_side,
                COUNT(*) AS windows_count,
                SUM(CASE WHEN bot_traded = 1 THEN 1 ELSE 0 END) AS traded_windows,
                SUM(CASE WHEN bot_observed = 1 THEN 1 ELSE 0 END) AS observed_windows
            FROM windows
            GROUP BY COALESCE(final_result_side, '(none)')
            ORDER BY windows_count DESC, result_side
        """)
        tables.append(make_table(
            "Window Outcome Coverage",
            "How much labeled parent-window data you have for future analysis and retraining.",
            ["Result Side", "Windows", "Bot Traded", "Observed Only"],
            [[row["result_side"], fmt_int(row["windows_count"]), fmt_int(row["traded_windows"]), fmt_int(row["observed_windows"])] for row in window_rows],
            "No parent window rows are available yet.",
        ))

    inventory_rows = [[table, fmt_int(counts.get(table, 0)), fmt_int(len(schema.get(table, []))), ", ".join(schema.get(table, [])[:6]) + ("..." if len(schema.get(table, [])) > 6 else "")] for table in sorted(schema)]
    tables.append(make_table(
        "Schema Inventory",
        "Quick schema and row-count scan for the selected database version.",
        ["Table", "Rows", "Columns", "Leading Columns"],
        inventory_rows,
        "No tables found in this database.",
    ))

    return {
        "title": "Dataset Coverage",
        "description": "How much labeled and auditable data the selected DB version gives you for validation and future model work.",
        "cards": cards,
        "tables": tables,
    }


def build_chart_panels(conn: sqlite3.Connection, schema: dict[str, list[str]]) -> list[dict]:
    if not table_exists(schema, "trades"):
        return []

    trades = query_all(conn, """
        SELECT id, pnl, won, confidence, edge_val
        FROM trades
        WHERE action != 'skip' AND won IS NOT NULL
        ORDER BY id
    """)
    if not trades:
        return []

    labels = [str(idx + 1) for idx in range(len(trades))]
    cumulative = []
    running = 0.0
    for trade in trades:
        running += float(trade.get("pnl") or 0)
        cumulative.append(round(running, 4))

    rolling_window = min(30, max(10, len(trades) // 4))
    rolling_labels = []
    rolling_wr = []
    if len(trades) >= rolling_window:
        for idx in range(rolling_window, len(trades) + 1):
            window = trades[idx - rolling_window:idx]
            wr = sum(1 for trade in window if trade.get("won") == 1) / len(window) * 100
            rolling_labels.append(str(idx))
            rolling_wr.append(round(wr, 2))
    else:
        rolling_labels = labels
        base_wr = sum(1 for trade in trades if trade.get("won") == 1) / len(trades) * 100
        rolling_wr = [round(base_wr, 2) for _ in trades]

    conf_rows = []
    if "confidence" in schema.get("trades", []):
        conf_rows = query_all(conn, """
            SELECT
                CASE
                    WHEN confidence < 0.55 THEN '< 0.55'
                    WHEN confidence < 0.60 THEN '0.55 - 0.60'
                    WHEN confidence < 0.65 THEN '0.60 - 0.65'
                    WHEN confidence < 0.70 THEN '0.65 - 0.70'
                    WHEN confidence < 0.75 THEN '0.70 - 0.75'
                    ELSE '0.75+'
                END AS bucket,
                ROUND(AVG(confidence) * 100, 2) AS avg_conf_pct,
                ROUND(100.0 * SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS actual_wr_pct
            FROM trades
            WHERE action != 'skip' AND confidence IS NOT NULL
            GROUP BY bucket
            ORDER BY MIN(confidence)
        """)

    edge_rows = []
    if "edge_val" in schema.get("trades", []):
        edge_rows = query_all(conn, """
            SELECT
                CASE
                    WHEN edge_val < 0.03 THEN '< 0.03'
                    WHEN edge_val < 0.05 THEN '0.03 - 0.05'
                    WHEN edge_val < 0.10 THEN '0.05 - 0.10'
                    WHEN edge_val < 0.15 THEN '0.10 - 0.15'
                    ELSE '0.15+'
                END AS bucket,
                ROUND(AVG(pnl), 3) AS avg_pnl,
                ROUND(100.0 * SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS win_rate_pct
            FROM trades
            WHERE action != 'skip' AND edge_val IS NOT NULL
            GROUP BY bucket
            ORDER BY MIN(edge_val)
        """)

    confusion_rows = []
    if table_exists(schema, "predictions"):
        confusion_rows = [query_one(conn, """
            SELECT
                SUM(CASE WHEN p.direction = 'UP' AND t.actual = 'UP' THEN 1 ELSE 0 END) AS tp,
                SUM(CASE WHEN p.direction = 'UP' AND t.actual = 'DOWN' THEN 1 ELSE 0 END) AS fp,
                SUM(CASE WHEN p.direction = 'DOWN' AND t.actual = 'UP' THEN 1 ELSE 0 END) AS fn,
                SUM(CASE WHEN p.direction = 'DOWN' AND t.actual = 'DOWN' THEN 1 ELSE 0 END) AS tn
            FROM predictions p
            JOIN trades t ON p.window_ts = t.window_ts
            WHERE p.direction IN ('UP', 'DOWN') AND t.action != 'skip' AND t.actual IN ('UP', 'DOWN')
        """)]

    panels = [
        {
            "id": "equityCurveChart",
            "title": "Equity Curve",
            "description": "Cumulative realized PnL across executed trades.",
            "type": "line",
            "data": {
                "labels": labels,
                "datasets": [
                    {
                        "label": "Cumulative PnL",
                        "data": cumulative,
                        "borderColor": "#155eef",
                        "backgroundColor": "rgba(21, 94, 239, 0.12)",
                        "fill": True,
                        "tension": 0.2,
                    }
                ],
            },
        },
        {
            "id": "rollingWinRateChart",
            "title": "Rolling Win Rate",
            "description": f"Trailing {rolling_window}-trade win rate.",
            "type": "line",
            "data": {
                "labels": rolling_labels,
                "datasets": [
                    {
                        "label": "Rolling Win Rate %",
                        "data": rolling_wr,
                        "borderColor": "#0f766e",
                        "backgroundColor": "rgba(15, 118, 110, 0.10)",
                        "fill": True,
                        "tension": 0.25,
                    },
                    {
                        "label": "50% Baseline",
                        "data": [50 for _ in rolling_labels],
                        "borderColor": "#b42318",
                        "borderDash": [6, 6],
                        "pointRadius": 0,
                    },
                ],
            },
        },
        {
            "id": "calibrationChart",
            "title": "Confidence Calibration",
            "description": "Average model confidence versus actual win rate by confidence bucket.",
            "type": "bar",
            "data": {
                "labels": [row["bucket"] for row in conf_rows],
                "datasets": [
                    {
                        "label": "Actual Win Rate %",
                        "data": [row["actual_wr_pct"] for row in conf_rows],
                        "backgroundColor": "rgba(21, 94, 239, 0.72)",
                    },
                    {
                        "label": "Average Confidence %",
                        "data": [row["avg_conf_pct"] for row in conf_rows],
                        "backgroundColor": "rgba(15, 118, 110, 0.62)",
                    },
                ],
            },
        },
        {
            "id": "edgePnlChart",
            "title": "Edge Cohort PnL",
            "description": "Average PnL and win rate by edge bucket.",
            "type": "bar",
            "data": {
                "labels": [row["bucket"] for row in edge_rows],
                "datasets": [
                    {
                        "label": "Average PnL",
                        "data": [row["avg_pnl"] for row in edge_rows],
                        "backgroundColor": "rgba(181, 71, 8, 0.72)",
                        "yAxisID": "y",
                    },
                    {
                        "label": "Win Rate %",
                        "data": [row["win_rate_pct"] for row in edge_rows],
                        "type": "line",
                        "borderColor": "#087443",
                        "backgroundColor": "rgba(8, 116, 67, 0.12)",
                        "tension": 0.25,
                        "yAxisID": "y1",
                    },
                ],
            },
            "options": {
                "scales": {
                    "y": {"position": "left", "ticks": {"color": "#475467"}, "grid": {"color": "#e6edf4"}},
                    "y1": {"position": "right", "ticks": {"color": "#475467"}, "grid": {"display": False}},
                    "x": {"ticks": {"color": "#475467"}, "grid": {"color": "#e6edf4"}},
                }
            },
        },
    ]
    if confusion_rows and confusion_rows[0]:
        row = confusion_rows[0]
        panels.append(
            {
                "id": "confusionChart",
                "title": "Confusion Breakdown",
                "description": "UP-vs-DOWN classification counts for executed predictions.",
                "type": "bar",
                "data": {
                    "labels": ["TP", "FP", "FN", "TN"],
                    "datasets": [
                        {
                            "label": "Count",
                            "data": [row.get("tp") or 0, row.get("fp") or 0, row.get("fn") or 0, row.get("tn") or 0],
                            "backgroundColor": ["#087443", "#b42318", "#f59e0b", "#155eef"],
                        }
                    ],
                },
            }
        )
    return panels


def build_trade_section(conn: sqlite3.Connection, schema: dict[str, list[str]]) -> dict:
    tables: list[dict] = []
    if not table_exists(schema, "trades"):
        return {
            "title": "Trade Insights",
            "description": "Executed trade performance, exit behavior, and confidence buckets.",
            "cards": [],
            "tables": [make_table("Trades", "This database has no trades table.", [], [], "No trades table found.")],
        }

    trade_cols = set(schema.get("trades", []))
    conf_avg = "ROUND(AVG(CASE WHEN action != 'skip' THEN confidence END), 3) AS avg_confidence" if "confidence" in trade_cols else "NULL AS avg_confidence"
    edge_avg = "ROUND(AVG(CASE WHEN action != 'skip' THEN edge_val END), 4) AS avg_edge" if "edge_val" in trade_cols else "NULL AS avg_edge"
    bet_avg = "ROUND(AVG(CASE WHEN action != 'skip' THEN bet_size END), 3) AS avg_bet" if "bet_size" in trade_cols else "NULL AS avg_bet"
    slip_avg = "ROUND(AVG(CASE WHEN action != 'skip' THEN slippage_pct END), 4) AS avg_slippage_pct" if "slippage_pct" in trade_cols else "NULL AS avg_slippage_pct"
    latest_balance = "ROUND((SELECT balance_after FROM trades WHERE balance_after IS NOT NULL ORDER BY id DESC LIMIT 1), 2) AS latest_balance" if "balance_after" in trade_cols else "NULL AS latest_balance"

    summary = query_one(conn, f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN action != 'skip' THEN 1 ELSE 0 END) AS executed_trades,
            SUM(CASE WHEN action = 'skip' THEN 1 ELSE 0 END) AS skipped_rows,
            SUM(CASE WHEN action != 'skip' AND won = 1 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN action != 'skip' AND won = 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(100.0 * SUM(CASE WHEN action != 'skip' AND won = 1 THEN 1 ELSE 0 END) /
                NULLIF(SUM(CASE WHEN action != 'skip' AND won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS win_rate_pct,
            ROUND(SUM(CASE WHEN action != 'skip' THEN COALESCE(pnl, 0) ELSE 0 END), 2) AS total_pnl,
            ROUND(AVG(CASE WHEN action != 'skip' THEN pnl END), 3) AS avg_pnl,
            {conf_avg},
            {edge_avg},
            {bet_avg},
            {slip_avg},
            {latest_balance}
        FROM trades
    """)

    cards = [
        make_card("Executed Trades", fmt_int(summary.get("executed_trades")), f"{fmt_int(summary.get('skipped_rows'))} skipped rows are also present.", "blue"),
        make_card("Win Rate", fmt_pct(summary.get("win_rate_pct")), f"{fmt_int(summary.get('wins'))} wins / {fmt_int(summary.get('losses'))} losses.", "green" if (summary.get("win_rate_pct") or 0) >= 50 else "yellow"),
        make_card("Total PnL", fmt_money(summary.get("total_pnl")), f"Average trade PnL: {fmt_money(summary.get('avg_pnl'), 3)}.", "green" if (summary.get("total_pnl") or 0) >= 0 else "red"),
        make_card("Entry Quality", fmt_num(summary.get("avg_confidence"), 3), f"Avg edge {fmt_num(summary.get('avg_edge'), 4)} and avg bet {fmt_money(summary.get('avg_bet'), 3)}.", "yellow"),
        make_card("Slippage", fmt_pct(summary.get("avg_slippage_pct"), 3), "Average recorded slippage across executed trades.", "red" if (summary.get("avg_slippage_pct") or 0) > 0 else "green"),
        make_card("Latest Balance", fmt_money(summary.get("latest_balance")), "Last recorded balance_after in the trade log.", "blue"),
    ]

    exit_rows = []
    if "exit_reason" in trade_cols:
        exit_rows = query_all(conn, """
            SELECT
                COALESCE(exit_reason, '(none)') AS exit_reason,
                COUNT(*) AS trades,
                SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END) AS losses,
                ROUND(100.0 * SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS win_rate_pct,
                ROUND(SUM(COALESCE(pnl, 0)), 2) AS total_pnl
            FROM trades
            WHERE action != 'skip'
            GROUP BY COALESCE(exit_reason, '(none)')
            ORDER BY trades DESC, exit_reason
        """)
    tables.append(make_table(
        "Exit Reason Breakdown",
        "Which exits are driving PnL and hit rate.",
        ["Exit Reason", "Trades", "Wins", "Losses", "Win Rate", "Total PnL"],
        [[row["exit_reason"], fmt_int(row["trades"]), fmt_int(row["wins"]), fmt_int(row["losses"]), fmt_pct(row["win_rate_pct"]), fmt_money(row["total_pnl"])] for row in exit_rows],
        "This DB version does not have exit_reason data yet." if "exit_reason" not in trade_cols else "No completed trades with exit reasons yet.",
    ))

    action_rows = query_all(conn, """
        SELECT COALESCE(action, '(none)') AS action, COUNT(*) AS trades,
               ROUND(100.0 * SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS win_rate_pct,
               ROUND(SUM(COALESCE(pnl, 0)), 2) AS total_pnl
        FROM trades
        WHERE action != 'skip'
        GROUP BY COALESCE(action, '(none)')
        ORDER BY trades DESC, action
    """)
    tables.append(make_table(
        "Direction Performance",
        "UP vs DOWN trade outcomes.",
        ["Action", "Trades", "Win Rate", "Total PnL"],
        [[row["action"], fmt_int(row["trades"]), fmt_pct(row["win_rate_pct"]), fmt_money(row["total_pnl"])] for row in action_rows],
        "No direction-level trade data yet.",
    ))

    confidence_rows = []
    if "confidence" in trade_cols:
        confidence_rows = query_all(conn, """
            SELECT
                CASE
                    WHEN confidence < 0.55 THEN '< 0.55'
                    WHEN confidence < 0.60 THEN '0.55 - 0.60'
                    WHEN confidence < 0.65 THEN '0.60 - 0.65'
                    WHEN confidence < 0.70 THEN '0.65 - 0.70'
                    WHEN confidence < 0.75 THEN '0.70 - 0.75'
                    ELSE '0.75+'
                END AS bucket,
                COUNT(*) AS trades,
                ROUND(100.0 * SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN won IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS win_rate_pct,
                ROUND(SUM(COALESCE(pnl, 0)), 2) AS total_pnl
            FROM trades
            WHERE action != 'skip' AND confidence IS NOT NULL
            GROUP BY bucket
            ORDER BY MIN(confidence)
        """)
    tables.append(make_table(
        "Confidence Buckets",
        "Whether stronger entry confidence is separating better trade cohorts.",
        ["Confidence", "Trades", "Win Rate", "Total PnL"],
        [[row["bucket"], fmt_int(row["trades"]), fmt_pct(row["win_rate_pct"]), fmt_money(row["total_pnl"])] for row in confidence_rows],
        "This DB version does not have confidence data yet." if "confidence" not in trade_cols else "No confidence values recorded yet.",
    ))

    time_expr = "COALESCE(time_str, created_at, '(unknown)')" if "created_at" in trade_cols else "COALESCE(time_str, '(unknown)')"
    conf_expr = "ROUND(confidence, 3)" if "confidence" in trade_cols else "NULL"
    edge_expr = "ROUND(edge_val, 4)" if "edge_val" in trade_cols else "NULL"
    exit_expr = "COALESCE(exit_reason, '(none)')" if "exit_reason" in trade_cols else "'(n/a)'"
    recent_rows = query_all(conn, f"""
        SELECT {time_expr} AS time_label, COALESCE(action, '(none)') AS action,
               {conf_expr} AS confidence, {edge_expr} AS edge_val,
               ROUND(pnl, 3) AS pnl, won, {exit_expr} AS exit_reason
        FROM trades
        ORDER BY id DESC
        LIMIT 12
    """)
    tables.append(make_table(
        "Recent Trades",
        "Latest rows for sanity checking while switching DB versions.",
        ["Time", "Action", "Confidence", "Edge", "PnL", "Won", "Exit Reason"],
        [[row["time_label"], row["action"], fmt_num(row["confidence"], 3), fmt_num(row["edge_val"], 4), fmt_money(row["pnl"], 3), str(row["won"]), row["exit_reason"]] for row in recent_rows],
        "No trade rows recorded yet.",
    ))

    return {"title": "Trade Insights", "description": "Executed trade performance, exit behavior, and confidence buckets.", "cards": cards, "tables": tables}


def build_model_section(conn: sqlite3.Connection, schema: dict[str, list[str]]) -> dict:
    cards: list[dict] = []
    tables: list[dict] = []

    if table_exists(schema, "predictions"):
        summary = query_one(conn, """
            SELECT COUNT(*) AS prediction_rows,
                   SUM(CASE WHEN traded = 1 THEN 1 ELSE 0 END) AS traded_predictions,
                   COUNT(DISTINCT window_ts) AS prediction_windows,
                   ROUND(AVG(confidence), 3) AS avg_confidence,
                   ROUND(AVG(prob_up), 3) AS avg_prob_up,
                   ROUND(AVG(edge_val), 4) AS avg_edge
            FROM predictions
        """)
        cards.extend([
            make_card("Prediction Rows", fmt_int(summary.get("prediction_rows")), f"{fmt_int(summary.get('prediction_windows'))} windows logged in predictions.", "blue"),
            make_card("Prediction Confidence", fmt_num(summary.get("avg_confidence"), 3), f"Average prob_up {fmt_num(summary.get('avg_prob_up'), 3)}.", "yellow"),
            make_card("Prediction Edge", fmt_num(summary.get("avg_edge"), 4), f"{fmt_int(summary.get('traded_predictions'))} prediction rows marked as traded.", "green"),
        ])

        direction_rows = query_all(conn, """
            SELECT COALESCE(direction, '(none)') AS direction, COUNT(*) AS rows_count,
                   ROUND(AVG(confidence), 3) AS avg_confidence, ROUND(AVG(edge_val), 4) AS avg_edge
            FROM predictions
            GROUP BY COALESCE(direction, '(none)')
            ORDER BY rows_count DESC, direction
        """)
        tables.append(make_table(
            "Prediction Direction Mix",
            "Which side the prediction table is favoring and how strong those calls are.",
            ["Direction", "Rows", "Avg Confidence", "Avg Edge"],
            [[row["direction"], fmt_int(row["rows_count"]), fmt_num(row["avg_confidence"], 3), fmt_num(row["avg_edge"], 4)] for row in direction_rows],
            "No prediction rows available yet.",
        ))

    if table_exists(schema, "predictions") and table_exists(schema, "trades"):
        joined = query_one(conn, """
            SELECT COUNT(*) AS matched_windows,
                   ROUND(100.0 * SUM(CASE WHEN p.direction = t.actual THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS direction_hit_rate,
                   ROUND(AVG(t.pnl), 3) AS avg_trade_pnl
            FROM predictions p
            JOIN trades t ON p.window_ts = t.window_ts
            WHERE p.direction IS NOT NULL AND t.action != 'skip' AND t.actual IN ('UP', 'DOWN')
        """)
        cards.extend([
            make_card("Prediction Match Rate", fmt_pct(joined.get("direction_hit_rate")), f"{fmt_int(joined.get('matched_windows'))} windows join cleanly between predictions and trades.", "green" if (joined.get("direction_hit_rate") or 0) >= 50 else "red"),
            make_card("Joined Trade PnL", fmt_money(joined.get("avg_trade_pnl"), 3), "Average PnL for windows with both prediction and executed trade data.", "blue"),
        ])

    if table_exists(schema, "window_snapshots"):
        snapshot = query_one(conn, """
            SELECT COUNT(*) AS snapshot_rows,
                   COUNT(DISTINCT window_ts) AS snapshot_windows,
                   ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT window_ts), 0), 2) AS avg_snapshots_per_window,
                   SUM(CASE WHEN early_model_prob_up IS NOT NULL THEN 1 ELSE 0 END) AS early_rows,
                   SUM(CASE WHEN late_model_signal IS NOT NULL THEN 1 ELSE 0 END) AS late_rows,
                   ROUND(AVG(early_model_confidence), 3) AS avg_early_confidence,
                   ROUND(AVG(late_model_confidence), 3) AS avg_late_confidence
            FROM window_snapshots
        """)
        cards.extend([
            make_card("Snapshot Coverage", fmt_int(snapshot.get("snapshot_rows")), f"{fmt_int(snapshot.get('snapshot_windows'))} windows, {fmt_num(snapshot.get('avg_snapshots_per_window'), 2)} snapshots per window.", "blue"),
            make_card("Early Model Coverage", fmt_int(snapshot.get("early_rows")), f"Average early confidence: {fmt_num(snapshot.get('avg_early_confidence'), 3)}.", "yellow"),
            make_card("Late Model Coverage", fmt_int(snapshot.get("late_rows")), f"Average late confidence: {fmt_num(snapshot.get('avg_late_confidence'), 3)}.", "green"),
        ])

        late_rows = query_all(conn, """
            SELECT COALESCE(late_model_signal, '(none)') AS signal, COUNT(*) AS rows_count,
                   ROUND(AVG(late_model_confidence), 3) AS avg_confidence, ROUND(AVG(late_model_edge), 4) AS avg_edge
            FROM window_snapshots
            GROUP BY COALESCE(late_model_signal, '(none)')
            ORDER BY rows_count DESC, signal
        """)
        tables.append(make_table(
            "Late Model Signal Mix",
            "How often each late-management signal appears in snapshots.",
            ["Signal", "Rows", "Avg Confidence", "Avg Edge"],
            [[row["signal"], fmt_int(row["rows_count"]), fmt_num(row["avg_confidence"], 3), fmt_num(row["avg_edge"], 4)] for row in late_rows],
            "No late-model snapshot rows yet.",
        ))

    if table_exists(schema, "bot_decisions"):
        decision = query_one(conn, """
            SELECT COUNT(*) AS decision_rows,
                   SUM(CASE WHEN executed = 1 THEN 1 ELSE 0 END) AS executed_rows,
                   COUNT(DISTINCT window_ts) AS decision_windows,
                   ROUND(AVG(early_model_confidence), 3) AS avg_early_confidence,
                   ROUND(AVG(late_model_confidence), 3) AS avg_late_confidence
            FROM bot_decisions
        """)
        cards.extend([
            make_card("Decision Rows", fmt_int(decision.get("decision_rows")), f"{fmt_int(decision.get('executed_rows'))} executed rows across {fmt_int(decision.get('decision_windows'))} windows.", "yellow"),
            make_card("Decision Context", fmt_num(decision.get("avg_early_confidence"), 3), f"Average late decision confidence: {fmt_num(decision.get('avg_late_confidence'), 3)}.", "blue"),
        ])

        type_rows = query_all(conn, """
            SELECT COALESCE(decision_type, '(none)') AS decision_type, COUNT(*) AS rows_count,
                   SUM(CASE WHEN executed = 1 THEN 1 ELSE 0 END) AS executed_rows
            FROM bot_decisions
            GROUP BY COALESCE(decision_type, '(none)')
            ORDER BY rows_count DESC, decision_type
        """)
        source_rows = query_all(conn, """
            SELECT COALESCE(decision_source, '(none)') AS decision_source, COUNT(*) AS rows_count,
                   SUM(CASE WHEN executed = 1 THEN 1 ELSE 0 END) AS executed_rows
            FROM bot_decisions
            GROUP BY COALESCE(decision_source, '(none)')
            ORDER BY rows_count DESC, decision_source
        """)
        tables.extend([
            make_table("Decision Type Breakdown", "Which event types are being logged and executed.", ["Decision Type", "Rows", "Executed"], [[row["decision_type"], fmt_int(row["rows_count"]), fmt_int(row["executed_rows"])] for row in type_rows], "No decision log rows yet."),
            make_table("Decision Source Breakdown", "Which component owns the decision flow.", ["Decision Source", "Rows", "Executed"], [[row["decision_source"], fmt_int(row["rows_count"]), fmt_int(row["executed_rows"])] for row in source_rows], "No decision-source rows yet."),
        ])

    return {"title": "Model Insights", "description": "Coverage and behavior of predictions, snapshots, and decision logs.", "cards": cards, "tables": tables}


def build_window_section(conn: sqlite3.Connection, schema: dict[str, list[str]], counts: dict[str, int]) -> dict:
    cards: list[dict] = []
    tables: list[dict] = []

    inventory_rows = [[table, fmt_int(counts.get(table, 0)), fmt_int(len(schema.get(table, []))), ", ".join(schema.get(table, [])[:6]) + ("..." if len(schema.get(table, [])) > 6 else "")] for table in sorted(schema)]
    tables.append(make_table(
        "Schema Inventory",
        "Which tables exist in the selected database and how much they contain.",
        ["Table", "Rows", "Columns", "Leading Columns"],
        inventory_rows,
        "No tables found in this database.",
    ))

    if table_exists(schema, "windows"):
        summary = query_one(conn, """
            SELECT COUNT(*) AS total_windows,
                   SUM(CASE WHEN resolved = 1 THEN 1 ELSE 0 END) AS resolved_windows,
                   SUM(CASE WHEN bot_traded = 1 THEN 1 ELSE 0 END) AS traded_windows,
                   SUM(CASE WHEN bot_observed = 1 THEN 1 ELSE 0 END) AS observed_windows,
                   ROUND(100.0 * SUM(CASE WHEN resolved = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS resolved_rate_pct
            FROM windows
        """)
        cards.extend([
            make_card("Windows Logged", fmt_int(summary.get("total_windows")), f"Resolved rate: {fmt_pct(summary.get('resolved_rate_pct'))}.", "blue"),
            make_card("Bot Participation", fmt_int(summary.get("traded_windows")), f"{fmt_int(summary.get('observed_windows'))} observed-only windows are recorded.", "yellow"),
        ])

        result_rows = query_all(conn, """
            SELECT COALESCE(final_result_side, '(none)') AS result_side, COUNT(*) AS windows_count,
                   SUM(CASE WHEN bot_traded = 1 THEN 1 ELSE 0 END) AS traded_windows
            FROM windows
            GROUP BY COALESCE(final_result_side, '(none)')
            ORDER BY windows_count DESC, result_side
        """)
        skip_rows = query_all(conn, """
            SELECT COALESCE(skipped_reason, '(none)') AS skipped_reason, COUNT(*) AS windows_count
            FROM windows
            GROUP BY COALESCE(skipped_reason, '(none)')
            ORDER BY windows_count DESC, skipped_reason
        """)
        tables.extend([
            make_table("Window Resolution Breakdown", "Resolved sides and how often the bot traded those windows.", ["Result Side", "Windows", "Bot Traded"], [[row["result_side"], fmt_int(row["windows_count"]), fmt_int(row["traded_windows"])] for row in result_rows], "No windows recorded yet."),
            make_table("Skip Reasons", "Why windows were skipped or only observed.", ["Skipped Reason", "Windows"], [[row["skipped_reason"], fmt_int(row["windows_count"])] for row in skip_rows], "No skip reasons recorded yet."),
        ])

    if table_exists(schema, "window_snapshots"):
        recent = query_all(conn, """
            SELECT window_ts, snapshot_ts, elapsed_s, remaining_s, ROUND(btc_price, 2) AS btc_price,
                   ROUND(poly_up_price, 3) AS poly_up_price, ROUND(poly_down_price, 3) AS poly_down_price,
                   COALESCE(late_model_signal, '(none)') AS late_model_signal
            FROM window_snapshots
            ORDER BY id DESC
            LIMIT 12
        """)
        tables.append(make_table(
            "Recent Snapshots",
            "Latest intra-window samples captured for retraining and debugging.",
            ["Window", "Snapshot TS", "Elapsed", "Remaining", "BTC", "Poly Up", "Poly Down", "Late Signal"],
            [[fmt_int(row["window_ts"]), fmt_int(row["snapshot_ts"]), fmt_int(row["elapsed_s"]), fmt_int(row["remaining_s"]), fmt_num(row["btc_price"], 2), fmt_num(row["poly_up_price"], 3), fmt_num(row["poly_down_price"], 3), row["late_model_signal"]] for row in recent],
            "No snapshot rows recorded yet.",
        ))

    if table_exists(schema, "bot_decisions"):
        recent = query_all(conn, """
            SELECT window_ts, decision_ts, COALESCE(decision_type, '(none)') AS decision_type,
                   COALESCE(decision_source, '(none)') AS decision_source, COALESCE(side, '(none)') AS side,
                   executed, COALESCE(reason, '(none)') AS reason
            FROM bot_decisions
            ORDER BY id DESC
            LIMIT 12
        """)
        tables.append(make_table(
            "Recent Decisions",
            "Latest enter, skip, hold, exit, and flip events.",
            ["Window", "Decision TS", "Type", "Source", "Side", "Executed", "Reason"],
            [[fmt_int(row["window_ts"]), fmt_int(row["decision_ts"]), row["decision_type"], row["decision_source"], row["side"], fmt_int(row["executed"]), row["reason"]] for row in recent],
            "No decision rows recorded yet.",
        ))

    return {"title": "Window and Schema Coverage", "description": "Parent windows, snapshot capture depth, decision logging, and table inventory across DB versions.", "cards": cards, "tables": tables}


def build_report(database: dict) -> tuple[dict, list[str]]:
    warnings: list[str] = []
    with connect_db(database["path"]) as conn:
        schema = get_schema(conn)
        counts = row_counts(conn, schema)
        metrics, summary = build_validation_metrics(conn, schema)
        chart_panels = build_chart_panels(conn, schema)

        if not schema:
            warnings.append("The selected database has no user tables.")
        if not table_exists(schema, "window_snapshots"):
            warnings.append("No window_snapshots table found. Snapshot-level retraining data is unavailable in this DB version.")
        elif counts.get("window_snapshots", 0) == 0:
            warnings.append("window_snapshots exists but is empty. Late-model training data has not been captured yet.")
        if not table_exists(schema, "bot_decisions"):
            warnings.append("No bot_decisions table found. Decision-source auditing is unavailable in this DB version.")
        if table_exists(schema, "candles"):
            warnings.append("This DB still contains the candles table. That is operationally useful, but it is separate from the model-audit schema.")

        verdict = "No Sample"
        verdict_tone = "blue"
        if metrics:
            pass_count = sum(1 for metric in metrics if metric["status"] == "pass")
            fail_count = sum(1 for metric in metrics if metric["status"] == "fail")
            if pass_count >= 6 and fail_count == 0:
                verdict = "Healthy"
                verdict_tone = "green"
            elif pass_count >= 4 and fail_count <= 2:
                verdict = "Mixed"
                verdict_tone = "yellow"
            else:
                verdict = "Weak"
                verdict_tone = "red"

        overview_cards = [
            make_card("Validation Verdict", verdict, f"{database['name']} • updated {fmt_stamp(database['mtime'])}.", verdict_tone),
            make_card("Live Sample", fmt_int(summary.get("trade_count")), "Executed trades available for validation." if summary else "No executed trade sample yet.", status_tone(metrics[0]["status"]) if metrics else "blue"),
            make_card("Win Rate", fmt_pct(summary.get("win_rate_pct")), "Realized hit rate after execution." if summary else "n/a", status_tone(metrics[1]["status"]) if len(metrics) > 1 else "blue"),
            make_card("Expected Value", fmt_money(summary.get("ev"), 3), "Average realized PnL per executed trade." if summary else "n/a", status_tone(metrics[2]["status"]) if len(metrics) > 2 else "blue"),
            make_card("Profit Factor", fmt_num(summary.get("profit_factor"), 2), "Gross profit divided by gross loss." if summary else "n/a", status_tone(metrics[3]["status"]) if len(metrics) > 3 else "blue"),
            make_card("Calibration Gap", fmt_pct(summary.get("calibration_error_pct")), "Confidence versus realized win-rate gap." if summary else "n/a", status_tone(metrics[6]["status"]) if len(metrics) > 6 and summary.get("calibration_error_pct") is not None else "blue"),
            make_card("Prediction Match", fmt_pct(summary.get("prediction_match_rate")), "Prediction direction versus actual trade outcome." if summary else "n/a", status_tone(metrics[8]["status"]) if len(metrics) > 8 and summary.get("prediction_match_rate") is not None else "blue"),
            make_card("F1 Score", fmt_num(summary.get("f1_up"), 3), "Precision/recall balance for UP predictions." if summary else "n/a", status_tone(metrics[11]["status"]) if len(metrics) > 11 and summary.get("f1_up") is not None else "blue"),
            make_card("Outlier Rate", fmt_pct(summary.get("outlier_rate_pct")), "Share of PnL tails outside Tukey IQR fences." if summary else "n/a", status_tone(metrics[13]["status"]) if len(metrics) > 13 and summary.get("outlier_rate_pct") is not None else "blue"),
            make_card("Coverage Rows", fmt_int(counts.get("window_snapshots", 0) + counts.get("bot_decisions", 0)), "Snapshot and decision rows available for deeper audit.", "yellow"),
        ]

        sections = [
            build_validation_section(conn, schema),
            build_data_scientist_section(conn, schema),
            build_notebook_diagnostics_section(conn, schema),
            build_dataset_coverage_section(conn, schema, counts),
        ]

    return {"overview_cards": overview_cards, "chart_panels": chart_panels, "sections": sections}, warnings


@app.route("/")
def index():
    databases = discover_databases()
    selected = select_database(databases, request.args.get("db"))
    report = None
    warnings: list[str] = []

    if selected:
        try:
            report, warnings = build_report(selected)
        except sqlite3.DatabaseError as exc:
            warnings = [f"Failed to read {selected['name']}: {exc}"]
    else:
        warnings = ["No database files found. Add versioned .db files to 04-Analysis/data to start reviewing them."]

    return render_template_string(
        HTML,
        databases=databases,
        selected=selected,
        report=report,
        warnings=warnings,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BTC trades database analysis dashboard.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the Flask server to.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port to bind the Flask server to.")
    parser.add_argument("--check", action="store_true", help="Print a one-line summary instead of starting the server.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_data_dir()
    if args.check:
        databases = discover_databases()
        if not databases:
            print("No database files found.")
            return
        selected = databases[0]
        report, warnings = build_report(selected)
        print(
            f"OK | selected={selected['name']} | overview_cards={len(report['overview_cards'])} "
            f"| sections={len(report['sections'])} | warnings={len(warnings)}"
        )
        return
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
