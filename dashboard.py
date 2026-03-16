"""
Web dashboard for BTC prediction bot — monitors trades & predictions from SQLite.

Usage:
    python dashboard.py
    Open http://localhost:5050
"""

import sqlite3
import math
import os
import json
import time as _time
from functools import wraps
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template_string, request, Response

from bot_logging import get_logger

DB_PATH = "data/trades.db"
STARTING_BALANCE = 100.0

log = get_logger("dashboard")

app = Flask(__name__)


def _get_dashboard_creds():
    """Read dashboard auth credentials from bot_config."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT key, value FROM bot_config WHERE key IN "
            "('dashboard_username', 'dashboard_password')"
        ).fetchall()
        conn.close()
        cfg = {r["key"]: r["value"] for r in rows}
        return cfg.get("dashboard_username", ""), cfg.get("dashboard_password", "")
    except Exception:
        return "", ""


def require_auth(f):
    """HTTP Basic Auth decorator. Skipped if no credentials are configured."""
    @wraps(f)
    def decorated(*args, **kwargs):
        username, password = _get_dashboard_creds()
        if not username and not password:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or auth.username != username or auth.password != password:
            return Response(
                "Login required.", 401,
                {"WWW-Authenticate": 'Basic realm="BTC Dashboard"'},
            )
        return f(*args, **kwargs)
    return decorated

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>BTC Bot Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root {
    --bg: #06090f; --surface: #0f1419; --surface2: #151c24;
    --border: #1e2a36; --border-light: #2a3a4a;
    --text: #e2e8f0; --dim: #64748b; --dim2: #475569;
    --green: #22c55e; --green-dim: #166534; --green-bg: rgba(34,197,94,0.08);
    --red: #ef4444; --red-dim: #991b1b; --red-bg: rgba(239,68,68,0.08);
    --yellow: #eab308; --yellow-dim: #854d0e;
    --blue: #3b82f6; --blue-dim: #1e40af; --blue-bg: rgba(59,130,246,0.06);
    --cyan: #06b6d4; --purple: #a855f7; --orange: #f97316;
    --glow-green: 0 0 20px rgba(34,197,94,0.15); --glow-red: 0 0 20px rgba(239,68,68,0.15);
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', system-ui, sans-serif; font-size: 14px; line-height: 1.5; }

  .container { max-width: 1800px; margin: 0 auto; padding: 0; }
  .page-layout { display: grid; grid-template-columns: 620px 1fr; min-height: 100vh; }
  .live-panel { background: var(--surface); border-right: 1px solid var(--border); padding: 14px 16px; position: sticky; top: 0; height: 100vh; overflow-y: auto; display: flex; flex-direction: column; }
  .right-panel { padding: 12px 24px; min-width: 0; }
  @media (max-width: 1100px) { .page-layout { grid-template-columns: 1fr; } .live-panel { position: static; height: auto; border-right: none; border-bottom: 1px solid var(--border); max-height: 50vh; } }

  /* Header */
  .header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; padding-bottom: 10px; border-bottom: 1px solid var(--border); }
  .header-left { display: flex; align-items: center; gap: 14px; }
  .logo { width: 36px; height: 36px; background: linear-gradient(135deg, var(--blue), var(--cyan)); border-radius: 10px; display: flex; align-items: center; justify-content: center; font-weight: 800; font-size: 16px; color: #fff; }
  .header h1 { font-size: 18px; font-weight: 700; color: var(--text); letter-spacing: -0.3px; }
  .header h1 span { color: var(--dim); font-weight: 400; font-size: 13px; margin-left: 8px; }
  .header-right { display: flex; align-items: center; gap: 10px; }
  .header-right select, .header-right button { background: var(--surface); color: var(--dim); border: 1px solid var(--border); padding: 6px 12px; border-radius: 8px; cursor: pointer; font-size: 12px; transition: all 0.15s; }
  .header-right button:hover { border-color: var(--blue); color: var(--text); }
  .last-update { color: var(--dim2); font-size: 11px; }

  /* Bot control */
  .bot-control { display: flex; align-items: center; gap: 14px; padding: 10px 16px; border-radius: 10px; margin-bottom: 10px; backdrop-filter: blur(8px); }
  .bot-control.running { background: linear-gradient(135deg, rgba(34,197,94,0.08), rgba(6,182,212,0.04)); border: 1px solid rgba(34,197,94,0.25); }
  .bot-control.halted { background: linear-gradient(135deg, rgba(239,68,68,0.08), rgba(249,115,22,0.04)); border: 1px solid rgba(239,68,68,0.25); }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .status-dot.pulse { animation: pulse 2s ease-in-out infinite; }
  @keyframes pulse { 0%, 100% { opacity: 1; box-shadow: 0 0 0 0 currentColor; } 50% { opacity: 0.7; box-shadow: 0 0 8px 2px currentColor; } }
  .bot-control .status-text { font-weight: 600; font-size: 14px; }
  .bot-control .halt-reason { font-size: 12px; color: var(--dim); flex: 1; }

  /* Stat cards */
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 6px; margin-bottom: 12px; }
  .stat { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; transition: border-color 0.15s; position: relative; overflow: hidden; }
  .stat:hover { border-color: var(--border-light); }
  .stat .label { font-size: 9px; color: var(--dim); text-transform: uppercase; letter-spacing: 0.8px; font-weight: 600; }
  .stat .value { font-size: 20px; font-weight: 800; margin-top: 3px; letter-spacing: -0.5px; }
  .stat .sub { font-size: 10px; color: var(--dim2); margin-top: 2px; }
  .stat.highlight { border-color: var(--blue-dim); background: linear-gradient(135deg, var(--surface), var(--blue-bg)); }

  /* Section headers */
  h2 { font-size: 11px; margin: 12px 0 8px; color: var(--dim); font-weight: 600; text-transform: uppercase; letter-spacing: 1px; }
  .section-divider { border: none; border-top: 1px solid var(--border); margin: 16px 0 4px; }

  /* Tabs */
  .tab-bar { display: flex; gap: 2px; margin-bottom: 16px; background: var(--surface); border-radius: 10px; padding: 3px; border: 1px solid var(--border); width: fit-content; }
  .tab { padding: 7px 16px; border-radius: 8px; cursor: pointer; font-size: 12px; font-weight: 500; color: var(--dim); transition: all 0.15s; user-select: none; }
  .tab:hover { color: var(--text); }
  .tab.active { background: var(--blue); color: #fff; font-weight: 600; }
  .tab-content { display: none; }
  .tab-content.active { display: block; }

  /* Tables */
  table { width: 100%; border-collapse: collapse; background: var(--surface); border-radius: 12px; overflow: hidden; margin-bottom: 16px; border: 1px solid var(--border); }
  th { background: var(--surface2); font-size: 10px; color: var(--dim); text-transform: uppercase; letter-spacing: 0.6px; padding: 10px 12px; text-align: left; white-space: nowrap; font-weight: 600; }
  td { padding: 9px 12px; border-top: 1px solid var(--border); font-size: 12px; white-space: nowrap; font-variant-numeric: tabular-nums; }
  tr:hover td { background: var(--surface2); }

  .green { color: var(--green); } .red { color: var(--red); } .yellow { color: var(--yellow); } .dim { color: var(--dim); } .blue { color: var(--blue); }

  /* Charts */
  .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 10px; }
  .chart-box { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 12px; }
  .chart-box h3 { font-size: 10px; font-weight: 600; color: var(--dim); margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
  .chart-wrap { position: relative; height: 180px; width: 100%; }

  /* Exit breakdown */
  .exit-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 6px; margin-bottom: 10px; }
  .exit-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 10px; text-align: center; transition: border-color 0.15s; }
  .exit-card:hover { border-color: var(--border-light); }
  .exit-card .count { font-size: 22px; font-weight: 800; letter-spacing: -1px; }
  .exit-card .elabel { font-size: 10px; color: var(--dim); margin-top: 2px; font-weight: 500; }
  .exit-card .winrate { font-size: 12px; margin-top: 2px; font-weight: 600; }

  /* Validation metrics */
  .val-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 6px; margin-bottom: 12px; }
  .val-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; display: flex; align-items: center; gap: 10px; }
  .val-status { width: 36px; height: 36px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 14px; flex-shrink: 0; }
  .val-pass { background: var(--green-bg); color: var(--green); border: 1px solid var(--green-dim); }
  .val-warn { background: rgba(234,179,8,0.08); color: var(--yellow); border: 1px solid var(--yellow-dim); }
  .val-fail { background: var(--red-bg); color: var(--red); border: 1px solid var(--red-dim); }
  .val-na { background: rgba(100,116,139,0.08); color: var(--dim); border: 1px solid var(--dim2); }
  .val-info { flex: 1; min-width: 0; }
  .val-name { font-weight: 600; font-size: 12px; color: var(--dim); }
  .val-value { font-size: 18px; font-weight: 800; margin: 2px 0; letter-spacing: -0.3px; }
  .val-threshold { font-size: 10px; color: var(--dim2); }
  .val-detail { font-size: 10px; color: var(--dim2); margin-top: 1px; }
  .acceptance-bar { height: 4px; border-radius: 2px; background: var(--border); margin-top: 6px; overflow: hidden; }
  .acceptance-fill { height: 100%; border-radius: 2px; transition: width 0.5s; }

  /* Verdict card */
  .verdict { border-radius: 10px; padding: 14px 18px; margin-bottom: 10px; display: flex; align-items: center; gap: 20px; backdrop-filter: blur(8px); }
  .verdict-pass { background: linear-gradient(135deg, rgba(34,197,94,0.1) 0%, rgba(6,182,212,0.05) 100%); border: 1px solid var(--green-dim); }
  .verdict-warn { background: linear-gradient(135deg, rgba(234,179,8,0.1) 0%, rgba(249,115,22,0.05) 100%); border: 1px solid var(--yellow-dim); }
  .verdict-fail { background: linear-gradient(135deg, rgba(239,68,68,0.1) 0%, rgba(249,115,22,0.05) 100%); border: 1px solid var(--red-dim); }
  .verdict-na { background: var(--surface); border: 1px solid var(--border); }
  .verdict-icon { font-size: 36px; flex-shrink: 0; }
  .verdict-body { flex: 1; }
  .verdict-title { font-size: 20px; font-weight: 800; letter-spacing: -0.3px; }
  .verdict-sub { font-size: 13px; margin-top: 4px; color: var(--dim); }
  .verdict-breakdown { font-size: 11px; color: var(--dim); margin-top: 8px; display: flex; gap: 14px; flex-wrap: wrap; }
  .verdict-breakdown span { display: inline-flex; align-items: center; gap: 4px; }
  .verdict-dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }

  /* Config form */
  .config-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 8px; }
  .config-group { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 12px 14px; }
  .config-group h3 { font-size: 9px; color: var(--blue); margin-bottom: 8px; text-transform: uppercase; letter-spacing: 1px; font-weight: 700; }
  .config-row { display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px; }
  .config-row label { font-size: 11px; color: var(--dim); }
  .config-row input, .config-row select { background: var(--bg); color: var(--text); border: 1px solid var(--border); padding: 4px 8px; border-radius: 6px; width: 100px; font-size: 12px; transition: border-color 0.15s; }
  .config-row input:focus, .config-row select:focus { outline: none; border-color: var(--blue); }
  .config-row input[type="password"] { width: 160px; }

  /* Buttons */
  .btn { border: none; padding: 8px 18px; border-radius: 8px; cursor: pointer; font-size: 13px; font-weight: 600; transition: all 0.15s; }
  .btn:hover { transform: translateY(-1px); }
  .btn:active { transform: translateY(0); }
  .btn-blue { background: var(--blue); color: #fff; }
  .btn-red { background: var(--red); color: #fff; }
  .btn-green { background: var(--green); color: #fff; }
  .btn-sm { padding: 5px 12px; font-size: 11px; }
  .btn-lg { padding: 10px 24px; font-size: 14px; }
  .btn-outline { background: transparent; border: 1px solid var(--border); color: var(--dim); }
  .btn-outline:hover { border-color: var(--blue); color: var(--text); }
  .config-actions { display: flex; gap: 8px; margin-top: 14px; align-items: center; }
  .msg { font-size: 12px; }
  .msg-ok { color: var(--green); }
  .msg-err { color: var(--red); }

  /* Collapsible panels */
  .collapsible { cursor: pointer; user-select: none; display: flex; align-items: center; gap: 8px; }
  .collapsible::after { content: '+'; font-size: 16px; color: var(--dim); font-weight: 400; margin-left: auto; transition: transform 0.2s; }
  .collapsible.open::after { content: '-'; }
  .panel { display: none; padding: 12px 0; }
  .panel.open { display: block; animation: fadeIn 0.2s; }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }

  /* Alert log */
  .alert-list { max-height: 300px; overflow-y: auto; background: var(--surface); border: 1px solid var(--border); border-radius: 12px; }
  .alert-item { padding: 10px 14px; border-bottom: 1px solid var(--border); font-size: 12px; display: flex; gap: 12px; align-items: flex-start; }
  .alert-item:last-child { border-bottom: none; }
  .alert-item:hover { background: var(--surface2); }
  .alert-type { font-weight: 700; min-width: 110px; font-size: 11px; }

  /* Log viewer */
  .log-viewer { max-height: 400px; overflow-y: auto; background: #050810; border: 1px solid var(--border); border-radius: 12px; padding: 10px; font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace; font-size: 11px; }
  .log-entry { padding: 3px 6px; border-radius: 4px; margin-bottom: 1px; }
  .log-entry:hover { background: rgba(255,255,255,0.03); }
  .log-DEBUG { color: var(--dim2); }
  .log-INFO { color: var(--text); }
  .log-WARNING { color: var(--yellow); }
  .log-ERROR { color: var(--red); }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--border-light); }

  @media (max-width: 900px) { .charts, .val-grid, .config-grid { grid-template-columns: 1fr; } .stats { grid-template-columns: repeat(2, 1fr); } }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
</head>
<body>
<div class="page-layout">

  <!-- LEFT: Live Panel (full height) -->
  <div class="live-panel">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid var(--border);">
      <div class="logo" style="width:28px;height:28px;font-size:13px;border-radius:7px;">B</div>
      <div><div style="font-weight:700;font-size:13px;">BTC Predictor</div><div style="font-size:10px;color:var(--dim);">5-Min Up/Down</div></div>
      <div id="liveStatus" style="margin-left:auto;font-size:11px;"></div>
    </div>
    <div id="liveView" style="font-family:'JetBrains Mono',monospace;font-size:12px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px 14px;line-height:1.75;white-space:pre;overflow-x:auto;"></div>
    <div style="margin-top:auto;padding-top:10px;flex:1;display:flex;flex-direction:column;min-height:0;">
      <div style="font-size:9px;color:var(--dim);text-transform:uppercase;letter-spacing:1px;font-weight:600;margin-bottom:4px;">Last Prediction</div>
      <div id="predFeed" style="font-family:'JetBrains Mono',monospace;font-size:12px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 14px;line-height:1.7;"></div>
    </div>
  </div>

  <!-- RIGHT: Header + Tabs + Content -->
  <div class="right-panel">

  <!-- Header -->
  <div class="header">
    <div class="header-left">
      <h1>Dashboard</h1>
    </div>
    <div class="header-right">
      <select id="refreshInterval" onchange="setRefresh()">
        <option value="0">Auto-refresh: Off</option>
        <option value="10" selected>Every 10s</option>
        <option value="30">Every 30s</option>
        <option value="60">Every 60s</option>
      </select>
      <button onclick="loadAll()">Refresh</button>
      <span class="last-update" id="lastUpdate"></span>
    </div>
  </div>

  <!-- Bot Control -->
  <div id="botControl" class="bot-control running"></div>
  <div class="tab-bar" style="margin-bottom:12px;">
    <div class="tab active" onclick="switchTab('overview',this)">Overview</div>
    <div class="tab" onclick="switchTab('trades',this)">Trades</div>
    <div class="tab" onclick="switchTab('preds',this)">Predictions</div>
    <div class="tab" onclick="switchTab('settings',this)">Settings</div>
    <div class="tab" onclick="switchTab('alerts',this)">Alerts</div>
    <div class="tab" onclick="switchTab('logs',this)">Logs</div>
  </div>

  <!-- Tab: Overview (stats, charts, validation) -->
  <div class="tab-content active" id="tab-overview">
    <div class="stats" id="statsRow"></div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px;">
      <div>
        <h2 style="margin-top:0;">Today</h2>
        <div class="stats" id="dailyRow" style="margin-bottom:0;"></div>
      </div>
      <div>
        <h2 style="margin-top:0;">Slippage</h2>
        <div class="stats" id="slippageRow" style="margin-bottom:0;"></div>
      </div>
    </div>
    <h2 style="margin-top:0;">Exit Strategy Breakdown</h2>
    <div class="exit-grid" id="exitGrid"></div>
    <div class="charts">
      <div class="chart-box"><h3>Equity Curve</h3><div class="chart-wrap"><canvas id="pnlChart"></canvas></div></div>
      <div class="chart-box"><h3>Confidence vs Outcome</h3><div class="chart-wrap"><canvas id="confChart"></canvas></div></div>
      <div class="chart-box"><h3>Rolling Win Rate</h3><div class="chart-wrap"><canvas id="rollingChart"></canvas></div></div>
      <div class="chart-box"><h3>Confidence Calibration</h3><div class="chart-wrap"><canvas id="calChart"></canvas></div></div>
    </div>
    <div class="charts" style="grid-template-columns:1fr;">
      <div class="chart-box"><h3>Slippage per Trade</h3><div class="chart-wrap"><canvas id="slipChart"></canvas></div></div>
    </div>
    <h2>Model Validation</h2>
    <div id="verdictCard"></div>
    <div class="val-grid" id="valGrid"></div>
  </div>

  <!-- Tab: Trades -->
  <div class="tab-content" id="tab-trades">
    <div style="overflow-x:auto;"><table><thead><tr id="tradesHead"></tr></thead><tbody id="tradesBody"></tbody></table></div>
  </div>

  <!-- Tab: Predictions -->
  <div class="tab-content" id="tab-preds">
    <div style="overflow-x:auto;"><table><thead><tr id="predsHead"></tr></thead><tbody id="predsBody"></tbody></table></div>
  </div>

  <!-- Tab: Settings -->
  <div class="tab-content" id="tab-settings">
    <div class="config-grid">
      <div class="config-group">
        <h3>Simulation</h3>
        <div class="config-row"><label>Starting Balance ($)</label><input type="number" id="cfg_starting_balance" step="1"></div>
      </div>
      <div class="config-group">
        <h3>Trading</h3>
        <div class="config-row"><label>Min Bet ($)</label><input type="number" id="cfg_min_bet" step="0.5"></div>
        <div class="config-row"><label>Max Bet ($)</label><input type="number" id="cfg_max_bet" step="0.5"></div>
        <div class="config-row"><label>Max Position (%)</label><input type="number" id="cfg_max_position_pct" step="1"></div>
      </div>
      <div class="config-group">
        <h3>Risk Management</h3>
        <div class="config-row"><label>Daily Loss Limit ($)</label><input type="number" id="cfg_daily_loss_limit" step="1"></div>
        <div class="config-row"><label>Balance Stop-Loss ($)</label><input type="number" id="cfg_stop_loss_balance" step="1"></div>
        <div class="config-row"><label>Drawdown Alert (%)</label><input type="number" id="cfg_drawdown_alert_pct" step="1"></div>
        <div class="config-row"><label>Stop-Loss % (of stake)</label><input type="number" id="cfg_stop_loss_pct" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Consecutive Loss Limit</label><input type="number" id="cfg_consecutive_loss_limit" step="1" min="1"></div>
        <div id="haltStatus"></div>
      </div>
      <div class="config-group">
        <h3>Early Exit</h3>
        <div class="config-row"><label>Profit Threshold (%)</label><input type="number" id="cfg_early_exit_profit_pct" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Window Min (sec)</label><input type="number" id="cfg_early_exit_window_min" step="10"></div>
        <div class="config-row"><label>Window Max (sec)</label><input type="number" id="cfg_early_exit_window_max" step="10"></div>
        <div class="config-row"><label>Flip Loss % (of stake)</label><input type="number" id="cfg_flip_loss_pct" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Flip Min Remaining (sec)</label><input type="number" id="cfg_flip_min_remaining" step="10"></div>
      </div>
      <div class="config-group">
        <h3>Market Entry</h3>
        <div class="config-row"><label>Momentum Entry (80c+)</label><input type="number" id="cfg_poly_momentum_entry" step="0.01" min="0" max="1"></div>
        <div class="config-row"><label>Slam Entry (90c+)</label><input type="number" id="cfg_poly_slam_entry" step="0.01" min="0" max="1"></div>
        <div class="config-row"><label>Slam Min Elapsed (sec)</label><input type="number" id="cfg_poly_slam_min_elapsed" step="5"></div>
        <div class="config-row"><label>Max Buy Price</label><input type="number" id="cfg_poly_momentum_max_buy" step="0.01" min="0" max="1"></div>
        <div class="config-row"><label>Momentum ATR Threshold</label><input type="number" id="cfg_momentum_atr_threshold" step="0.1"></div>
      </div>
      <div class="config-group">
        <h3>Smart Exit</h3>
        <div class="config-row"><label>Market Agree Hold</label><input type="number" id="cfg_market_agree_hold" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Market Disagree Sell</label><input type="number" id="cfg_market_disagree_sell" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Low Volume Threshold ($)</label><input type="number" id="cfg_low_volume_threshold" step="100"></div>
        <div class="config-row"><label>High Volume Threshold ($)</label><input type="number" id="cfg_high_volume_threshold" step="100"></div>
        <div class="config-row"><label>Conviction Hold Threshold</label><input type="number" id="cfg_conviction_hold_threshold" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Calibration Min Trades</label><input type="number" id="cfg_calibration_min_trades" step="5" min="10"></div>
      </div>
      <div class="config-group">
        <h3>Entry Timing</h3>
        <div class="config-row"><label>Entry After (sec)</label><input type="number" id="cfg_entry_after" step="5" min="0"></div>
        <div class="config-row"><label>Entry Before (sec)</label><input type="number" id="cfg_entry_before" step="5" min="0"></div>
        <div class="config-row"><label>Phase 1 Max Elapsed (sec)</label><input type="number" id="cfg_phase1_max_elapsed" step="5"></div>
        <div class="config-row"><label>Phase 2 Max Elapsed (sec)</label><input type="number" id="cfg_phase2_max_elapsed" step="5"></div>
      </div>
      <div class="config-group">
        <h3>Entry Confidence Gates</h3>
        <div class="config-row"><label>Phase 1 Min Confidence</label><input type="number" id="cfg_phase1_min_confidence" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Phase 1 Min Edge</label><input type="number" id="cfg_phase1_min_edge" step="0.01" min="0" max="1"></div>
        <div class="config-row"><label>Phase 2 Min Confidence</label><input type="number" id="cfg_phase2_min_confidence" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Phase 2 Min Edge</label><input type="number" id="cfg_phase2_min_edge" step="0.01" min="0" max="1"></div>
        <div class="config-row"><label>Phase 3 Min Confidence</label><input type="number" id="cfg_phase3_min_confidence" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Phase 3 Min Edge</label><input type="number" id="cfg_phase3_min_edge" step="0.01" min="0" max="1"></div>
      </div>
      <div class="config-group">
        <h3>Market Strategy Gates</h3>
        <div class="config-row"><label>Slam Min Confidence</label><input type="number" id="cfg_slam_min_confidence" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Slam Strong Disagree</label><input type="number" id="cfg_slam_strong_disagree" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Momentum Strong Disagree</label><input type="number" id="cfg_momentum_strong_disagree" step="0.05" min="0" max="1"></div>
      </div>
      <div class="config-group">
        <h3>Bet Sizing Weights</h3>
        <div class="config-row"><label>Confidence Weight</label><input type="number" id="cfg_bet_conf_weight" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Edge Weight</label><input type="number" id="cfg_bet_edge_weight" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Confidence Base</label><input type="number" id="cfg_bet_conf_base" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Confidence Range</label><input type="number" id="cfg_bet_conf_range" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Edge Base</label><input type="number" id="cfg_bet_edge_base" step="0.01" min="0" max="1"></div>
        <div class="config-row"><label>Edge Range</label><input type="number" id="cfg_bet_edge_range" step="0.01" min="0" max="1"></div>
      </div>
      <div class="config-group">
        <h3>Position Flip</h3>
        <div class="config-row"><label>Flip Min Edge</label><input type="number" id="cfg_flip_min_edge" step="0.01" min="0" max="1"></div>
        <div class="config-row"><label>Flip Min Confidence</label><input type="number" id="cfg_flip_min_confidence" step="0.05" min="0" max="1"></div>
      </div>
      <div class="config-group">
        <h3>Take Profit Tuning</h3>
        <div class="config-row"><label>Conviction Bonus</label><input type="number" id="cfg_take_profit_conviction_bonus" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Max Take Profit</label><input type="number" id="cfg_take_profit_max" step="0.05" min="0" max="1"></div>
        <div class="config-row"><label>Resolution Threshold</label><input type="number" id="cfg_resolution_threshold" step="0.05" min="0" max="1"></div>
      </div>
      <div class="config-group">
        <h3>Slippage Simulation</h3>
        <div class="config-row"><label>Enabled</label><select id="cfg_slippage_enabled"><option value="1">Yes</option><option value="0">No</option></select></div>
        <div class="config-row"><label>Random Factor</label><input type="number" id="cfg_slippage_factor" step="0.001"></div>
      </div>
      <div class="config-group">
        <h3>Telegram Alerts</h3>
        <div class="config-row"><label>Bot Token</label><input type="password" id="cfg_telegram_bot_token"></div>
        <div class="config-row"><label>Chat ID</label><input type="text" id="cfg_telegram_chat_id"></div>
        <div class="config-row"><label>Enabled</label><select id="cfg_telegram_alerts_enabled"><option value="1">Yes</option><option value="0">No</option></select></div>
        <button class="btn btn-sm btn-outline" onclick="testTelegram()">Test Connection</button>
        <div id="telegramMsg" class="msg"></div>
      </div>
      <div class="config-group">
        <h3>Dashboard Auth</h3>
        <div class="config-row"><label>Username</label><input type="text" id="cfg_dashboard_username"></div>
        <div class="config-row"><label>Password</label><input type="password" id="cfg_dashboard_password"></div>
        <div class="dim" style="font-size:11px;margin-top:4px;">Leave both empty to disable auth. Changes take effect on next request.</div>
      </div>
    </div>
    <div class="config-actions">
      <button class="btn btn-blue" onclick="saveConfig()">Save Settings</button>
      <div id="configMsg" class="msg"></div>
    </div>
  </div>

  <!-- Tab: Alerts -->
  <div class="tab-content" id="tab-alerts">
    <div class="alert-list" id="alertList"><div class="dim">No alerts yet</div></div>
  </div>

  <!-- Tab: Logs -->
  <div class="tab-content" id="tab-logs">
    <div style="margin-bottom:10px;display:flex;align-items:center;gap:10px;">
      <span style="font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:0.5px;">Level filter:</span>
      <select id="logLevel" onchange="loadLogs()" style="background:var(--surface);color:var(--text);border:1px solid var(--border);padding:5px 10px;border-radius:8px;font-size:12px;">
        <option value="all">All</option>
        <option value="INFO" selected>INFO+</option>
        <option value="WARNING">WARNING+</option>
        <option value="ERROR">ERROR</option>
      </select>
    </div>
    <div class="log-viewer" id="logViewer"><div class="dim">No log entries</div></div>
  </div>

  </div><!-- end right-panel -->

</div><!-- end page-layout -->

<script>
let refreshTimer = null;
const charts = {};

function switchTab(name, el) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('tab-'+name).classList.add('active');
  if (name === 'settings') loadConfig();
  if (name === 'alerts') loadAlerts();
  if (name === 'logs') loadLogs();
}

function setRefresh() {
  if (refreshTimer) clearInterval(refreshTimer);
  const sec = parseInt(document.getElementById('refreshInterval').value);
  if (sec > 0) refreshTimer = setInterval(loadAll, sec * 1000);
}

function loadAll() {
  fetch('/api/summary').then(r=>r.json()).then(renderStats);
  fetch('/api/trades').then(r=>r.json()).then(d => { renderTrades(d); renderCharts(d); renderSlippageChart(d); });
  fetch('/api/predictions').then(r=>r.json()).then(renderPreds);
  fetch('/api/exits').then(r=>r.json()).then(renderExits);
  fetch('/api/validation').then(r=>r.json()).then(renderValidation);
  fetch('/api/daily').then(r=>r.json()).then(renderDaily);
  fetch('/api/slippage').then(r=>r.json()).then(renderSlippage);
  fetch('/api/config').then(r=>r.json()).then(renderBotControl);
  document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();
}

function renderBotControl(cfg) {
  const el = document.getElementById('botControl');
  const cfgMap = {};
  cfg.forEach(c => cfgMap[c.key] = c.value);
  const halted = cfgMap['trading_halted'] === '1';
  const reason = cfgMap['halt_reason'] || '';
  const reasonText = reason ? reason.replace(/_/g,' ') : '';
  if (halted) {
    el.className = 'bot-control halted';
    el.innerHTML = `
      <div class="status-dot" style="background:var(--red);color:var(--red);" class="pulse"></div>
      <div class="status-text red">Paused</div>
      <div class="halt-reason">${reasonText || 'Manually paused'}</div>
      <button class="btn btn-green btn-lg" onclick="toggleBot(false)">Resume Trading</button>
    `;
  } else {
    el.className = 'bot-control running';
    el.innerHTML = `
      <div class="status-dot pulse" style="background:var(--green);color:var(--green);"></div>
      <div class="status-text green">Active</div>
      <div class="halt-reason">Trading normally</div>
      <button class="btn btn-red btn-lg" onclick="toggleBot(true)">Pause</button>
    `;
  }
}

function toggleBot(pause) {
  const data = pause ? {trading_halted:'1',halt_reason:'manual_pause'} : {trading_halted:'0',halt_reason:''};
  fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}).then(()=>loadAll());
}

function togglePanel(id, el) {
  const panel = document.getElementById(id);
  panel.classList.toggle('open');
  el.classList.toggle('open');
}

function loadConfig() {
  fetch('/api/config').then(r=>r.json()).then(d => {
    d.forEach(c => { const el = document.getElementById('cfg_'+c.key); if(el) el.value=c.value; });
    const halted = d.find(c=>c.key==='trading_halted');
    const reason = d.find(c=>c.key==='halt_reason');
    const hs = document.getElementById('haltStatus');
    if(halted && halted.value==='1') {
      hs.innerHTML='<div style="margin-top:10px;padding:10px;background:var(--red-bg);border:1px solid var(--red-dim);border-radius:8px;"><span class="red" style="font-weight:700;">HALTED</span> <span class="dim">'+(reason?reason.value:'')+'</span><br><button class="btn btn-green btn-sm" style="margin-top:6px;" onclick="toggleBot(false)">Resume</button></div>';
    } else {
      hs.innerHTML='<div style="margin-top:8px;font-size:12px;" class="dim">Trading active</div>';
    }
  });
}

function saveConfig() {
  const keys=['starting_balance','min_bet','max_bet','max_position_pct','daily_loss_limit','stop_loss_balance','drawdown_alert_pct','stop_loss_pct','consecutive_loss_limit','early_exit_profit_pct','early_exit_window_min','early_exit_window_max','flip_loss_pct','flip_min_remaining','poly_momentum_entry','poly_slam_entry','poly_slam_min_elapsed','poly_momentum_max_buy','momentum_atr_threshold','market_agree_hold','market_disagree_sell','low_volume_threshold','high_volume_threshold','conviction_hold_threshold','calibration_min_trades','entry_after','entry_before','phase1_max_elapsed','phase1_min_confidence','phase1_min_edge','phase2_max_elapsed','phase2_min_confidence','phase2_min_edge','phase3_min_confidence','phase3_min_edge','slam_min_confidence','slam_strong_disagree','momentum_strong_disagree','bet_conf_weight','bet_edge_weight','bet_conf_base','bet_conf_range','bet_edge_base','bet_edge_range','flip_min_edge','flip_min_confidence','take_profit_conviction_bonus','take_profit_max','resolution_threshold','slippage_enabled','slippage_factor','telegram_bot_token','telegram_chat_id','telegram_alerts_enabled','dashboard_username','dashboard_password'];
  const data={};
  keys.forEach(k=>{const el=document.getElementById('cfg_'+k);if(el)data[k]=el.value;});
  fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)})
    .then(r=>r.json()).then(d=>{
      const msg=document.getElementById('configMsg');
      msg.className=d.ok?'msg msg-ok':'msg msg-err';
      msg.textContent=d.ok?'Saved!':'Error: '+d.error;
      setTimeout(()=>msg.textContent='',3000);
    });
}

function testTelegram() {
  const token=document.getElementById('cfg_telegram_bot_token').value;
  const chatId=document.getElementById('cfg_telegram_chat_id').value;
  const msg=document.getElementById('telegramMsg');
  msg.className='msg';msg.textContent='Sending...';
  fetch('/api/telegram/test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token,chat_id:chatId})})
    .then(r=>r.json()).then(d=>{
      msg.className=d.ok?'msg msg-ok':'msg msg-err';
      msg.textContent=d.ok?'Test sent!':'Failed: '+(d.error||'Unknown');
    });
}

function renderStats(d) {
  document.getElementById('statsRow').innerHTML = `
    <div class="stat highlight"><div class="label">Balance</div><div class="value ${d.balance>=100?'green':'red'}">$${d.balance.toFixed(2)}</div><div class="sub">Started $100</div></div>
    <div class="stat"><div class="label">Total P&L</div><div class="value ${d.total_pnl>=0?'green':'red'}">${d.total_pnl>=0?'+':''}$${d.total_pnl.toFixed(2)}</div><div class="sub">${d.total_trades} trades</div></div>
    <div class="stat"><div class="label">Win Rate</div><div class="value ${d.win_rate>=55?'green':d.win_rate>=50?'yellow':'red'}">${d.win_rate.toFixed(1)}%</div><div class="sub">${d.wins}W ${d.losses}L</div></div>
    <div class="stat"><div class="label">Avg P&L</div><div class="value ${d.avg_pnl>=0?'green':'red'}">${d.avg_pnl>=0?'+':''}$${d.avg_pnl.toFixed(2)}</div><div class="sub">per trade</div></div>
    <div class="stat"><div class="label">Confidence</div><div class="value">${(d.avg_confidence*100).toFixed(1)}%</div><div class="sub">avg at entry</div></div>
    <div class="stat"><div class="label">Edge</div><div class="value">${(d.avg_edge*100).toFixed(1)}%</div><div class="sub">vs market</div></div>
    <div class="stat"><div class="label">Conviction</div><div class="value">${d.avg_conviction!=null?(d.avg_conviction*100).toFixed(0)+'%':'--'}</div><div class="sub">entry score</div></div>
  `;
}

function renderDaily(d) {
  const row=document.getElementById('dailyRow');
  if(!d||!d.length){row.innerHTML='<div class="stat"><div class="label">Today</div><div class="value dim">--</div><div class="sub">No data</div></div>';return;}
  const today=d[0];const w7=d.slice(0,7).reduce((s,x)=>s+x.pnl,0);
  row.innerHTML=`
    <div class="stat"><div class="label">P&L</div><div class="value ${today.pnl>=0?'green':'red'}">${today.pnl>=0?'+':''}$${today.pnl.toFixed(2)}</div><div class="sub">${today.trades_count} trades</div></div>
    <div class="stat"><div class="label">7-Day</div><div class="value ${w7>=0?'green':'red'}">${w7>=0?'+':''}$${w7.toFixed(2)}</div><div class="sub">cumulative</div></div>
  `;
}

function renderSlippage(d) {
  const row=document.getElementById('slippageRow');
  if(!d||!d.count){row.innerHTML='<div class="stat"><div class="label">Slippage</div><div class="value dim">--</div><div class="sub">No data</div></div>';return;}
  row.innerHTML=`
    <div class="stat"><div class="label">Avg Slip</div><div class="value">${d.avg_slippage.toFixed(3)}%</div><div class="sub">${d.count} trades</div></div>
    <div class="stat"><div class="label">Total Cost</div><div class="value ${d.total_cost>0?'red':'dim'}">$${d.total_cost.toFixed(2)}</div><div class="sub">cumulative</div></div>
  `;
}

function loadLive() {
  fetch('/api/live').then(r=>r.json()).then(d=>{
    const st=document.getElementById('liveStatus');
    const v=document.getElementById('liveView');
    if(d.error){
      st.innerHTML=`<span style="color:var(--red);">&#x25CF;</span> <span class="dim">${d.error}</span>`;
      v.innerHTML='<span class="dim">Waiting for bot to start...</span>';
      return;
    }
    const stale=d.stale;
    const dot=stale?'color:var(--yellow)':'color:var(--green)';
    const label=stale?'Stale (bot may be stopped)':'Connected';
    st.innerHTML=`<span style="${dot};font-size:14px;">&#x25CF;</span> <span class="dim">${label}</span>`;

    const c=(v,cls)=>`<span class="${cls}">${v}</span>`;
    const g=v=>c(v,'green');
    const r=v=>c(v,'red');
    const y=v=>c(v,'yellow');
    const dm=v=>c(v,'dim');
    const b=v=>`<b>${v}</b>`;
    const money=(v,prefix='')=> v>=0?g(`${prefix}$${v.toFixed(2)}`):r(`${prefix}$${v.toFixed(2)}`);
    const pct=(v)=>(v*100).toFixed(1)+'%';

    // Price
    const ptb=d.price_to_beat?`$${d.price_to_beat.toLocaleString(undefined,{minimumFractionDigits:2})}`:'-';
    const btcP=d.btc_price?`$${d.btc_price.toLocaleString(undefined,{minimumFractionDigits:2})}`:'-';
    const diff=d.btc_diff||0;
    const diffStr=diff>=0?g(`+${diff.toFixed(2)} ^ above PTB`):r(`${diff.toFixed(2)} v below PTB`);

    // Time bar
    const rem=d.time_remaining||0;
    const mins=Math.floor(rem/60);
    const secs=rem%60;
    const elapsed=d.elapsed||0;
    const barW=30;
    const filled=Math.round(barW*elapsed/300);
    const bar='#'.repeat(filled)+'-'.repeat(barW-filled);
    const timeColor=rem<30?'red':rem<60?'yellow':'blue';
    const timeStr=c(`${mins}m ${String(secs).padStart(2,'0')}s`,timeColor);

    // Volume
    const vol5m=d.btc_volume_5m?`$${Math.round(d.btc_volume_5m).toLocaleString()}`:'...';
    const vol24h=d.btc_volume_24h?`$${Math.round(d.btc_volume_24h).toLocaleString()}`:'...';

    // Prediction
    let predLine=dm('Loading...');
    let confLine=dm('---');
    let edgeLine=dm('---');
    if(d.prediction){
      if(d.prediction.error){
        predLine=r('Error: '+d.prediction.error);
      } else {
        const dir=d.prediction.direction;
        const conf=d.prediction.confidence;
        const sig=d.prediction.signal;
        const dirColor=dir==='UP'?'green':'red';
        predLine=c((dir==='UP'?'ABOVE':'BELOW')+' '+ptb,dirColor);
        const sigColor=sig.includes('STRONG')?'green':sig.includes('MODERATE')?'yellow':'dim';
        const calStr=d.prediction.calibrated?dm(` (cal: ${(d.prediction.calibrated*100).toFixed(1)}%)`):'';
        confLine=c(`${(conf*100).toFixed(1)}% - ${sig}`,sigColor)+calStr;
      }
    }
    if(d.edge && d.edge.side){
      const ec=d.edge.side==='UP'?'green':'red';
      edgeLine=c(b(`>> BUY ${d.edge.side} (${(d.edge.value*100).toFixed(1)}% vs market)`),ec);
    } else if(d.prediction && !d.prediction.error){
      edgeLine=dm('NO EDGE (model ~ market)');
    }

    // Polymarket
    const pu=(d.polymarket.up_price*100).toFixed(1);
    const pd2=(d.polymarket.down_price*100).toFixed(1);
    const pDir=d.polymarket.direction;
    const pDirColor=pDir==='UP'?'green':'red';
    const polyLine=g(`Up ${pu}c`)+' | '+r(`Down ${pd2}c`)+'  '+c(`>> ${pDir}`,pDirColor);
    const pm=d.polymarket;
    const convColor=pm.conviction==='SLAM'||pm.conviction==='STRONG'?'green':pm.conviction==='MODERATE'?'yellow':'dim';
    const volLabel=pm.volume_label==='HIGH'?'green':pm.volume_label==='MED'?'yellow':'dim';
    const mktLine=`Signal: ${c(pm.conviction+' '+(Math.max(pm.up_price,pm.down_price)*100).toFixed(0)+'%',convColor)}  Vol: $${pm.volume.toLocaleString()} ${c(pm.volume_label,volLabel)}  Liq: $${pm.liquidity.toLocaleString()}`;

    // Position
    let posLine=dm('Waiting...');
    let holdLine='';
    const ps=d.position_status;
    if(ps==='open'&&d.position){
      const p=d.position;
      const sc=p.side==='UP'?'green':'red';
      posLine=c(b(`Bought ${p.side} at ${(p.buy_price*100).toFixed(1)}c`),sc)+
        `  Stake: $${p.bet_size.toFixed(2)}  Value: $${p.current_value.toFixed(2)}  `+
        (p.unrealized_pnl>=0?g(`P&L: $${p.unrealized_pnl.toFixed(2)}`):r(`P&L: $${p.unrealized_pnl.toFixed(2)}`));
    } else if(ps==='sold'&&d.position){
      const p=d.position;
      posLine=y(`SOLD ${p.side} at ${(p.exit_price*100).toFixed(1)}c (${p.exit_reason})`)+
        '  '+(p.realized_pnl>=0?g(`Realized: $${p.realized_pnl.toFixed(2)}`):r(`Realized: $${p.realized_pnl.toFixed(2)}`));
    } else if(ps==='flipped'&&d.position){
      const p=d.position;
      posLine=y(`FLIPPED: ${p.flipped_from} -> ${p.side} at ${(p.buy_price*100).toFixed(1)}c`)+
        '  '+(p.unrealized_pnl>=0?g(`P&L: $${p.unrealized_pnl.toFixed(2)}`):r(`P&L: $${p.unrealized_pnl.toFixed(2)}`));
    } else if(ps==='paused'){
      const reason=d.stats.halt_reason?d.stats.halt_reason.replace(/_/g,' '):'Manual Pause';
      posLine=r(b(`PAUSED - ${reason}`))+dm('  (resume from dashboard)');
    } else if(ps==='observing'){
      posLine=y('OBSERVING (first window, no trade)');
    } else if(ps==='scanning'){
      posLine=y('Scanning for entry...');
    } else if(ps==='closed'){
      posLine=dm('Entry window closed - no trade');
    }

    if(d.hold_logic){
      const h=d.hold_logic;
      const mr=h.market_ratio;
      const mrPct=(mr*100).toFixed(0)+'%';
      const mrColor=mr>=0.5?'green':mr>=0.25?'yellow':'red';
      const mrLabel=mr>=0.5?'AGREE':mr>=0.25?'WEAK':'DISAGREE';
      const conv=(h.conviction*100).toFixed(0)+'%';
      const convC=h.conviction>=0.6?'green':h.conviction>=0.3?'yellow':'dim';
      holdLine=`${b('Hold Logic:')}        Mkt ratio: ${c(mrPct+' '+mrLabel,mrColor)}  Conv: ${c(conv,convC)}`;
    }

    // Stats
    const s=d.stats;
    const balColor=s.balance>=100?'green':'red';
    const wl=`(${g(s.wins+'W')} ${r(s.losses+'L')})`;
    const dailyColor=s.daily_pnl>=0?'green':'red';
    let statsLine=`${b('Balance:')}  ${c('$'+s.balance.toFixed(2),balColor)}  ${b('P&L:')} ${money(s.total_pnl)}  ${wl}  ${dm('Skipped: '+s.skipped)}  ${b('Daily:')} ${c('$'+s.daily_pnl.toFixed(2),dailyColor)}`;
    if(s.trading_halted) statsLine+=`  ${r(b('[HALTED: '+s.halt_reason+']'))}`;

    // Config
    const cfg=d.config;
    const slipStr=cfg.slippage_enabled?g('ON')+` (${cfg.slippage_factor})`:dm('OFF');
    const tgStr=cfg.telegram_enabled?g('ON'):dm('OFF');
    const cfgLine=dm('Config:')+` Bet $${cfg.min_bet}-$${cfg.max_bet}  MaxPos ${cfg.max_position_pct}%  DailyLim $${cfg.daily_loss_limit}  StopBal $${cfg.stop_loss_balance}  Slip ${slipStr}  TG ${tgStr}`;

    const sep='='.repeat(75);
    const sep2='-'.repeat(75);

    v.innerHTML=[
      b('BTC Up/Down 5m - Live Dashboard'),
      dm(d.market_title||''),
      sep,
      `${b('Price to beat:')}     ${ptb}  ${dm('(Chainlink)')}`,
      `${b('BTC Price:')}         ${btcP} (${diffStr})`,
      `${b('BTC Volume:')}        5m: ${vol5m}  |  24h: ${vol24h}`,
      `${b('Time remaining:')}    ${timeStr}  ${bar}`,
      sep2,
      `${b('Prediction:')}        ${predLine}`,
      `${b('Confidence:')}        ${confLine}`,
      `${b('Polymarket:')}        ${polyLine}`,
      `${b('Market:')}            ${mktLine}`,
      sep2,
      `${b('Edge:')}              ${edgeLine}`,
      `${b('Position:')}          ${posLine}`,
      holdLine?holdLine:'',
      sep,
      statsLine,
      cfgLine,
    ].filter(l=>l!=='').join('\n');

    // Prediction feed — show only last prediction with details
    const pf=document.getElementById('predFeed');
    if(d.pred_feed && d.pred_feed.length){
      const p=d.pred_feed[d.pred_feed.length-1];
      const ts=new Date(p.t*1000).toLocaleTimeString();
      if(p.error){
        pf.innerHTML=`<div style="color:var(--red);">ERROR: ${p.error}</div>`;
      } else {
        const dc=p.dir==='UP'?'var(--green)':'var(--red)';
        const sc=p.sig.includes('STRONG')?'var(--green)':p.sig.includes('MODERATE')?'var(--yellow)':'var(--dim)';
        const diffVal=p.btc&&p.ptb?(p.btc-p.ptb):0;
        const diffColor=diffVal>=0?'var(--green)':'var(--red)';
        const diffSign=diffVal>=0?'+':'';
        const fmtK=v=>v>=1e9?'$'+(v/1e9).toFixed(1)+'B':v>=1e6?'$'+(v/1e6).toFixed(1)+'M':'$'+Math.round(v).toLocaleString();
        pf.innerHTML=[
          `<div style="margin-bottom:6px;"><span class="dim">Last run:</span> <b>${ts}</b></div>`,
          `<div>Prediction: <span style="color:${dc};font-weight:700;">${p.dir}</span> <span style="color:${sc};">${(p.conf*100).toFixed(1)}% ${p.sig}</span></div>`,
          p.btc?`<div>BTC Price:  <b>$${p.btc.toLocaleString()}</b> <span style="color:${diffColor};">(${diffSign}${diffVal.toFixed(2)})</span></div>`:'',
          p.ptb?`<div>Price to Beat: <span class="dim">$${p.ptb.toLocaleString()}</span></div>`:'',
          p.vol5m?`<div>5m Volume:  ${fmtK(p.vol5m)}</div>`:'',
          p.vol24h?`<div>24h Volume: ${fmtK(p.vol24h)}</div>`:'',
          p.atr?`<div>ATR: <span class="dim">${p.atr.toFixed(2)}</span></div>`:'',
        ].filter(l=>l).join('');
      }
    } else {
      pf.innerHTML='<span class="dim">Waiting for first prediction...</span>';
    }
  }).catch(()=>{
    document.getElementById('liveStatus').innerHTML=`<span style="color:var(--red);">&#x25CF;</span> <span class="dim">Connection error</span>`;
  });
}

function renderExits(d) {
  const g=document.getElementById('exitGrid');
  if(!d.length){g.innerHTML='<div class="dim">No exit data yet</div>';return;}
  const labels={hold_to_resolution:'Held to End',take_profit:'Take Profit',market_disagree:'Market Exit',stop_loss:'Stop Loss',flip:'Flip'};
  g.innerHTML=d.map(e=>{
    const wc=e.win_rate>=55?'green':e.win_rate>=45?'yellow':'red';
    const pc=e.total_pnl>=0?'green':'red';
    return`<div class="exit-card"><div class="count">${e.count}</div><div class="elabel">${labels[e.reason]||e.reason||'Unknown'}</div><div class="winrate ${wc}">${e.win_rate.toFixed(0)}% win</div><div class="${pc}" style="font-size:12px;margin-top:2px;">${e.total_pnl>=0?'+':''}$${e.total_pnl.toFixed(2)}</div></div>`;
  }).join('');
}

const cc={bg:'#06090f',grid:'#152030',tick:'#475569',green:'#22c55e',red:'#ef4444',blue:'#3b82f6',yellow:'#eab308',cyan:'#06b6d4'};
const baseOpts=(extra={})=>({
  responsive:true,maintainAspectRatio:false,
  plugins:{legend:{display:false,labels:{color:cc.tick,font:{size:11}}},...extra.plugins},
  scales:{
    x:{ticks:{color:cc.tick,maxRotation:45,maxTicksLimit:15,font:{size:10}},grid:{color:cc.grid,lineWidth:0.5},...extra.x},
    y:{ticks:{color:cc.tick,font:{size:10}},grid:{color:cc.grid,lineWidth:0.5},...extra.y},
  }
});

function makeChart(id,cfg){if(charts[id])charts[id].destroy();const ctx=document.getElementById(id);if(!ctx)return;charts[id]=new Chart(ctx,cfg);}

function renderSlippageChart(rows) {
  const slipped=rows.filter(r=>r.slippage_pct!=null&&r.slippage_pct>0).reverse();
  if(!slipped.length)return;
  makeChart('slipChart',{type:'bar',data:{labels:slipped.map((r,i)=>r.time_str||'#'+(i+1)),datasets:[{data:slipped.map(r=>r.slippage_pct),backgroundColor:'rgba(249,115,22,0.5)',borderColor:'rgba(249,115,22,0.8)',borderWidth:1,borderRadius:4}]},options:{...baseOpts({y:{ticks:{callback:v=>v+'%'}}}),plugins:{legend:{display:false}}}});
}

function renderCharts(rows) {
  const traded=rows.filter(r=>r.action!=='skip'&&r.won!==null).reverse();
  if(!traded.length)return;

  let cum=0;
  const eqData=traded.map(r=>{cum+=r.pnl||0;return+cum.toFixed(2);});
  makeChart('pnlChart',{type:'line',data:{labels:traded.map((r,i)=>r.time_str||'#'+(i+1)),datasets:[{data:eqData,borderColor:cum>=0?cc.green:cc.red,borderWidth:2,pointRadius:1.5,pointHoverRadius:4,tension:0.3,fill:{target:'origin',above:'rgba(34,197,94,0.06)',below:'rgba(239,68,68,0.06)'}}]},options:{...baseOpts({y:{ticks:{callback:v=>'$'+v}}}),plugins:{legend:{display:false}}}});

  const won=traded.filter(r=>r.won===1).map(r=>({x:+(r.confidence*100).toFixed(1),y:r.pnl}));
  const lost=traded.filter(r=>r.won===0).map(r=>({x:+(r.confidence*100).toFixed(1),y:r.pnl}));
  makeChart('confChart',{type:'scatter',data:{datasets:[{label:'Won',data:won,backgroundColor:'rgba(34,197,94,0.6)',pointRadius:5,pointHoverRadius:7},{label:'Lost',data:lost,backgroundColor:'rgba(239,68,68,0.6)',pointRadius:5,pointHoverRadius:7}]},options:{...baseOpts({x:{title:{display:true,text:'Confidence %',color:cc.tick,font:{size:10}}},y:{title:{display:true,text:'P&L ($)',color:cc.tick,font:{size:10}},ticks:{callback:v=>'$'+v}}}),plugins:{legend:{display:true,labels:{color:cc.tick,font:{size:11},usePointStyle:true,pointStyle:'circle'}}}}});

  const winSize=Math.min(50,Math.max(10,Math.floor(traded.length/3)));
  if(traded.length>=winSize){
    const rl=[],rd=[];
    for(let i=winSize;i<=traded.length;i++){const w=traded.slice(i-winSize,i);rd.push(+(w.filter(t=>t.won===1).length/w.length*100).toFixed(1));rl.push(traded[i-1].time_str||'#'+i);}
    makeChart('rollingChart',{type:'line',data:{labels:rl,datasets:[{data:rd,borderColor:cc.blue,borderWidth:2,pointRadius:0,tension:0.3,fill:{target:'origin',above:'rgba(59,130,246,0.05)'}},{data:rl.map(()=>50),borderColor:cc.red,borderWidth:1,borderDash:[5,5],pointRadius:0},{data:rl.map(()=>55),borderColor:cc.yellow,borderWidth:1,borderDash:[3,3],pointRadius:0}]},options:{...baseOpts({y:{min:0,max:100,ticks:{callback:v=>v+'%'}}}),plugins:{legend:{display:false}}}});
  }

  const buckets={};
  traded.forEach(t=>{const b=Math.floor(t.confidence*20)*5;if(!buckets[b])buckets[b]={wins:0,total:0};buckets[b].total++;if(t.won===1)buckets[b].wins++;});
  const bk=Object.keys(buckets).map(Number).sort((a,b)=>a-b);
  if(bk.length>=2){
    const counts=bk.map(k=>buckets[k].total);
    makeChart('calChart',{type:'bar',data:{labels:bk.map(k=>k+'%'),datasets:[{type:'bar',label:'Actual Win %',data:bk.map(k=>+(buckets[k].wins/buckets[k].total*100).toFixed(1)),backgroundColor:'rgba(34,197,94,0.5)',borderColor:'rgba(34,197,94,0.8)',borderWidth:1,borderRadius:6},{type:'line',label:'Perfect Cal.',data:bk.map(k=>k+2.5),borderColor:cc.yellow,borderWidth:2,borderDash:[5,5],pointRadius:0}]},options:{...baseOpts({y:{min:0,max:100,ticks:{callback:v=>v+'%'}}}),plugins:{legend:{display:true,labels:{color:cc.tick,font:{size:11}}},tooltip:{callbacks:{afterLabel:ctx=>'n='+counts[ctx.dataIndex]}}}}});
  }
}

function renderValidation(d) {
  const v=document.getElementById('verdictCard');const g=document.getElementById('valGrid');
  if(!d||!d.length){v.innerHTML='';g.innerHTML='<div class="dim">Need trades to compute validation</div>';return;}
  const counted=d.filter(m=>m.status!=='na');const passed=counted.filter(m=>m.status==='pass').length;const warned=counted.filter(m=>m.status==='warn').length;const failed=counted.filter(m=>m.status==='fail').length;const total=counted.length;const score=Math.round((passed+warned*0.5)/total*100);
  const coreNames=['Win Rate','Expected Value (EV)','Profit Factor','Maximum Drawdown'];const coreMetrics=d.filter(m=>coreNames.includes(m.name));const corePassed=coreMetrics.filter(m=>m.status==='pass').length;const coreTotal=coreMetrics.length;const allCorePassed=corePassed===coreTotal;
  const tradeMeta=d.find(m=>m.name==='Total Trades');const tradeCount=tradeMeta?parseInt(tradeMeta.display):0;
  let verdict,vc,vs,vi;
  if(total===0){verdict='Insufficient Data';vc='verdict-na';vi='?';vs='Need more trades.';}
  else if(tradeCount<200){
    if(allCorePassed&&score>=70){verdict='Valid \u2014 Confirming';vc='verdict-warn';vi='!';vs=`Core metrics pass. ${200-tradeCount} more trades needed (${tradeCount}/200).`;}
    else if(allCorePassed&&score>=50){verdict='Promising';vc='verdict-warn';vi='!';vs=`Early signs positive. ${200-tradeCount} more needed (${tradeCount}/200).`;}
    else if(corePassed>=3){verdict='Marginal';vc='verdict-warn';vi='!';vs=`Some weaknesses. ${200-tradeCount} more needed (${tradeCount}/200).`;}
    else{verdict='Underperforming';vc='verdict-fail';vi='&#10007;';vs=`Core metrics failing at ${tradeCount} trades.`;}
  } else if(tradeCount<500){
    if(allCorePassed&&score>=70){verdict='Valid Strategy';vc='verdict-pass';vi='&#10003;';vs=`Confirmed at ${tradeCount} trades. Target 500 for full validation.`;}
    else if(allCorePassed&&score>=50){verdict='Conditionally Valid';vc='verdict-warn';vi='!';vs=`Core pass but secondary weak at ${tradeCount} trades.`;}
    else if(corePassed>=3&&score>=40){verdict='Marginal';vc='verdict-warn';vi='!';vs=`Weaknesses at ${tradeCount} trades.`;}
    else{verdict='Invalid Strategy';vc='verdict-fail';vi='&#10007;';vs=`Failing at ${tradeCount} trades.`;}
  } else {
    if(allCorePassed&&score>=70){verdict='Fully Validated';vc='verdict-pass';vi='&#10003;';vs=`${tradeCount} trades. All core metrics pass. Production ready.`;}
    else if(allCorePassed&&score>=50){verdict='Valid with Caveats';vc='verdict-pass';vi='&#10003;';vs=`Core solid over ${tradeCount} trades.`;}
    else if(corePassed>=3&&score>=40){verdict='Marginal';vc='verdict-warn';vi='!';vs=`Weaknesses persist at ${tradeCount} trades.`;}
    else{verdict='Invalid';vc='verdict-fail';vi='&#10007;';vs=`Failed at ${tradeCount} trades.`;}
  }
  v.innerHTML=`<div class="verdict ${vc}"><div class="verdict-icon">${vi}</div><div class="verdict-body"><div class="verdict-title">${verdict}</div><div class="verdict-sub">${vs}</div><div class="verdict-breakdown"><span><span class="verdict-dot" style="background:var(--green)"></span>${passed} pass</span><span><span class="verdict-dot" style="background:var(--yellow)"></span>${warned} warn</span><span><span class="verdict-dot" style="background:var(--red)"></span>${failed} fail</span><span style="color:var(--text)">${score}%</span><span>Core ${corePassed}/${coreTotal}</span></div></div></div>`;
  g.innerHTML=d.map(m=>{
    const sc=m.status==='pass'?'val-pass':m.status==='warn'?'val-warn':m.status==='fail'?'val-fail':'val-na';
    const icon=m.status==='pass'?'&#10003;':m.status==='warn'?'!':m.status==='fail'?'&#10007;':'?';
    const mvc=m.status==='pass'?'green':m.status==='warn'?'yellow':m.status==='fail'?'red':'dim';
    let pct=Math.min(100,Math.max(0,m.acceptance_pct||0));
    const bc=m.status==='pass'?cc.green:m.status==='warn'?cc.yellow:m.status==='fail'?cc.red:cc.tick;
    return`<div class="val-card"><div class="val-status ${sc}">${icon}</div><div class="val-info"><div class="val-name">${m.name}</div><div class="val-value ${mvc}">${m.display}</div><div class="val-threshold">${m.threshold}</div>${m.detail?'<div class="val-detail">'+m.detail+'</div>':''}<div class="acceptance-bar"><div class="acceptance-fill" style="width:${pct}%;background:${bc}"></div></div></div></div>`;
  }).join('');
}

function renderTrades(rows) {
  if(!rows.length){document.getElementById('tradesHead').innerHTML='<th>No trades yet</th>';document.getElementById('tradesBody').innerHTML='';return;}
  const cols=[
    {k:'time_str',l:'Time'},{k:'action',l:'Side'},{k:'price_to_beat',l:'PTB',fmt:v=>'$'+Number(v).toLocaleString(undefined,{maximumFractionDigits:0})},
    {k:'buy_price',l:'Buy',fmt:v=>(v*100).toFixed(1)+'c'},{k:'sell_price',l:'Sell',fmt:v=>(v*100).toFixed(0)+'c'},
    {k:'bet_size',l:'Stake',fmt:v=>'$'+Number(v).toFixed(2)},{k:'confidence',l:'Conf',fmt:v=>(v*100).toFixed(1)+'%'},
    {k:'edge_val',l:'Edge',fmt:v=>(v*100).toFixed(1)+'%'},{k:'entry_conviction',l:'Conv',fmt:v=>v!=null?(v*100).toFixed(0)+'%':'\u2014'},
    {k:'actual',l:'Result'},{k:'pnl',l:'P&L',fmt:v=>(v>=0?'+':'')+('$'+Number(v).toFixed(2))},
    {k:'slippage_pct',l:'Slip',fmt:v=>v!=null&&v>0?v.toFixed(2)+'%':'\u2014'},
    {k:'exit_reason',l:'Exit',fmt:v=>({hold_to_resolution:'held',take_profit:'profit',market_disagree:'mkt_out',stop_loss:'stop',flip:'flip'}[v]||v||'\u2014')},
    {k:'balance_after',l:'Balance',fmt:v=>'$'+Number(v).toFixed(2)},
  ];
  document.getElementById('tradesHead').innerHTML=cols.map(c=>`<th>${c.l}</th>`).join('');
  document.getElementById('tradesBody').innerHTML=rows.map(r=>'<tr>'+cols.map(c=>{
    let v=r[c.k],cls='';
    if(c.k==='action')cls=v==='UP'?'green':v==='DOWN'?'red':'dim';
    if(c.k==='actual')cls=v==='UP'?'green':v==='DOWN'?'red':'dim';
    if(c.k==='pnl')cls=v>=0?'green':'red';
    if(c.k==='exit_reason')cls=v==='hold_to_resolution'?'dim':v==='take_profit'?'green':(v==='market_disagree'||v==='stop_loss')?'red':v==='flip'?'yellow':'dim';
    let d=v;if(c.fmt&&v!=null)d=c.fmt(v);else if(v==null)d='\u2014';
    return`<td class="${cls}">${d}</td>`;
  }).join('')+'</tr>').join('');
}

function renderPreds(rows) {
  if(!rows.length){document.getElementById('predsHead').innerHTML='<th>No predictions yet</th>';document.getElementById('predsBody').innerHTML='';return;}
  const cols=[
    {k:'created_at',l:'Time',fmt:v=>v?(v.split(' ')[1]||v):''},{k:'elapsed_s',l:'Elapsed'},
    {k:'direction',l:'Dir'},{k:'confidence',l:'Conf',fmt:v=>(v*100).toFixed(1)+'%'},
    {k:'prob_up',l:'P(Up)',fmt:v=>(v*100).toFixed(1)+'%'},{k:'edge_val',l:'Edge',fmt:v=>(v*100).toFixed(1)+'%'},
    {k:'poly_up',l:'Poly Up',fmt:v=>(v*100).toFixed(1)+'c'},{k:'poly_down',l:'Poly Dn',fmt:v=>(v*100).toFixed(1)+'c'},
    {k:'chainlink_price',l:'BTC',fmt:v=>'$'+Number(v).toLocaleString()},
    {k:'traded',l:'Traded',fmt:v=>v?'Yes':'No'},
  ];
  document.getElementById('predsHead').innerHTML=cols.map(c=>`<th>${c.l}</th>`).join('');
  document.getElementById('predsBody').innerHTML=rows.slice(0,50).map(r=>'<tr>'+cols.map(c=>{
    let v=r[c.k],cls='';
    if(c.k==='direction')cls=v==='UP'?'green':'red';
    if(c.k==='traded')cls=v?'green':'dim';
    let d=c.fmt&&v!=null?c.fmt(v):(v!=null?v:'\u2014');
    return`<td class="${cls}">${d}</td>`;
  }).join('')+'</tr>').join('');
}

function loadAlerts() {
  fetch('/api/alerts').then(r=>r.json()).then(d=>{
    const el=document.getElementById('alertList');
    if(!d.length){el.innerHTML='<div style="padding:20px;text-align:center;" class="dim">No alerts yet</div>';return;}
    el.innerHTML=d.map(a=>{
      const ok=a.sent_ok?'green':'red';const time=a.created_at?a.created_at.split('T')[1]?.substring(0,8)||'':'';
      return`<div class="alert-item"><span class="dim" style="min-width:60px;">${time}</span><span class="alert-type ${ok}">${a.alert_type}</span><span style="flex:1;">${a.message}</span>${a.error_msg?'<span class="red" style="font-size:11px;">'+a.error_msg+'</span>':''}</div>`;
    }).join('');
  });
}

function loadLogs() {
  const level=document.getElementById('logLevel').value;
  fetch('/api/logs?level='+level).then(r=>r.json()).then(d=>{
    const el=document.getElementById('logViewer');
    if(!d.length){el.innerHTML='<div style="padding:20px;text-align:center;" class="dim">No log entries</div>';return;}
    el.innerHTML=d.map(l=>{
      const time=l.timestamp?l.timestamp.split('T')[1]?.substring(0,12)||'':'';
      return`<div class="log-entry log-${l.level}"><span class="dim">${time}</span> <span style="font-weight:600;">[${l.level}]</span> ${l.event}${l.data?' <span class="dim">'+JSON.stringify(l.data)+'</span>':''}</div>`;
    }).join('');
  });
}

loadAll();
setRefresh();
// Live panel always polls (pinned, not a tab)
loadLive();
setInterval(loadLive, 1500);
</script>
</body>
</html>
"""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
@require_auth
def index():
    return render_template_string(HTML)


@app.route("/api/summary")
@require_auth
def api_summary():
    conn = get_db()
    trades = conn.execute(
        "SELECT * FROM trades WHERE action != 'skip' AND won IS NOT NULL"
    ).fetchall()
    balance_row = conn.execute(
        "SELECT balance_after FROM trades ORDER BY id DESC LIMIT 1"
    ).fetchone()
    skipped = conn.execute(
        "SELECT COUNT(*) as c FROM trades WHERE action = 'skip'"
    ).fetchone()["c"]
    conn.close()

    if not trades:
        return jsonify(
            balance=100.0, total_pnl=0, total_trades=0, wins=0, losses=0,
            win_rate=0, avg_pnl=0, avg_confidence=0, avg_edge=0,
            avg_conviction=None, skipped=skipped,
        )

    wins = sum(1 for t in trades if t["won"])
    losses = sum(1 for t in trades if not t["won"])
    total_pnl = sum(t["pnl"] or 0 for t in trades)
    n = len(trades)
    convictions = [t["entry_conviction"] for t in trades if t["entry_conviction"] is not None]

    return jsonify(
        balance=balance_row["balance_after"] if balance_row else 100.0,
        total_pnl=total_pnl,
        total_trades=n,
        wins=wins,
        losses=losses,
        win_rate=(wins / n * 100) if n else 0,
        avg_pnl=total_pnl / n if n else 0,
        avg_confidence=sum(t["confidence"] or 0 for t in trades) / n,
        avg_edge=sum(t["edge_val"] or 0 for t in trades) / n,
        avg_conviction=sum(convictions) / len(convictions) if convictions else None,
        skipped=skipped,
    )


@app.route("/api/trades")
@require_auth
def api_trades():
    conn = get_db()
    rows = conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 200").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/predictions")
@require_auth
def api_predictions():
    conn = get_db()
    rows = conn.execute("SELECT * FROM predictions ORDER BY id DESC LIMIT 50").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/exits")
@require_auth
def api_exits():
    conn = get_db()
    rows = conn.execute("""
        SELECT exit_reason as reason, COUNT(*) as count,
               SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
               SUM(pnl) as total_pnl, AVG(pnl) as avg_pnl
        FROM trades WHERE action != 'skip' AND won IS NOT NULL
        GROUP BY exit_reason ORDER BY count DESC
    """).fetchall()
    conn.close()
    return jsonify([{
        "reason": r["reason"], "count": r["count"],
        "wins": r["wins"] or 0,
        "win_rate": ((r["wins"] or 0) / r["count"] * 100) if r["count"] else 0,
        "total_pnl": r["total_pnl"] or 0, "avg_pnl": r["avg_pnl"] or 0,
    } for r in rows])


@app.route("/api/validation")
@require_auth
def api_validation():
    """Compute all 13 model validation metrics with acceptance thresholds."""
    conn = get_db()
    trades = conn.execute(
        "SELECT * FROM trades WHERE action != 'skip' AND won IS NOT NULL ORDER BY id"
    ).fetchall()
    preds = conn.execute(
        "SELECT * FROM predictions WHERE traded = 1 ORDER BY id"
    ).fetchall()
    conn.close()

    metrics = []
    n = len(trades)

    if n == 0:
        return jsonify([])

    wins = [t for t in trades if t["won"]]
    losses = [t for t in trades if not t["won"]]
    pnls = [t["pnl"] or 0 for t in trades]
    win_pnls = [t["pnl"] or 0 for t in wins]
    loss_pnls = [t["pnl"] or 0 for t in losses]

    # ── 1. Total Trades ──
    pct_of_200 = min(100, n / 200 * 100)
    metrics.append({
        "name": "Total Trades",
        "display": str(n),
        "threshold": "Target: >= 200 trades",
        "detail": f"{200 - n} more needed" if n < 200 else "Sample size sufficient",
        "status": "pass" if n >= 200 else "warn" if n >= 100 else "fail",
        "acceptance_pct": pct_of_200,
    })

    # ── 2. Win Rate ──
    wr = len(wins) / n * 100 if n else 0
    metrics.append({
        "name": "Win Rate",
        "display": f"{wr:.1f}%",
        "threshold": "Accept: >= 55% | Strong: >= 60%",
        "detail": f"{len(wins)}W / {len(losses)}L",
        "status": "pass" if wr >= 55 else "warn" if wr >= 50 else "fail",
        "acceptance_pct": min(100, wr / 60 * 100),
    })

    # ── 3. Avg Win / Avg Loss Ratio ──
    avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0
    avg_loss = abs(sum(loss_pnls) / len(loss_pnls)) if loss_pnls else 1
    wl_ratio = avg_win / avg_loss if avg_loss > 0 else 0
    metrics.append({
        "name": "Win/Loss Ratio",
        "display": f"{wl_ratio:.2f}",
        "threshold": "Accept: >= 1.0 | Ideal: 1.2-1.5",
        "detail": f"Avg win: ${avg_win:.2f} / Avg loss: ${avg_loss:.2f}",
        "status": "pass" if wl_ratio >= 1.0 else "warn" if wl_ratio >= 0.8 else "fail",
        "acceptance_pct": min(100, wl_ratio / 1.5 * 100),
    })

    # ── 4. Expected Value (EV) ──
    ev = sum(pnls) / n if n else 0
    metrics.append({
        "name": "Expected Value (EV)",
        "display": f"${ev:+.2f}",
        "threshold": "Accept: EV > $0 consistently",
        "detail": f"Total P&L: ${sum(pnls):+.2f} over {n} trades",
        "status": "pass" if ev > 0 else "warn" if ev > -0.20 else "fail",
        "acceptance_pct": min(100, max(0, (ev + 1) / 2 * 100)),
    })

    # ── 5. Profit Factor ──
    gross_profit = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p < 0))
    pf = gross_profit / gross_loss if gross_loss > 0 else (99.0 if gross_profit > 0 else 0)
    metrics.append({
        "name": "Profit Factor",
        "display": f"{pf:.2f}",
        "threshold": "Accept: >= 1.3 | Strong: >= 1.5",
        "detail": f"Gross profit: ${gross_profit:.2f} / Gross loss: ${gross_loss:.2f}",
        "status": "pass" if pf >= 1.3 else "warn" if pf >= 1.0 else "fail",
        "acceptance_pct": min(100, pf / 1.5 * 100),
    })

    # ── 6. Maximum Drawdown ──
    cum = 0
    peak = 0
    max_dd = 0
    for p in pnls:
        cum += p
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
    # Read starting balance from config, fallback to module constant
    conn2 = get_db()
    sb_row = conn2.execute("SELECT value FROM bot_config WHERE key='starting_balance'").fetchone()
    conn2.close()
    starting_bal = float(sb_row["value"]) if sb_row else STARTING_BALANCE
    dd_pct = (max_dd / starting_bal * 100) if starting_bal > 0 else 0
    metrics.append({
        "name": "Maximum Drawdown",
        "display": f"${max_dd:.2f} ({dd_pct:.1f}%)",
        "threshold": "Accept: <= 25% of starting balance",
        "detail": f"Peak equity: ${peak:+.2f} above start",
        "status": "pass" if dd_pct <= 25 else "warn" if dd_pct <= 35 else "fail",
        "acceptance_pct": min(100, max(0, (50 - dd_pct) / 50 * 100)),
    })

    # ── 7. Sharpe Ratio ──
    if n >= 2:
        mean_pnl = sum(pnls) / n
        std_pnl = (sum((p - mean_pnl) ** 2 for p in pnls) / (n - 1)) ** 0.5
        sharpe = (mean_pnl / std_pnl) if std_pnl > 0 else 0
        # Annualize: ~105,120 five-minute periods per year
        sharpe_ann = sharpe * math.sqrt(105120)
    else:
        sharpe = 0
        sharpe_ann = 0
    metrics.append({
        "name": "Sharpe Ratio",
        "display": f"{sharpe_ann:.2f}",
        "threshold": "Accept: >= 1.0 | Good: >= 1.5",
        "detail": f"Per-trade Sharpe: {sharpe:.4f}",
        "status": "pass" if sharpe_ann >= 1.0 else "warn" if sharpe_ann >= 0.5 else "fail",
        "acceptance_pct": min(100, max(0, sharpe_ann / 2.0 * 100)),
    })

    # ── 8. Equity Curve Slope ──
    if n >= 5:
        # Linear regression: y = pnl cumulative, x = trade index
        xs = list(range(n))
        cum_pnls = []
        c = 0
        for p in pnls:
            c += p
            cum_pnls.append(c)
        x_mean = sum(xs) / n
        y_mean = sum(cum_pnls) / n
        num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, cum_pnls))
        den = sum((x - x_mean) ** 2 for x in xs)
        slope = num / den if den > 0 else 0
        # R-squared
        ss_res = sum((y - (slope * x + (y_mean - slope * x_mean))) ** 2 for x, y in zip(xs, cum_pnls))
        ss_tot = sum((y - y_mean) ** 2 for y in cum_pnls)
        r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
    else:
        slope = 0
        r2 = 0
    metrics.append({
        "name": "Equity Curve Slope",
        "display": f"${slope:+.3f}/trade",
        "threshold": "Accept: positive slope, consistent growth",
        "detail": f"R² = {r2:.2f} (1.0 = perfectly linear growth)",
        "status": "pass" if slope > 0 and r2 > 0.3 else "warn" if slope > 0 else "fail",
        "acceptance_pct": min(100, max(0, (slope + 0.5) / 1.0 * 100)),
    })

    # ── 9. Edge vs Random Baseline ──
    # Random baseline: 50% win rate with same avg bet → expected loss from spread
    avg_buy = sum((t["buy_price"] or 0.5) for t in trades) / n
    random_ev = 0.5 * (1.0 - avg_buy) - 0.5 * avg_buy  # per share
    actual_wr = len(wins) / n if n else 0.5
    model_ev = ev
    edge_vs_random = (actual_wr - 0.5) * 100  # percentage points above 50%
    metrics.append({
        "name": "Edge vs Random",
        "display": f"+{edge_vs_random:.1f}%" if edge_vs_random >= 0 else f"{edge_vs_random:.1f}%",
        "threshold": "Accept: >= 10-15% above 50% baseline",
        "detail": f"Model WR: {actual_wr*100:.1f}% vs Random: 50%",
        "status": "pass" if edge_vs_random >= 10 else "warn" if edge_vs_random >= 5 else "fail",
        "acceptance_pct": min(100, max(0, edge_vs_random / 15 * 100)),
    })

    # ── 10. Execution Latency ──
    # Measure when the bot actually enters relative to the 90-240s entry window
    # Phase 1: 90-120s (strong only), Phase 2: 120-180s (moderate), Phase 3: 180-240s (strong)
    # Slam entries can happen at 30s+. Most entries naturally land in Phase 2 (120-180s).
    if preds:
        latencies = [p["elapsed_s"] for p in preds if p["elapsed_s"] is not None]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        min_lat = min(latencies) if latencies else 0
        max_lat = max(latencies) if latencies else 0
    else:
        avg_latency = 0
        min_lat = 0
        max_lat = 0
    # Good: entering before 180s (Phases 1-2). Warn: 180-240s (Phase 3, late). Fail: no entry.
    metrics.append({
        "name": "Execution Latency",
        "display": f"{avg_latency:.0f}s avg entry",
        "threshold": "Accept: avg entry <= 180s (Phase 1-2)",
        "detail": f"Range: {min_lat:.0f}s-{max_lat:.0f}s | P1: <120s, P2: 120-180s, P3: 180-240s",
        "status": "pass" if avg_latency <= 180 else "warn" if avg_latency <= 240 else "fail",
        "acceptance_pct": min(100, max(0, (240 - avg_latency) / 150 * 100)),
    })

    # ── 11. Confidence Calibration ──
    # Compare average predicted confidence to actual win rate
    avg_conf = sum((t["confidence"] or 0.5) for t in trades) / n
    cal_error = abs(actual_wr - avg_conf)
    metrics.append({
        "name": "Confidence Calibration",
        "display": f"{cal_error*100:.1f}% error",
        "threshold": "Accept: predicted prob ~ actual win rate (error < 5%)",
        "detail": f"Avg confidence: {avg_conf*100:.1f}% vs Actual WR: {actual_wr*100:.1f}%",
        "status": "pass" if cal_error < 0.05 else "warn" if cal_error < 0.10 else "fail",
        "acceptance_pct": min(100, max(0, (15 - cal_error * 100) / 15 * 100)),
    })

    # ── 12. Rolling Win Rate Stability ──
    rolling_window = min(50, max(10, n // 3))
    min_rolling_wr = 100
    if n >= rolling_window:
        for i in range(rolling_window, n + 1):
            window = trades[i - rolling_window:i]
            wr_w = sum(1 for t in window if t["won"]) / len(window) * 100
            if wr_w < min_rolling_wr:
                min_rolling_wr = wr_w
    else:
        min_rolling_wr = wr
    metrics.append({
        "name": "Rolling WR Stability",
        "display": f"{min_rolling_wr:.1f}% min",
        "threshold": f"Accept: no drop below 50% in rolling {rolling_window} trades",
        "detail": f"Worst {rolling_window}-trade window win rate",
        "status": "pass" if min_rolling_wr >= 50 else "warn" if min_rolling_wr >= 45 else "fail",
        "acceptance_pct": min(100, min_rolling_wr / 50 * 100),
    })

    # ── 13. Market Regime Stability ──
    # Split trades into volatility regimes based on price_to_beat changes
    ptbs = [(t["price_to_beat"] or 0) for t in trades]
    if n >= 10 and len(set(ptbs)) > 1:
        # Compute 5-trade rolling volatility of PTB
        regimes = {"low_vol": [], "mid_vol": [], "high_vol": []}
        for i in range(5, n):
            window_ptbs = ptbs[i-5:i]
            vol = max(window_ptbs) - min(window_ptbs)
            t = trades[i]
            if vol < 50:
                regimes["low_vol"].append(t)
            elif vol < 200:
                regimes["mid_vol"].append(t)
            else:
                regimes["high_vol"].append(t)

        positive_regimes = 0
        regime_details = []
        for name, rtrades in regimes.items():
            if len(rtrades) >= 3:
                r_pnl = sum(t["pnl"] or 0 for t in rtrades)
                r_wr = sum(1 for t in rtrades if t["won"]) / len(rtrades) * 100
                if r_pnl > 0:
                    positive_regimes += 1
                regime_details.append(f"{name}: {len(rtrades)}t, {r_wr:.0f}%WR, ${r_pnl:+.2f}")
        active_regimes = sum(1 for r in regimes.values() if len(r) >= 3)
    else:
        positive_regimes = 0
        active_regimes = 0
        regime_details = ["Not enough data for regime analysis"]

    metrics.append({
        "name": "Market Regime Stability",
        "display": f"{positive_regimes}/{active_regimes} profitable",
        "threshold": "Accept: positive EV in >= 2 regimes",
        "detail": " | ".join(regime_details) if regime_details else "N/A",
        "status": "pass" if positive_regimes >= 2 else "warn" if positive_regimes >= 1 else "fail" if active_regimes >= 2 else "na",
        "acceptance_pct": min(100, positive_regimes / max(1, min(active_regimes, 3)) * 100),
    })

    return jsonify(metrics)


# ─── Config API ──────────────────────────────────────────────────────────────

@app.route("/api/config")
@require_auth
def api_config():
    conn = get_db()
    rows = conn.execute("SELECT key, value, updated_at FROM bot_config ORDER BY key").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/config", methods=["POST"])
@require_auth
def api_config_update():
    data = request.get_json()
    if not data:
        return jsonify(ok=False, error="No data"), 400

    conn = get_db()
    now_str = datetime.now(timezone.utc).isoformat()

    # Check if starting_balance changed (triggers rebalance)
    rebalance = False
    if "starting_balance" in data:
        old = conn.execute("SELECT value FROM bot_config WHERE key='starting_balance'").fetchone()
        if old and old["value"] != str(data["starting_balance"]):
            rebalance = True

    for key, value in data.items():
        conn.execute(
            "INSERT OR REPLACE INTO bot_config (key, value, updated_at) VALUES (?, ?, ?)",
            (key, str(value), now_str),
        )
    conn.commit()

    if rebalance:
        _rebalance_trades(conn, float(data["starting_balance"]))

    conn.close()
    log.info("Config updated", extra={"data": {k: v for k, v in data.items() if "token" not in k}})
    return jsonify(ok=True)


def _rebalance_trades(conn, new_starting_balance):
    """Recalculate all balance_after values with new starting balance."""
    rows = conn.execute("SELECT * FROM trades ORDER BY window_ts ASC").fetchall()
    balance = new_starting_balance
    for r in rows:
        pnl = r["pnl"] or 0
        action = r["action"]
        if action != "skip" and pnl:
            balance += pnl
        conn.execute("UPDATE trades SET balance_after = ? WHERE id = ?",
                     (round(balance, 2), r["id"]))
    conn.commit()


# ─── Daily Stats API ─────────────────────────────────────────────────────────

@app.route("/api/daily")
@require_auth
def api_daily():
    conn = get_db()
    # Compute daily stats directly from trades table (authoritative source)
    rows = conn.execute("""
        SELECT DATE(time_str) as date,
               COUNT(*) as trades_count,
               SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END) as losses,
               SUM(pnl) as pnl
        FROM trades
        GROUP BY DATE(time_str)
        ORDER BY date DESC
        LIMIT 30
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ─── Slippage API ────────────────────────────────────────────────────────────

@app.route("/api/slippage")
@require_auth
def api_slippage():
    conn = get_db()
    row = conn.execute("""
        SELECT AVG(slippage_pct) as avg_slippage,
               SUM(slippage_cost) as total_cost,
               MAX(slippage_pct) as max_slippage,
               COUNT(*) as count
        FROM trades
        WHERE slippage_pct IS NOT NULL AND slippage_pct > 0
    """).fetchone()
    conn.close()
    return jsonify({
        "avg_slippage": row["avg_slippage"] or 0,
        "total_cost": row["total_cost"] or 0,
        "max_slippage": row["max_slippage"] or 0,
        "count": row["count"] or 0,
    })


# ─── Telegram Test API ──────────────────────────────────────────────────────

@app.route("/api/telegram/test", methods=["POST"])
@require_auth
def api_telegram_test():
    data = request.get_json()
    token = data.get("token", "")
    chat_id = data.get("chat_id", "")
    if not token or not chat_id:
        return jsonify(ok=False, error="Token and Chat ID required")
    from notifications import test_connection
    ok, err = test_connection(token, chat_id)
    return jsonify(ok=ok, error=err)


# ─── Alert Log API ──────────────────────────────────────────────────────────

@app.route("/api/alerts")
@require_auth
def api_alerts():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM alert_log ORDER BY id DESC LIMIT 50"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ─── Log Viewer API ─────────────────────────────────────────────────────────

@app.route("/api/logs")
@require_auth
def api_logs():
    level_filter = request.args.get("level", "all")
    log_file = "data/bot.log"
    if not os.path.exists(log_file):
        return jsonify([])

    levels = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
    min_level = levels.get(level_filter, 0) if level_filter != "all" else 0

    entries = []
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for line in lines[-100:]:
            try:
                entry = json.loads(line.strip())
                entry_level = levels.get(entry.get("level", "DEBUG"), 0)
                if entry_level >= min_level:
                    entries.append(entry)
            except (json.JSONDecodeError, ValueError):
                continue
    except Exception:
        pass

    return jsonify(list(reversed(entries)))


# ─── Live State API ──────────────────────────────────────────────────────────

@app.route("/api/live")
@require_auth
def api_live():
    path = "data/live_state.json"
    if not os.path.exists(path):
        return jsonify({"error": "Bot not running"}), 503
    try:
        with open(path, "r") as f:
            data = json.load(f)
        if _time.time() - data.get("timestamp", 0) > 10:
            data["stale"] = True
        return jsonify(data)
    except Exception:
        return jsonify({"error": "Failed to read state"}), 500


if __name__ == "__main__":
    import subprocess
    import sys

    # Auto-start auto.py in background
    bot_proc = None
    auto_py = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auto.py")
    if os.path.exists(auto_py):
        print("[*] Starting bot (auto.py) in background...")
        bot_proc = subprocess.Popen(
            [sys.executable, auto_py],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print(f"[*] Bot started (PID: {bot_proc.pid})")
    else:
        print("[WARN] auto.py not found, bot not started")

    # Wait briefly for DB init if needed
    import time as _t
    for _ in range(10):
        if os.path.exists(DB_PATH):
            break
        _t.sleep(0.5)

    if not os.path.exists(DB_PATH):
        print(f"[ERROR] Database not found at {DB_PATH}")
        exit(1)

    print("Dashboard: http://localhost:5050")
    try:
        app.run(host="0.0.0.0", port=5050, debug=False)
    finally:
        if bot_proc:
            print("[*] Stopping bot...")
            bot_proc.terminate()
            bot_proc.wait(timeout=5)
