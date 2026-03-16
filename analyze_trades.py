import sqlite3

db_path = "C:/Users/user/Desktop/Projects/btc-pred/data/trades.db"
conn = sqlite3.connect(db_path)
c = conn.cursor()

print("="*80)
print("0. SCHEMA")
print("="*80)
c.execute("SELECT sql FROM sqlite_master WHERE type='table'")
for row in c.fetchall():
    print(row[0])
    print()

print("="*80)
print("1. OVERALL STATS")
print("="*80)
c.execute("""
    SELECT
        COUNT(*) as total_trades,
        SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN won=0 THEN 1 ELSE 0 END) as losses,
        ROUND(100.0*SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)/COUNT(*),2) as win_rate_pct,
        ROUND(SUM(pnl),4) as total_pnl,
        ROUND(AVG(pnl),4) as avg_pnl
    FROM trades
""")
row = c.fetchone()
print(f"Total Trades: {row[0]}")
print(f"Wins: {row[1]}")
print(f"Losses: {row[2]}")
print(f"Win Rate: {row[3]}%")
print(f"Total PnL: {row[4]}")
print(f"Avg PnL/Trade: {row[5]}")

print()
print("="*80)
print("2. PERFORMANCE BY EXIT_REASON")
print("="*80)
c.execute("""
    SELECT
        exit_reason,
        COUNT(*) as cnt,
        SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN won=0 THEN 1 ELSE 0 END) as losses,
        ROUND(100.0*SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)/COUNT(*),2) as win_rate,
        ROUND(SUM(pnl),4) as total_pnl,
        ROUND(AVG(pnl),4) as avg_pnl
    FROM trades
    GROUP BY exit_reason
    ORDER BY cnt DESC
""")
header = f"{'Exit Reason':<25} {'Count':>6} {'Wins':>6} {'Losses':>6} {'WinRate%':>8} {'TotalPnL':>12} {'AvgPnL':>10}"
print(header)
print("-"*80)
for row in c.fetchall():
    er = str(row[0]) if row[0] else "NULL"
    print(f"{er:<25} {row[1]:>6} {row[2]:>6} {row[3]:>6} {row[4]:>8} {row[5]:>12} {row[6]:>10}")

print()
print("="*80)
print("3. PERFORMANCE BY ACTION (UP vs DOWN)")
print("="*80)
c.execute("""
    SELECT
        action,
        COUNT(*) as cnt,
        ROUND(100.0*SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)/COUNT(*),2) as win_rate,
        ROUND(AVG(pnl),4) as avg_pnl,
        ROUND(SUM(pnl),4) as total_pnl
    FROM trades
    GROUP BY action
    ORDER BY action
""")
print(f"{'Action':<10} {'Count':>6} {'WinRate%':>8} {'AvgPnL':>10} {'TotalPnL':>12}")
print("-"*50)
for row in c.fetchall():
    print(f"{str(row[0]):<10} {row[1]:>6} {row[2]:>8} {row[3]:>10} {row[4]:>12}")

print()
print("="*80)
print("4. PERFORMANCE BY CONFIDENCE BUCKETS")
print("="*80)
c.execute("""
    SELECT
        CASE
            WHEN confidence < 0.55 THEN '< 0.55'
            WHEN confidence < 0.60 THEN '0.55-0.60'
            WHEN confidence < 0.65 THEN '0.60-0.65'
            WHEN confidence < 0.70 THEN '0.65-0.70'
            WHEN confidence < 0.75 THEN '0.70-0.75'
            ELSE '0.75+'
        END as bucket,
        COUNT(*) as cnt,
        ROUND(100.0*SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)/COUNT(*),2) as win_rate,
        ROUND(AVG(pnl),4) as avg_pnl,
        ROUND(SUM(pnl),4) as total_pnl
    FROM trades
    GROUP BY bucket
    ORDER BY bucket
""")
print(f"{'Confidence':<12} {'Count':>6} {'WinRate%':>8} {'AvgPnL':>10} {'TotalPnL':>12}")
print("-"*52)
for row in c.fetchall():
    print(f"{str(row[0]):<12} {row[1]:>6} {row[2]:>8} {row[3]:>10} {row[4]:>12}")

print()
print("="*80)
print("5. PERFORMANCE BY EDGE BUCKETS")
print("="*80)
c.execute("""
    SELECT
        CASE
            WHEN edge_val < 0.03 THEN '< 0.03'
            WHEN edge_val < 0.05 THEN '0.03-0.05'
            WHEN edge_val < 0.10 THEN '0.05-0.10'
            WHEN edge_val < 0.15 THEN '0.10-0.15'
            ELSE '0.15+'
        END as bucket,
        COUNT(*) as cnt,
        ROUND(100.0*SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)/COUNT(*),2) as win_rate,
        ROUND(AVG(pnl),4) as avg_pnl,
        ROUND(SUM(pnl),4) as total_pnl
    FROM trades
    GROUP BY bucket
    ORDER BY bucket
""")
print(f"{'Edge':<12} {'Count':>6} {'WinRate%':>8} {'AvgPnL':>10} {'TotalPnL':>12}")
print("-"*52)
for row in c.fetchall():
    print(f"{str(row[0]):<12} {row[1]:>6} {row[2]:>8} {row[3]:>10} {row[4]:>12}")

print()
print("="*80)
print("6. AVG BET SIZE: WINS vs LOSSES")
print("="*80)
c.execute("""
    SELECT
        CASE WHEN won=1 THEN 'Win' ELSE 'Loss' END as outcome,
        ROUND(AVG(bet_size),4) as avg_bet,
        ROUND(MIN(bet_size),4) as min_bet,
        ROUND(MAX(bet_size),4) as max_bet
    FROM trades
    GROUP BY won
""")
for row in c.fetchall():
    print(f"{row[0]}: avg={row[1]}, min={row[2]}, max={row[3]}")

print()
print("="*80)
print("7. SLIPPAGE STATS")
print("="*80)
try:
    c.execute("""
        SELECT
            ROUND(AVG(slippage_pct),6) as avg_slippage_pct,
            ROUND(SUM(slippage_cost),4) as total_slippage_cost,
            ROUND(AVG(slippage_cost),4) as avg_slippage_cost,
            COUNT(CASE WHEN slippage_pct > 0 THEN 1 END) as positive_slippage_cnt,
            COUNT(CASE WHEN slippage_pct < 0 THEN 1 END) as negative_slippage_cnt,
            COUNT(CASE WHEN slippage_pct IS NOT NULL THEN 1 END) as non_null
        FROM trades
    """)
    row = c.fetchone()
    print(f"Avg Slippage %: {row[0]}")
    print(f"Total Slippage Cost: {row[1]}")
    print(f"Avg Slippage Cost: {row[2]}")
    print(f"Trades with positive slippage: {row[3]}")
    print(f"Trades with negative slippage: {row[4]}")
    print(f"Non-null slippage records: {row[5]}")
except Exception as e:
    print(f"Slippage columns not found: {e}")

print()
print("="*80)
print("8. CONSECUTIVE LOSSES (LONGEST STREAK)")
print("="*80)
c.execute("SELECT won FROM trades ORDER BY rowid")
results = [r[0] for r in c.fetchall()]
max_loss_streak = 0
current_streak = 0
max_win_streak = 0
current_win = 0
for w in results:
    if w == 0:
        current_streak += 1
        max_loss_streak = max(max_loss_streak, current_streak)
        current_win = 0
    else:
        current_win += 1
        max_win_streak = max(max_win_streak, current_win)
        current_streak = 0
print(f"Longest loss streak: {max_loss_streak}")
print(f"Longest win streak: {max_win_streak}")

print()
print("="*80)
print("9. TIME-OF-DAY ANALYSIS (by hour)")
print("="*80)
try:
    c.execute("""
        SELECT
            SUBSTR(time_str, 12, 2) as hour,
            COUNT(*) as cnt,
            ROUND(100.0*SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)/COUNT(*),2) as win_rate,
            ROUND(AVG(pnl),4) as avg_pnl,
            ROUND(SUM(pnl),4) as total_pnl
        FROM trades
        WHERE time_str IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """)
    rows = c.fetchall()
    print(f"{'Hour':<6} {'Count':>6} {'WinRate%':>8} {'AvgPnL':>10} {'TotalPnL':>12}")
    print("-"*46)
    for row in rows:
        print(f"{str(row[0]):<6} {row[1]:>6} {row[2]:>8} {row[3]:>10} {row[4]:>12}")
except Exception as e:
    print(f"Error: {e}")

print()
print("="*80)
print("10. LAST 20 TRADES")
print("="*80)
c.execute("""
    SELECT time_str, action, confidence, edge_val, buy_price, pnl, won, exit_reason, bet_size
    FROM trades ORDER BY rowid DESC LIMIT 20
""")
print(f"{'Time':<22} {'Act':>4} {'Conf':>6} {'Edge':>6} {'BuyPx':>8} {'PnL':>10} {'Won':>4} {'ExitReason':<22} {'BetSize':>8}")
print("-"*100)
for row in c.fetchall():
    print(f"{str(row[0]):<22} {str(row[1]):>4} {row[2]:>6.3f} {row[3]:>6.3f} {row[4]:>8.4f} {row[5]:>10.4f} {row[6]:>4} {str(row[7]):<22} {row[8]:>8.4f}")

print()
print("="*80)
print("11. AVG CONFIDENCE & EDGE: CORRECT vs WRONG")
print("="*80)
c.execute("""
    SELECT
        CASE WHEN won=1 THEN 'Correct' ELSE 'Wrong' END as outcome,
        COUNT(*) as cnt,
        ROUND(AVG(confidence),4) as avg_conf,
        ROUND(AVG(edge_val),4) as avg_edge,
        ROUND(AVG(bet_size),4) as avg_bet
    FROM trades
    GROUP BY won
""")
for row in c.fetchall():
    print(f"{row[0]}: count={row[1]}, avg_conf={row[2]}, avg_edge={row[3]}, avg_bet={row[4]}")

print()
print("="*80)
print("12. BUY_PRICE DISTRIBUTION")
print("="*80)
c.execute("""
    SELECT
        CASE
            WHEN buy_price < 0.40 THEN '< 0.40'
            WHEN buy_price < 0.50 THEN '0.40-0.50'
            WHEN buy_price < 0.60 THEN '0.50-0.60'
            ELSE '0.60+'
        END as bucket,
        COUNT(*) as cnt,
        ROUND(100.0*SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)/COUNT(*),2) as win_rate,
        ROUND(AVG(pnl),4) as avg_pnl,
        ROUND(SUM(pnl),4) as total_pnl
    FROM trades
    GROUP BY bucket
    ORDER BY bucket
""")
print(f"{'BuyPrice':<12} {'Count':>6} {'WinRate%':>8} {'AvgPnL':>10} {'TotalPnL':>12}")
print("-"*52)
for row in c.fetchall():
    print(f"{str(row[0]):<12} {row[1]:>6} {row[2]:>8} {row[3]:>10} {row[4]:>12}")

print()
print("="*80)
print("13. PREDICTIONS TABLE")
print("="*80)
try:
    c.execute("SELECT COUNT(*), ROUND(AVG(confidence),4), ROUND(AVG(edge_val),4) FROM predictions")
    row = c.fetchone()
    print(f"Total predictions: {row[0]}, Avg confidence: {row[1]}, Avg edge: {row[2]}")

    c.execute("PRAGMA table_info(predictions)")
    cols = [r[1] for r in c.fetchall()]
    print(f"Columns: {cols}")

    if "traded" in cols:
        c.execute("""
            SELECT traded, COUNT(*), ROUND(AVG(confidence),4), ROUND(AVG(edge_val),4)
            FROM predictions GROUP BY traded
        """)
        print("Traded vs Not-Traded:")
        for row in c.fetchall():
            print(f"  traded={row[0]}: count={row[1]}, avg_conf={row[2]}, avg_edge={row[3]}")
    else:
        print("No 'traded' column in predictions table")
except Exception as e:
    print(f"Predictions table error: {e}")

print()
print("="*80)
print("15. DAILY P&L BREAKDOWN")
print("="*80)
c.execute("""
    SELECT
        SUBSTR(time_str, 1, 10) as date,
        COUNT(*) as trades,
        SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as wins,
        ROUND(100.0*SUM(CASE WHEN won=1 THEN 1 ELSE 0 END)/COUNT(*),1) as wr,
        ROUND(SUM(pnl),4) as daily_pnl
    FROM trades
    WHERE time_str IS NOT NULL
    GROUP BY date
    ORDER BY date
""")
rows = c.fetchall()
cumulative = 0.0
print(f"{'Date':<12} {'Trades':>6} {'Wins':>5} {'WR%':>6} {'DailyPnL':>12} {'CumulPnL':>12}")
print("-"*58)
for row in rows:
    cumulative += row[4]
    print(f"{str(row[0]):<12} {row[1]:>6} {row[2]:>5} {row[3]:>6} {row[4]:>12} {cumulative:>12.4f}")

print()
print("="*80)
print("16. HOLDING TIME / ELAPSED TIME")
print("="*80)
c.execute("PRAGMA table_info(trades)")
trade_cols = [r[1] for r in c.fetchall()]
print(f"Trade columns: {trade_cols}")
time_cols = ["holding_time", "elapsed", "duration", "entry_elapsed", "exit_time", "close_time", "hold_minutes", "seconds_held"]
for col in time_cols:
    if col in trade_cols:
        c.execute(f"SELECT ROUND(AVG({col}),2), ROUND(MIN({col}),2), ROUND(MAX({col}),2) FROM trades")
        row = c.fetchone()
        print(f"{col}: avg={row[0]}, min={row[1]}, max={row[2]}")

if "entry_elapsed" in trade_cols:
    c.execute("""
        SELECT
            CASE WHEN won=1 THEN 'Win' ELSE 'Loss' END,
            ROUND(AVG(entry_elapsed),2)
        FROM trades GROUP BY won
    """)
    print("Entry elapsed by outcome:")
    for row in c.fetchall():
        print(f"  {row[0]}: {row[1]}")

conn.close()
print()
print("DONE")
