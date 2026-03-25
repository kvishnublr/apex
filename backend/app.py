"""
APEX NSE v7 — Flask Backend
Pure Python. Requires: pip install flask kiteconnect
Data source: Zerodha Kite API ONLY (historical + live LTP)
No Yahoo Finance dependency
Works on Python 3.9, 3.10, 3.11, 3.12, 3.13 — any version
Run: python backend/app.py
"""
import sys, os, json, sqlite3, datetime, math, random, threading, time
sys.path.insert(0, os.path.dirname(__file__))

from flask import Flask, jsonify, request, send_file, Response, stream_with_context
from engine import (
    UNIVERSE, SECTORS, FILTER_NAMES, WIN_RATES,
    gen_ohlcv, get_ohlcv, compute_indicators,
    score_candle, compute_levels, run_backtest,
    ENTRY_TIMES_OPEN, ENTRY_TIMES_POWER,
)

app  = Flask(__name__)
BASE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(BASE, "../data/trades.db")
CFG  = os.path.join(BASE, "../data/config.json")

_cache = {}   # sym -> (rows, inds)

# ── DATABASE ─────────────────────────────
def get_db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
    except Exception:
        pass
    return conn

def init_db():
    """Initialize SQLite database and ensure all columns exist."""
    conn = get_db()
    # Base tables
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, sector TEXT, direction TEXT, trade_type TEXT DEFAULT 'SWING',
        entry_date TEXT, exit_date TEXT,
        entry_price REAL, exit_price REAL, stop_loss REAL,
        qty INTEGER, risk_inr REAL, charges REAL,
        net_pnl REAL, actual_r REAL, exit_type TEXT,
        score INTEGER, hold_days INTEGER, notes TEXT,
        capital_before REAL, capital_after REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS signal_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_date TEXT, symbol TEXT, sector TEXT,
        direction TEXT, score INTEGER, trade_type TEXT DEFAULT 'SWING',
        entry REAL, sl REAL, t1 REAL, t2 REAL, t3 REAL,
        adx REAL, rsi REAL, vol_ratio REAL,
        filters TEXT, entry_time TEXT,
        atr REAL DEFAULT 0,
        live_price REAL DEFAULT 0,
        risk_pct REAL DEFAULT 0,
        status TEXT DEFAULT 'ACTIVE',
        triggered_at TEXT,
        pnl REAL DEFAULT 0,
        logged_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS sector_analysis (
        symbol TEXT PRIMARY KEY,
        sector TEXT, company TEXT, direction TEXT,
        score REAL, live_price REAL, entry_price REAL,
        sl REAL, t1 REAL, t2 REAL, t3 REAL,
        adx REAL, rsi REAL, vol_ratio REAL,
        risk_pct REAL, risk_per REAL, atr REAL, trade_type TEXT,
        e20 REAL DEFAULT 0, e50 REAL DEFAULT 0, e200 REAL DEFAULT 0,
        supertrend REAL DEFAULT 0, st_dir INTEGER DEFAULT 0,
        vwap REAL DEFAULT 0, poc REAL DEFAULT 0, va_low REAL DEFAULT 0, va_high REAL DEFAULT 0,
        index_tag TEXT DEFAULT '',
        index_weight REAL DEFAULT 0,
        ai_prediction REAL, ai_confidence INTEGER,
        momentum_score INTEGER, updated_at TEXT
    );
    """)
    
    # Check for missing columns in sector_analysis (for migrations)
    cursor = conn.execute("PRAGMA table_info(sector_analysis)")
    sector_info_rows = cursor.fetchall()
    columns = [row[1] for row in sector_info_rows]
    symbol_pk = any(r[1] == "symbol" and r[5] == 1 for r in sector_info_rows)
    if not symbol_pk:
        print("[DB] Migrating sector_analysis to v2 schema (dedupe by symbol)")
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sector_analysis_v2 (
            symbol TEXT PRIMARY KEY,
            sector TEXT, company TEXT, direction TEXT,
            score REAL, live_price REAL, entry_price REAL,
            sl REAL, t1 REAL, t2 REAL, t3 REAL,
            adx REAL, rsi REAL, vol_ratio REAL,
            risk_pct REAL, risk_per REAL, atr REAL, trade_type TEXT,
            e20 REAL DEFAULT 0, e50 REAL DEFAULT 0, e200 REAL DEFAULT 0,
            supertrend REAL DEFAULT 0, st_dir INTEGER DEFAULT 0,
            vwap REAL DEFAULT 0, poc REAL DEFAULT 0, va_low REAL DEFAULT 0, va_high REAL DEFAULT 0,
            ai_prediction REAL, ai_confidence INTEGER,
            momentum_score INTEGER, updated_at TEXT
        );
        """)
        conn.execute("""
            INSERT OR REPLACE INTO sector_analysis_v2
            (symbol, sector, company, direction, score, live_price, entry_price, sl, t1, t2, t3,
             adx, rsi, vol_ratio, risk_pct, risk_per, atr, trade_type,
             e20, e50, e200, supertrend, st_dir, vwap, poc, va_low, va_high,
             ai_prediction, ai_confidence, momentum_score, updated_at)
            SELECT symbol, sector, company, direction, score, live_price, entry_price, sl, t1, t2, t3,
                   adx, rsi, vol_ratio, risk_pct, risk_per, atr, trade_type,
                   e20, e50, e200, supertrend, st_dir, vwap, poc, va_low, va_high,
                   ai_prediction, ai_confidence, momentum_score, updated_at
            FROM sector_analysis
            WHERE id IN (SELECT MAX(id) FROM sector_analysis GROUP BY symbol)
        """)
        conn.execute("DROP TABLE sector_analysis")
        conn.execute("ALTER TABLE sector_analysis_v2 RENAME TO sector_analysis")
        cursor = conn.execute("PRAGMA table_info(sector_analysis)")
        sector_info_rows = cursor.fetchall()
        columns = [row[1] for row in sector_info_rows]
    
    if "t3" not in columns:
        print("[DB] Adding missing column 't3' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN t3 REAL DEFAULT 0")
    if "risk_per" not in columns:
        print("[DB] Adding missing column 'risk_per' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN risk_per REAL DEFAULT 0")
    if "atr" not in columns:
        print("[DB] Adding missing column 'atr' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN atr REAL DEFAULT 0")
    if "e20" not in columns:
        print("[DB] Adding missing column 'e20' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN e20 REAL DEFAULT 0")
    if "e50" not in columns:
        print("[DB] Adding missing column 'e50' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN e50 REAL DEFAULT 0")
    if "e200" not in columns:
        print("[DB] Adding missing column 'e200' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN e200 REAL DEFAULT 0")
    if "supertrend" not in columns:
        print("[DB] Adding missing column 'supertrend' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN supertrend REAL DEFAULT 0")
    if "st_dir" not in columns:
        print("[DB] Adding missing column 'st_dir' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN st_dir INTEGER DEFAULT 0")
    if "vwap" not in columns:
        print("[DB] Adding missing column 'vwap' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN vwap REAL DEFAULT 0")
    if "poc" not in columns:
        print("[DB] Adding missing column 'poc' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN poc REAL DEFAULT 0")
    if "va_low" not in columns:
        print("[DB] Adding missing column 'va_low' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN va_low REAL DEFAULT 0")
    if "va_high" not in columns:
        print("[DB] Adding missing column 'va_high' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN va_high REAL DEFAULT 0")
    if "index_tag" not in columns:
        print("[DB] Adding missing column 'index_tag' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN index_tag TEXT DEFAULT ''")
    if "index_weight" not in columns:
        print("[DB] Adding missing column 'index_weight' to sector_analysis")
        conn.execute("ALTER TABLE sector_analysis ADD COLUMN index_weight REAL DEFAULT 0")
        
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS price_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE, date TEXT,
        open_price REAL, high_price REAL, low_price REAL,
        close_price REAL, volume INTEGER,
        atr REAL DEFAULT 0, adx REAL DEFAULT 0, rsi REAL DEFAULT 0,
        e20 REAL DEFAULT 0, e50 REAL DEFAULT 0, e200 REAL DEFAULT 0,
        supertrend REAL DEFAULT 0, st_dir INTEGER DEFAULT 0,
        vwap REAL DEFAULT 0, poc REAL DEFAULT 0, va_low REAL DEFAULT 0, va_high REAL DEFAULT 0,
        vr REAL DEFAULT 0, atr_rk REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(symbol, date)
    );
    CREATE TABLE IF NOT EXISTS live_prices (
        symbol TEXT PRIMARY KEY,
        price REAL DEFAULT 0,
        prev_close REAL DEFAULT 0,
        day_change_pct REAL DEFAULT 0,
        volume INTEGER DEFAULT 0,
        avg_volume REAL DEFAULT 0,
        vol_ratio REAL DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS momentum_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, company TEXT, sector TEXT,
        alert_type TEXT,  -- 'BREAKOUT', 'SELLING', 'VOLUME_SPIKE', 'REVERSAL', 'MOMENTUM_SHIFT'
        direction TEXT,    -- 'LONG' or 'SHORT'
        price REAL, prev_price REAL, change_pct REAL,
        vol_ratio REAL, score INTEGER,
        entry_price REAL, sl REAL, t1 REAL, risk_per REAL,
        adx REAL, rsi REAL,
        acknowledged INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS ai_predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, sector TEXT, direction TEXT,
        current_price REAL, predicted_target REAL,
        confidence INTEGER, ai_score INTEGER,
        reasoning TEXT, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS backtest_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        params TEXT,
        summary TEXT,
        trades TEXT,
        equity TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS option_oi_snapshot (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT,
        index_name TEXT,
        tradingsymbol TEXT,
        expiry TEXT,
        strike REAL,
        opt_type TEXT,
        ltp REAL,
        oi INTEGER,
        volume INTEGER
    );
    CREATE TABLE IF NOT EXISTS option_oi_signal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT,
        index_name TEXT,
        tradingsymbol TEXT,
        expiry TEXT,
        strike REAL,
        opt_type TEXT,
        ltp REAL,
        oi INTEGER,
        doi INTEGER,
        doi_pct REAL,
        dltp_pct REAL,
        signal TEXT,
        score REAL,
        side TEXT,
        reason TEXT
    );
    CREATE TABLE IF NOT EXISTS whatsapp_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_key TEXT UNIQUE,
        kind TEXT,
        message TEXT,
        sent_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_signal_date ON signal_log(signal_date);
    CREATE INDEX IF NOT EXISTS idx_signal_symbol ON signal_log(symbol);
    CREATE INDEX IF NOT EXISTS idx_price_symbol ON price_cache(symbol);
    CREATE INDEX IF NOT EXISTS idx_alert_ack ON momentum_alerts(acknowledged);
    CREATE INDEX IF NOT EXISTS idx_opt_snap_ts ON option_oi_snapshot(ts);
    CREATE INDEX IF NOT EXISTS idx_opt_snap_sym ON option_oi_snapshot(tradingsymbol);
    CREATE INDEX IF NOT EXISTS idx_opt_sig_ts ON option_oi_signal(ts);
    CREATE INDEX IF NOT EXISTS idx_opt_sig_idx ON option_oi_signal(index_name);
    CREATE INDEX IF NOT EXISTS idx_wa_outbox_kind ON whatsapp_outbox(kind);
    """)

    try:
        conn.execute("""
            DELETE FROM signal_log
            WHERE id NOT IN (
                SELECT MAX(id) FROM signal_log GROUP BY signal_date, symbol, trade_type
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uniq_signal_day ON signal_log(signal_date, symbol, trade_type)")
    except Exception as e:
        print(f"[DB] Signal log dedupe/index warning: {e}")
    
    cursor = conn.execute("PRAGMA table_info(price_cache)")
    price_cols = [row[1] for row in cursor.fetchall()]
    if "st_dir" not in price_cols:
        print("[DB] Adding missing column 'st_dir' to price_cache")
        conn.execute("ALTER TABLE price_cache ADD COLUMN st_dir INTEGER DEFAULT 0")
    if "vwap" not in price_cols:
        print("[DB] Adding missing column 'vwap' to price_cache")
        conn.execute("ALTER TABLE price_cache ADD COLUMN vwap REAL DEFAULT 0")
    if "poc" not in price_cols:
        print("[DB] Adding missing column 'poc' to price_cache")
        conn.execute("ALTER TABLE price_cache ADD COLUMN poc REAL DEFAULT 0")
    if "va_low" not in price_cols:
        print("[DB] Adding missing column 'va_low' to price_cache")
        conn.execute("ALTER TABLE price_cache ADD COLUMN va_low REAL DEFAULT 0")
    if "va_high" not in price_cols:
        print("[DB] Adding missing column 'va_high' to price_cache")
        conn.execute("ALTER TABLE price_cache ADD COLUMN va_high REAL DEFAULT 0")
    if "vr" not in price_cols:
        print("[DB] Adding missing column 'vr' to price_cache")
        conn.execute("ALTER TABLE price_cache ADD COLUMN vr REAL DEFAULT 0")
    if "atr_rk" not in price_cols:
        print("[DB] Adding missing column 'atr_rk' to price_cache")
        conn.execute("ALTER TABLE price_cache ADD COLUMN atr_rk REAL DEFAULT 0")
    conn.commit(); conn.close()

init_db()

# ── INDEX: NIFTY50 WEIGHTS ──────────────────────────
NIFTY50_WEIGHTS = {
    "HDFCBANK": 11.54, "RELIANCE": 9.41, "ICICIBANK": 8.12, "INFY": 6.05, "ITC": 3.96,
    "BHARTIARTL": 4.13, "LT": 4.08, "TCS": 4.07, "AXISBANK": 2.90, "SBIN": 2.78,
    "KOTAKBANK": 2.46, "M&M": 2.17, "HINDUNILVR": 2.16, "TITAN": 1.76, "SUNPHARMA": 1.77,
    "BAJFINANCE": 1.98, "HCLTECH": 1.80, "TATAMOTORS": 1.53, "NTPC": 1.57, "ULTRACEMCO": 1.20,
    "MARUTI": 1.45, "POWERGRID": 1.25, "ASIANPAINT": 1.24, "GRASIM": 1.80, "TATASTEEL": 1.22,
    "ADANIPORTS": 0.99, "ONGC": 1.05, "COALINDIA": 0.89, "BEL": 0.94, "TECHM": 0.89,
    "HINDALCO": 0.91, "INDUSINDBK": 0.83, "JSWSTEEL": 1.18, "BAJAJ-AUTO": 1.17, "WIPRO": 0.88,
    "SHRIRAMFIN": 0.81, "ADANIENT": 0.95, "BAJAJFINSV": 1.24, "NESTLEIND": 0.99, "DRREDDY": 0.67,
    "CIPLA": 0.68, "APOLLOHOSP": 0.61, "EICHERMOT": 0.83, "HEROMOTOCO": 0.53, "TATACONSUM": 0.68,
    "BRITANNIA": 0.59, "SBILIFE": 0.70, "HDFCLIFE": 0.67, "TRENT": 1.46, "JIOFIN": 0.89
}

# ── DATABASE HELPERS ──────────────────────────

def upsert_price(symbol, date, open_p, high_p, low_p, close_p, volume, indicators):
    """Store/update price with indicators in cache."""
    conn = get_db()
    conn.execute("""
        INSERT INTO price_cache (symbol, date, open_price, high_price, low_price, close_price, volume, atr, adx, rsi, e20, e50, e200, supertrend, st_dir, vwap, poc, va_low, va_high, vr, atr_rk, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(symbol, date) DO UPDATE SET
            open_price=excluded.open_price, high_price=excluded.high_price,
            low_price=excluded.low_price, close_price=excluded.close_price,
            volume=excluded.volume, atr=excluded.atr, adx=excluded.adx,
            rsi=excluded.rsi, e20=excluded.e20, e50=excluded.e50,
            e200=excluded.e200, supertrend=excluded.supertrend,
            st_dir=excluded.st_dir, vwap=excluded.vwap, poc=excluded.poc,
            va_low=excluded.va_low, va_high=excluded.va_high, vr=excluded.vr,
            atr_rk=excluded.atr_rk,
            updated_at=CURRENT_TIMESTAMP
    """, (symbol, date, open_p, high_p, low_p, close_p, volume,
          indicators.get("atr", 0), indicators.get("adx", 0),
          indicators.get("rsi", 0), indicators.get("e20", 0),
          indicators.get("e50", 0), indicators.get("e200", 0),
          indicators.get("st", 0), indicators.get("st_dir", 0),
          indicators.get("vwap", 0), indicators.get("poc", 0),
          indicators.get("va_low", 0), indicators.get("va_high", 0),
          indicators.get("vr", 0), indicators.get("atr_rk", 0)))
    conn.commit(); conn.close()

def update_live_price(symbol, price, prev_close, volume, avg_vol):
    """Update live price in DB."""
    chg = round((price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0
    vr = round(volume / avg_vol, 2) if avg_vol > 0 else 1.0
    conn = get_db()
    conn.execute("""
        INSERT INTO live_prices (symbol, price, prev_close, day_change_pct, volume, avg_volume, vol_ratio, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(symbol) DO UPDATE SET
            price=excluded.price, prev_close=excluded.prev_close,
            day_change_pct=excluded.day_change_pct, volume=excluded.volume,
            avg_volume=excluded.avg_volume, vol_ratio=excluded.vol_ratio,
            updated_at=CURRENT_TIMESTAMP
    """, (symbol, price, prev_close, chg, volume, avg_vol, vr))
    conn.commit(); conn.close()

def get_cached_price(symbol):
    """Get latest cached price data from DB."""
    conn = get_db()
    row = conn.execute("SELECT * FROM live_prices WHERE symbol=?", (symbol,)).fetchone()
    conn.close()
    if row:
        return dict(row)
    return None

def get_all_live_prices_db():
    """Get all live prices from cache."""
    conn = get_db()
    rows = conn.execute("SELECT * FROM live_prices ORDER BY vol_ratio DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def store_momentum_alert(alert_data):
    """Store a momentum alert in DB."""
    conn = get_db()
    # Check if we already have a recent unacknowledged alert for this symbol
    existing = conn.execute(
        "SELECT id FROM momentum_alerts WHERE symbol=? AND acknowledged=0 AND created_at > datetime('now', '-5 minutes')",
        (alert_data["symbol"],)
    ).fetchone()
    if existing:
        conn.close()
        return  # Don't duplicate alerts within 5 minutes
    cur = conn.execute("""
        INSERT INTO momentum_alerts (symbol, company, sector, alert_type, direction, price, prev_price, change_pct, vol_ratio, score, entry_price, sl, t1, risk_per, adx, rsi)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (alert_data["symbol"], alert_data["company"], alert_data["sector"],
          alert_data["alert_type"], alert_data["direction"],
          alert_data["price"], alert_data["prev_price"],
          alert_data["change_pct"], alert_data["vol_ratio"],
          alert_data["score"], alert_data["entry_price"],
          alert_data["sl"], alert_data["t1"],
          alert_data["risk_per"], alert_data["adx"],
          alert_data["rsi"]))
    alert_id = cur.lastrowid
    conn.commit(); conn.close()
    try:
        _maybe_send_whatsapp_alert(alert_data, alert_id)
    except Exception as e:
        print(f"[WHATSAPP] alert notify error: {e}")

def get_unacknowledged_alerts():
    """Get all unacknowledged momentum alerts."""
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM momentum_alerts WHERE acknowledged=0 ORDER BY created_at DESC LIMIT 50"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def acknowledge_alert(alert_id):
    conn = get_db()
    conn.execute("UPDATE momentum_alerts SET acknowledged=1 WHERE id=?", (alert_id,))
    conn.commit(); conn.close()

def log_signal(signal_data):
    """Store a signal in the log with detailed entry info."""
    try:
        conn = get_db()
        conn.execute("""
            INSERT INTO signal_log (signal_date, symbol, sector, direction, score, trade_type,
                entry, sl, t1, t2, t3, adx, rsi, vol_ratio, filters, entry_time, atr, live_price, risk_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_date, symbol, trade_type) DO UPDATE SET
                sector=excluded.sector,
                direction=excluded.direction,
                score=excluded.score,
                entry=excluded.entry,
                sl=excluded.sl,
                t1=excluded.t1,
                t2=excluded.t2,
                t3=excluded.t3,
                adx=excluded.adx,
                rsi=excluded.rsi,
                vol_ratio=excluded.vol_ratio,
                filters=excluded.filters,
                entry_time=excluded.entry_time,
                atr=excluded.atr,
                live_price=excluded.live_price,
                risk_pct=excluded.risk_pct,
                logged_at=CURRENT_TIMESTAMP
        """, (signal_data["date"], signal_data["symbol"], signal_data["sector"],
              signal_data["direction"], signal_data["score"], signal_data.get("trade_type", "SWING"),
              signal_data["entry"], signal_data["sl"], signal_data["t1"], signal_data["t2"], signal_data["t3"],
              signal_data["adx"], signal_data["rsi"], signal_data["vol_ratio"],
              json.dumps(signal_data["filters"]), signal_data.get("entry_time", "09:20"),
              signal_data.get("atr", 0), signal_data.get("live_price", 0),
              signal_data.get("risk_pct", 0)))
        conn.commit()
        conn.close()
        print(f"[DB] Logged {signal_data.get('trade_type')} signal: {signal_data['symbol']}")
        try:
            _maybe_send_whatsapp_signal(signal_data)
        except Exception as e:
            print(f"[WHATSAPP] signal notify error: {e}")
    except Exception as e:
        print(f"[DB ERROR] log_signal: {e}")

def get_signal_log(limit=100):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM signal_log ORDER BY logged_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ── BACKGROUND SCANNER ──────────────────────────
_scanner_running = False
_scanner_thread = None
_options_oi_running = False
_options_oi_thread = None
_nfo_instruments_cache = {"ts": 0.0, "rows": None}
_whatsapp_event_cache = {}
_whatsapp_event_lock = threading.Lock()

def _store_sector_analysis(info, sym, direction, score, levels, live_price, risk_pct, trade_type, meta, ind):
    """Store stock analysis data for sector views and AI predictions."""
    try:
        conn = get_db()
        now = datetime.datetime.now().isoformat()
        
        # Calculate momentum score based on multiple factors
        adx = meta.get("adx", 0)
        rsi = meta.get("rsi", 50)
        vr = meta.get("vr", 1)
        atr = ind.get("atr", 0)
        
        # AI prediction score (0-100)
        ai_score = min(100, int(score * 11.1))  # score 9 = 100
        
        # Confidence based on ADX and volume
        confidence = min(95, int((adx / 30 * 40) + (vr / 3 * 40) + 20))
        
        # Momentum score
        momentum = int((adx / 30 * 35) + (rsi / 100 * 25) + (vr / 2 * 25) + (score / 9 * 15))
        
        conn.execute("""
            INSERT OR REPLACE INTO sector_analysis 
            (sector, symbol, company, direction, score, live_price, entry_price, sl, t1, t2, t3,
             adx, rsi, vol_ratio, risk_pct, risk_per, atr, trade_type,
             e20, e50, e200, supertrend, st_dir, vwap, poc, va_low, va_high,
             index_tag, index_weight,
             ai_prediction, ai_confidence, momentum_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            info[3], sym, info[1], direction, score, live_price, levels["entry"],
            levels["sl"], levels["t1"], levels["t2"], levels.get("t3", 0),
            adx, rsi, vr, risk_pct, levels.get("risk_per", 0), atr, trade_type,
            ind.get("e20") or 0, ind.get("e50") or 0, ind.get("e200") or 0,
            ind.get("st") or 0, ind.get("st_dir") or 0,
            ind.get("vwap") or 0, ind.get("poc") or 0,
            ind.get("va_low") or 0, ind.get("va_high") or 0,
            ("NIFTY50" if sym in NIFTY50_WEIGHTS else ""), NIFTY50_WEIGHTS.get(sym, 0),
            levels.get("t2", 0), confidence, momentum, now
        ))
        
        conn.commit()
        conn.close()
        print(f"[STORE] {sym} -> sector_analysis (score={score}, momentum={momentum})")
    except Exception as e:
        print(f"[ERROR] _store_sector_analysis: {e}")

def _generate_ai_predictions():
    """Generate AI predictions for next movers based on stored analysis."""
    try:
        conn = get_db()
        now = datetime.datetime.now().isoformat()
        
        # Get top stocks by momentum score
        cur = conn.execute("""
            SELECT symbol, sector, direction, live_price, t2, ai_confidence, momentum_score, 
                   adx, rsi, vol_ratio, score
            FROM sector_analysis 
            WHERE score >= 6
            ORDER BY momentum_score DESC LIMIT 20
        """)
        
        stocks = cur.fetchall()
        
        for s in stocks:
            sym, sector, direction, current, target, conf, momentum, adx, rsi, vr, score = s
            
            # AI reasoning based on indicators
            reasoning_parts = []
            if adx >= 25:
                reasoning_parts.append(f"Strong trend (ADX:{adx})")
            if rsi < 30:
                reasoning_parts.append("Oversold - potential bounce")
            elif rsi > 70:
                reasoning_parts.append("Overbought - caution")
            if vr >= 2:
                reasoning_parts.append(f"High volume ({vr}x)")
            if score >= 8:
                reasoning_parts.append(f"High score ({score}/9)")
            
            reasoning = "; ".join(reasoning_parts) if reasoning_parts else "Analyzing..."
            
            # Predicted target (T2 from levels)
            predicted_target = target if target else current * 1.05
            
            conn.execute("""
                INSERT INTO ai_predictions 
                (symbol, sector, direction, current_price, predicted_target, confidence, ai_score, reasoning, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (sym, sector, direction, current, predicted_target, conf, momentum, reasoning, now))
        
        conn.commit()
        conn.close()
        print(f"[AI] Generated predictions for {len(stocks)} stocks")
    except Exception as e:
        pass

def _start_background_scanner():
    """Start the background scanner thread."""
    global _scanner_running
    if _scanner_running:
        return
    _scanner_running = True
    t = threading.Thread(target=_background_scanner_loop, daemon=True)
    t.start()
    print("[SCANNER] Background scanner started")

def _start_options_oi_scanner():
    global _options_oi_running, _options_oi_thread
    if _options_oi_running:
        return
    _options_oi_running = True
    _options_oi_thread = threading.Thread(target=_options_oi_loop, daemon=True)
    _options_oi_thread.start()
    print("[OI] Options OI scanner started")

def _background_scanner_loop():
    """Continuously scan for momentum changes every 60 seconds."""
    global _scanner_running, _zerodha_ready
    interval = 60  # seconds
    last_full_scan = 0
    
    # Wait for Zerodha to be ready if enabled
    cfg = gcfg()
    if cfg.get("use_zerodha_ltp", True):
        max_wait = 30
        while not _zerodha_ready and max_wait > 0:
            time.sleep(1)
            max_wait -= 1
    
    while _scanner_running:
        try:
            now = time.time()
            
            # Every 5 minutes, do a full universe scan + cache prices
            if now - last_full_scan >= 300:
                print(f"[BG-SCAN] Full scan at {datetime.datetime.now().strftime('%H:%M:%S')}...")
                _full_universe_scan()
                _generate_ai_predictions()  # Generate AI predictions
                last_full_scan = now
            
            # Every cycle, check for momentum alerts using cached data
            _check_momentum_alerts()
            
        except Exception as e:
            print(f"[BG-SCAN] Error: {e}")
        
        time.sleep(interval)

def _full_universe_scan():
    """Full scan of all 207 stocks, cache prices + indicators."""
    from engine import get_ohlcv, compute_indicators, score_candle, compute_levels, _train_models, _ml_trained
    cfg = gcfg()
    use_real = cfg.get("use_real", True)
    
    # Use the global get_zerodha_live_prices function
    live_prices = get_zerodha_live_prices([u[0] for u in UNIVERSE])
    
    _ml_data_cache = {}
    scanned_count = 0
    
    for info in UNIVERSE:
        sym = info[0]
        try:
            rows = get_ohlcv(info, months=2, use_real=use_real)
            if not rows or len(rows) < 30:
                continue
            
            inds = compute_indicators(rows)
            last = rows[-1]
            ind = inds[-1]
            
            _ml_data_cache[sym] = rows
            scanned_count += 1
            
            # Cache price data
            upsert_price(sym, last["date"], last["open"], last["high"],
                        last["low"], last["close"], last["volume"], ind)
            
            # Cache live price
            lp = live_prices.get(sym, last["close"])
            vol_window = [r["volume"] for r in rows[-21:-1]] if len(rows) >= 21 else [r["volume"] for r in rows[:-1]]
            avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1
            update_live_price(sym, lp, last["close"], last["volume"], avg_vol)
            
            # Score and log signal (Accuracy focus: Score >= 7)
            sc, direction, fl, meta = score_candle(last, ind, rows)
            if sc >= 7:
                trade_type = "SWING" if meta.get("adx", 0) >= 22 else "INTRA"
                lv = compute_levels(lp, ind.get("atr", last["close"] * 0.015), direction, trade_type)
                risk_pct = round(lv["risk_per"] / lp * 100, 2) if lp > 0 else 0
                
                signal_data = {
                    "date": last["date"], "symbol": sym, "sector": info[3],
                    "direction": direction, "score": sc,
                    "trade_type": trade_type,
                    "entry": lv["entry"], "sl": lv["sl"],
                    "t1": lv["t1"], "t2": lv["t2"], "t3": lv["t3"],
                    "adx": meta.get("adx", 0), "rsi": meta.get("rsi", 50),
                    "vol_ratio": meta.get("vr", 1),
                    "filters": fl,
                    "entry_time": "09:20",
                    "atr": ind.get("atr", 0),
                    "live_price": lp,
                    "risk_pct": risk_pct,
                }
                log_signal(signal_data)
                print(f"[SIGNAL] HIGH ACCURACY: {sym} ({direction}) Score: {sc}")
                
        except Exception as e:
            continue
    
    # Store ALL scanned stocks in sector_analysis (not just score >= 5)
    for sym, data in _ml_data_cache.items():
        try:
            # Get stock info
            info = next((x for x in UNIVERSE if x[0] == sym), None)
            if not info:
                continue
            
            # Get live price
            lp = live_prices.get(sym, 0)
            if lp <= 0:
                continue
            
            # Get latest row and indicators
            rows = data
            if not rows or len(rows) < 30:
                continue
            last = rows[-1]
            ind = compute_indicators(rows)[-1]
            
            sc, direction, fl, meta = score_candle(last, ind, rows)
            trade_type = meta.get("trend_quality", "SWING")
            lv = compute_levels(lp, ind.get("atr", last["close"] * 0.015), direction, trade_type)
            risk_pct = round(lv["risk_per"] / lp * 100, 2) if lp > 0 else 0
            
            # Store analysis for sector views
            _store_sector_analysis(info, sym, direction, sc, lv, lp, risk_pct, trade_type, meta, ind)
        except:
            continue
    
    if scanned_count > 0:
        # Save to DB immediately after each full scan to ensure persistence
        print(f"[DB] Saving {scanned_count} analyzed stocks to persistent cache...")
        # (This is already handled by _store_sector_analysis inside the loop)
        
        print(f"[ML] Training models on {scanned_count} symbols with latest market data...")
        _train_models(_ml_data_cache)

def _get_nfo_instruments():
    now = time.time()
    if _nfo_instruments_cache["rows"] is not None and (now - _nfo_instruments_cache["ts"]) < 3600:
        return _nfo_instruments_cache["rows"]
    kite = _get_zerodha_kite()
    if not kite:
        return None
    rows = kite.instruments("NFO")
    _nfo_instruments_cache["rows"] = rows
    _nfo_instruments_cache["ts"] = now
    return rows

def _nearest_expiry(instruments, name, inst_type):
    today = datetime.datetime.now().date()
    expiries = sorted({i.get("expiry") for i in instruments if i.get("name") == name and i.get("instrument_type") == inst_type and i.get("expiry")})
    for e in expiries:
        if hasattr(e, "date"):
            e_date = e.date()
        else:
            e_date = e
        if e_date >= today:
            return e_date
    return expiries[0].date() if expiries else None

def _pick_index_option_contracts(index_name, strikes_each_side=6):
    instruments = _get_nfo_instruments()
    if not instruments:
        return None

    expiry = _nearest_expiry(instruments, index_name, "FUT")
    opt_expiry = _nearest_expiry(instruments, index_name, "CE") or _nearest_expiry(instruments, index_name, "PE")
    if not opt_expiry:
        return None

    fut = None
    for i in instruments:
        if i.get("name") == index_name and i.get("instrument_type") == "FUT":
            e = i.get("expiry")
            e_date = e.date() if hasattr(e, "date") else e
            if e_date == expiry:
                fut = i
                break

    kite = _get_zerodha_kite()
    if not kite or not fut:
        return None

    fut_key = f"NFO:{fut.get('tradingsymbol')}"
    q = kite.quote([fut_key])
    fut_ltp = float(q.get(fut_key, {}).get("last_price") or 0)
    if fut_ltp <= 0:
        return None

    step = 50 if index_name == "NIFTY" else 100
    atm = int(round(fut_ltp / step) * step)

    candidates = [i for i in instruments if i.get("name") == index_name and i.get("expiry") and (i.get("instrument_type") in ("CE", "PE"))]
    out = []
    for i in candidates:
        e = i.get("expiry")
        e_date = e.date() if hasattr(e, "date") else e
        if e_date != opt_expiry:
            continue
        strike = int(i.get("strike") or 0)
        if strike <= 0:
            continue
        if abs(strike - atm) <= (strikes_each_side * step):
            out.append({
                "tradingsymbol": i.get("tradingsymbol"),
                "strike": strike,
                "opt_type": i.get("instrument_type"),
                "expiry": str(opt_expiry),
            })

    return {"index": index_name, "expiry": str(opt_expiry), "atm": atm, "contracts": out, "fut_ltp": fut_ltp}

def _scan_index_options_once(index_name):
    meta = _pick_index_option_contracts(index_name=index_name)
    if not meta or not meta.get("contracts"):
        return {"index": index_name, "signals": [], "top": None, "note": "No option contracts found"}

    kite = _get_zerodha_kite()
    if not kite:
        return {"index": index_name, "signals": [], "top": None, "note": "Kite not available"}

    keys = [f"NFO:{c['tradingsymbol']}" for c in meta["contracts"]]
    quotes = kite.quote(keys)

    now_iso = datetime.datetime.now().isoformat()
    conn = get_db()

    signals = []

    for c in meta["contracts"]:
        k = f"NFO:{c['tradingsymbol']}"
        d = quotes.get(k, {})
        ltp = float(d.get("last_price") or 0)
        oi = int(d.get("oi") or 0)
        vol = int(d.get("volume") or 0)
        if ltp <= 0 or oi <= 0:
            continue

        prev = conn.execute(
            "SELECT ltp, oi, ts FROM option_oi_snapshot WHERE tradingsymbol=? ORDER BY id DESC LIMIT 1",
            (c["tradingsymbol"],),
        ).fetchone()

        try:
            conn.execute(
                "INSERT INTO option_oi_snapshot (ts, index_name, tradingsymbol, expiry, strike, opt_type, ltp, oi, volume) VALUES (?,?,?,?,?,?,?,?,?)",
                (now_iso, index_name, c["tradingsymbol"], c["expiry"], c["strike"], c["opt_type"], ltp, oi, vol),
            )
        except Exception:
            pass

        if not prev:
            continue

        prev_ltp = float(prev["ltp"] or 0)
        prev_oi = int(prev["oi"] or 0)
        if prev_ltp <= 0 or prev_oi <= 0:
            continue

        doi = oi - prev_oi
        doi_pct = (doi / prev_oi) * 100.0
        dltp_pct = ((ltp - prev_ltp) / prev_ltp) * 100.0

        if abs(doi_pct) < 12 or abs(dltp_pct) < 4:
            continue

        if doi_pct > 0 and dltp_pct > 0:
            signal = "HERO"
        elif doi_pct > 0 and dltp_pct < 0:
            signal = "ZERO"
        else:
            continue

        side = "BULLISH" if c["opt_type"] == "CE" else "BEARISH"
        score = round(abs(doi_pct) * 1.4 + abs(dltp_pct) * 3.0 + (math.log(vol + 1) * 2.0), 1)
        reason = f"ΔOI {doi_pct:.1f}% | ΔP {dltp_pct:.1f}% | Vol {vol}"

        rec = {
            "ts": now_iso,
            "index": index_name,
            "tradingsymbol": c["tradingsymbol"],
            "expiry": c["expiry"],
            "strike": c["strike"],
            "opt_type": c["opt_type"],
            "ltp": ltp,
            "oi": oi,
            "doi": doi,
            "doi_pct": round(doi_pct, 2),
            "dltp_pct": round(dltp_pct, 2),
            "signal": signal,
            "score": score,
            "side": side,
            "action": f"BUY {c['opt_type']}" if signal == "HERO" else "AVOID",
            "reason": reason,
        }

        try:
            conn.execute(
                "INSERT INTO option_oi_signal (ts,index_name,tradingsymbol,expiry,strike,opt_type,ltp,oi,doi,doi_pct,dltp_pct,signal,score,side,reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (now_iso, index_name, c["tradingsymbol"], c["expiry"], c["strike"], c["opt_type"], ltp, oi, doi, doi_pct, dltp_pct, signal, score, side, reason),
            )
        except Exception:
            pass
        signals.append(rec)

    conn.commit()
    conn.close()

    signals.sort(key=lambda x: x["score"], reverse=True)
    top = None
    for s in signals:
        if s["signal"] == "HERO":
            top = s
            break
    if top is None and signals:
        top = signals[0]

    # Build summary across CE/PE for momentum direction
    ce_oi = 0; pe_oi = 0; ce_doi = 0; pe_doi = 0
    for s in signals:
        if s["opt_type"] == "CE":
            ce_oi += int(s.get("oi") or 0)
            ce_doi += int(s.get("doi") or 0)
        else:
            pe_oi += int(s.get("oi") or 0)
            pe_doi += int(s.get("doi") or 0)
    oi_imbalance = (ce_oi - pe_oi)
    doi_imbalance = (ce_doi - pe_doi)
    direction = "BULLISH" if (oi_imbalance > 0 and doi_imbalance >= 0) or (doi_imbalance > 0) else "BEARISH"
    summary = {
        "ce_total_oi": ce_oi, "pe_total_oi": pe_oi,
        "ce_total_doi": ce_doi, "pe_total_doi": pe_doi,
        "oi_imbalance": oi_imbalance, "doi_imbalance": doi_imbalance,
        "momentum": direction
    }

    return {
        "index": index_name,
        "signals": signals[:20],
        "top": top,
        "meta": {"expiry": meta["expiry"], "atm": meta["atm"], "fut_ltp": meta["fut_ltp"]},
        "summary": summary
    }

def _options_oi_loop():
    global _options_oi_running, _zerodha_ready
    interval = 60
    max_wait = 30
    while not _zerodha_ready and max_wait > 0:
        time.sleep(1)
        max_wait -= 1
    while _options_oi_running:
        try:
            _scan_index_options_once("NIFTY")
            _scan_index_options_once("BANKNIFTY")
        except Exception as e:
            print(f"[OI] Options scanner error: {e}")
        time.sleep(interval)

def _check_momentum_alerts():
    """Check for sudden momentum changes using cached data."""
    from engine import compute_indicators, score_candle, compute_levels
    cfg = gcfg()
    use_real = cfg.get("use_real", True)
    
    # Use cached live prices
    cached_prices = get_all_live_prices_db()
    if not cached_prices:
        return
    
    # Get a few stocks with highest vol_ratio for alert check
    top_spikes = sorted(cached_prices, key=lambda x: -x.get("vol_ratio", 0))[:30]
    
    for row in top_spikes:
        sym = row["symbol"]
        live_price = row.get("price", 0)
        if live_price <= 0:
            continue
        
        # Find company name
        company = sym
        sector = ""
        for info in UNIVERSE:
            if info[0] == sym:
                company = info[2]
                sector = info[3]
                break
        
        try:
            # Get recent data for scoring
            for info in UNIVERSE:
                if info[0] == sym:
                    rows = get_ohlcv(info, months=1, use_real=use_real)
                    if not rows or len(rows) < 10:
                        continue
                    
                    inds = compute_indicators(rows)
                    last = rows[-1]
                    ind = inds[-1]
                    
                    sc, direction, fl, meta = score_candle(last, ind, rows)
                    adx = meta.get("adx", 0)
                    rsi_val = meta.get("rsi", 50)
                    vr = meta.get("vr", 1)
                    chg_pct = row.get("day_change_pct", 0)
                    
                    # Determine alert type
                    alert_type = None
                    if adx >= 28 and vr >= 2.0 and abs(chg_pct) >= 1.0:
                        alert_type = "BREAKOUT"
                    elif chg_pct <= -2.0 and vr >= 1.8:
                        alert_type = "SELLING"
                    elif vr >= 2.5 and sc >= 6:
                        alert_type = "VOLUME_SPIKE"
                    elif (rsi_val <= 30 or rsi_val >= 70) and vr >= 1.5:
                        alert_type = "REVERSAL"
                    elif adx >= 25 and abs(chg_pct) >= 1.5 and sc >= 6:
                        alert_type = "MOMENTUM_SHIFT"
                    
                    if alert_type and sc >= 5:
                        trade_type = "SWING" if adx >= 22 else "INTRA"
                        lv = compute_levels(live_price, ind.get("atr", live_price * 0.015), direction, trade_type)
                        
                        alert_data = {
                            "symbol": sym, "company": company, "sector": sector,
                            "alert_type": alert_type, "direction": direction,
                            "price": live_price, "prev_price": row.get("prev_close", live_price),
                            "change_pct": chg_pct, "vol_ratio": vr,
                            "score": sc,
                            "entry_price": lv["entry"], "sl": lv["sl"],
                            "t1": lv["t1"], "risk_per": lv["risk_per"],
                            "adx": adx, "rsi": rsi_val,
                        }
                        store_momentum_alert(alert_data)
                    break
        except Exception as e:
            continue
    
    unack = get_unacknowledged_alerts()
    if unack:
        print(f"[ALERTS] {len(unack)} unacknowledged momentum alerts")

# ── CONFIG ───────────────────────────────
# Default: use REAL data from Zerodha
DEFAULTS = {
    "capital": 100000,
    "risk_pct": 1.5,
    "max_positions": 2,
    "use_real": True,
    # If Zerodha credentials exist, enable true LTP streaming.
    # Uses Zerodha API for both historical and live data.
    "use_zerodha_ltp": True,
}

def _load_simple_env_file():
    """
    Loads KEY=VALUE pairs from ../.env.txt (if present) into os.environ.
    This is intentionally minimal to avoid extra dependencies.
    """
    root = os.path.join(BASE, "..")
    env_path = os.path.join(root, ".env")
    fallback_env_path = os.path.join(root, ".env.txt")
    print(f"[ENV] Looking for .env in: {env_path}")
    print(f"[ENV] .env exists: {os.path.exists(env_path)}")
    if not os.path.exists(env_path):
        env_path = fallback_env_path
    
    # ALWAYS load from .env.txt if it exists, as it has the freshest token
    if os.path.exists(fallback_env_path):
        env_path = fallback_env_path
        
    print(f"[ENV] Loading from: {env_path}")
    try:
        with open(env_path, "r", encoding="utf-8-sig") as f: # Use utf-8-sig to handle BOM
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip()
                if k:
                    # Force overwrite with .env.txt values
                    os.environ[k] = v
    except Exception as e:
        print(f"[ENV] Failed to load .env.txt: {e}")
    print(f"[ENV] KITE_ACCESS_TOKEN = {os.environ.get('KITE_ACCESS_TOKEN', 'NOT SET')}")
    print(f"[ENV] KITE_API_KEY = {os.environ.get('KITE_API_KEY', 'NOT SET')}")

_load_simple_env_file()

def gcfg():
    try:
        if os.path.exists(CFG):
            with open(CFG) as f:
                cfg = {**DEFAULTS, **json.load(f)}
                cfg["use_real"] = True
                cfg["use_zerodha_ltp"] = True
                return cfg
    except Exception:
        pass
    cfg = DEFAULTS.copy()
    cfg["use_real"] = True
    cfg["use_zerodha_ltp"] = True
    return cfg

def scfg(d):
    os.makedirs(os.path.dirname(CFG), exist_ok=True)
    with open(CFG, "w") as f:
        json.dump(d, f, indent=2)


_zerodha_ready = False

# def _start_background_scanner():  # Redundant definition removed

# ── DATA HELPERS ─────────────────────────
_cache_time = {}  # sym -> timestamp

# ── REAL-TIME LTP (Zerodha streaming) ─────────────────────────────
_ltp_cache = {}  # symbol -> {"ltp": float, "ts": unix_seconds, "fresh": bool}
_ltp_lock = threading.Lock()
_ltp_stream_thread_started = False
_zerodha_ready = False

def _maybe_start_zerodha_ltp_stream(run_in_main_thread=False):
    global _ltp_stream_thread_started, _zerodha_ready
    if _ltp_stream_thread_started:
        return

    cfg = gcfg()
    if not cfg.get("use_zerodha_ltp", True):
        return

    api_key = os.environ.get("KITE_API_KEY", "").strip()
    access_token = os.environ.get("KITE_ACCESS_TOKEN", "").strip()
    
    # Debug print
    print(f"[ZERODHA] Initializing with key={api_key[:5]}... and token={access_token[:5]}...")
    
    # KITE_API_SECRET is not strictly required for streaming with access_token,
    # but we load it anyway for completeness.
    api_secret = os.environ.get("KITE_API_SECRET", "").strip()
    if not api_key or not access_token:
        return

    _zerodha_ready = False
    try:
        from kiteconnect import KiteConnect, KiteTicker
    except Exception as e:
        print(f"[ZERODHA] kiteconnect not installed/available: {e}", flush=True)
        return

    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        print(f"[ZERODHA] Manual check with key={api_key[:5]} and token={access_token[:5]}...")
        profile = kite.profile()
        print(f"[ZERODHA] Auth SUCCESS! User: {profile.get('user_name')}")
        _zerodha_ready = True
    except Exception as e:
        print(f"[ZERODHA] Auth FAILED: {e}")
        # If manual check fails, don't even try the worker to avoid spamming
        # But we'll let it continue for now to see if it somehow works in the worker

    _ltp_stream_thread_started = True
    
    # Start background scanner only after Zerodha is confirmed ready
    _start_background_scanner()
    _start_options_oi_scanner()

    def _worker():
        nonlocal api_key, access_token
        try:
            from kiteconnect import KiteConnect, KiteTicker
        except Exception as e:
            print(f"[ZERODHA] kiteconnect not installed/available: {e}", flush=True)
            return

        try:
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
        except Exception as e:
            print(f"[ZERODHA] Failed to init KiteConnect: {e}", flush=True)
            return

        print("[ZERODHA] Worker started", flush=True)

        # Validate auth early so we don't spam reconnect attempts.
        try:
            print("[ZERODHA] Validating credentials (REST profile)...", flush=True)
            _ = kite.profile()
        except Exception as e:
            global _zerodha_ready
            _zerodha_ready = False
            print(f"[ZERODHA] Auth failed (fix KITE_ACCESS_TOKEN): {e}", flush=True)
            # Keep backend alive; Flask is running in another thread.
            while True:
                time.sleep(30)
            return

        # Build token map for our UNIVERSE symbols.
        token_to_sym = {}
        try:
            print("[ZERODHA] Fetching instrument master for NSE...", flush=True)
            instruments = kite.instruments("NSE")
            inst_by_ts = {i.get("tradingsymbol"): i for i in instruments}

            # Some symbols (like M&M) may be URL-encoded in Zerodha instruments.
            from urllib.parse import quote
            for sym in [u[0] for u in UNIVERSE]:
                candidates = {sym, sym.replace("&", "%26")}
                try:
                    candidates.add(quote(sym, safe=""))
                except Exception:
                    pass

                found = None
                for c in candidates:
                    if c in inst_by_ts:
                        found = inst_by_ts[c]
                        break
                if found is None:
                    continue
                token_to_sym[int(found["instrument_token"])] = sym
        except Exception as e:
            print(f"[ZERODHA] Failed building instrument map: {e}", flush=True)
            return

        tokens = list(token_to_sym.keys())
        if not tokens:
            print("[ZERODHA] No instrument tokens resolved; skipping stream start.", flush=True)
            return

        # Mark stream as ready only after we have a token map.
        _zerodha_ready = True

        print(f"[ZERODHA] Streaming LTP for {len(tokens)} NSE tokens...", flush=True)

        def on_connect(ws, response):
            try:
                ws.subscribe(tokens)
                ws.set_mode(ws.MODE_LTP, tokens)
            except Exception as e:
                print(f"[ZERODHA] on_connect subscribe error: {e}")

        def on_ticks(ws, ticks):
            now = time.time()
            with _ltp_lock:
                for t in ticks:
                    try:
                        token = int(t.get("instrument_token"))
                        sym = token_to_sym.get(token)
                        if not sym:
                            continue
                        ltp = t.get("last_price")
                        if ltp is None:
                            continue
                        ltp = float(ltp)
                        if ltp <= 0:
                            continue
                        _ltp_cache[sym] = {"ltp": ltp, "ts": now, "fresh": True}
                    except Exception:
                        continue

        def on_close(ws, code, reason):
            print(f"[ZERODHA] WebSocket closed: code={code} reason={reason}")

        def on_error(ws, code, reason):
            print(f"[ZERODHA] WebSocket error: code={code} reason={reason}")

        # Reconnect loop: kite ticker "connect" may return on disconnect.
        while True:
            try:
                kws = KiteTicker(api_key, access_token)
                kws.on_ticks = on_ticks
                kws.on_connect = on_connect
                kws.on_close = on_close
                kws.on_error = on_error
                kws.connect(threaded=False)
            except Exception as e:
                print(f"[ZERODHA] Stream exception: {e}")
            # Backoff before retry
            time.sleep(3)

    if run_in_main_thread:
        # IMPORTANT: kiteconnect may use Twisted and tries to install signal handlers.
        # That breaks when executed in a non-main thread on Windows.
        _worker()
    else:
        t = threading.Thread(target=_worker, daemon=True)
        t.start()

def _zerodha_stream_active():
    # Stream is considered "available" if creds were present and the stream thread started.
    return _zerodha_ready

def _fresh_ltps(now_ts, max_age_sec=2.5):
    """
    Returns a snapshot list of all symbols with prices (freshness optional).
    SSE caller will throttle by itself.
    """
    updates = []
    with _ltp_lock:
        for sym, v in _ltp_cache.items():
            ltp = v.get("ltp")
            ts = v.get("ts")
            if ltp is None or ts is None:
                continue
            fresh = (now_ts - float(ts)) <= max_age_sec
            updates.append({"symbol": sym, "ltp": float(ltp), "ts": ts, "fresh": fresh})
    return updates

# Start Zerodha streaming later (inside __main__) to keep Twisted happy.

def get_stock(sym, use_real=None, force_refresh=False):
    cfg = gcfg()
    if use_real is None:
        use_real = True
    
    # Cache for 5 minutes if using real data, always refresh if force_refresh
    import time
    now = time.time()
    cache_ttl = 300  # 5 minutes
    if sym in _cache and not force_refresh:
        if use_real and sym in _cache_time and (now - _cache_time[sym]) < cache_ttl:
            return _cache[sym]
        elif not use_real:
            return _cache[sym]
    
    info = next((x for x in UNIVERSE if x[0] == sym), None)
    if not info:
        print(f"[WARN] Symbol not found: {sym}")
        return None, None
    
    print(f"[DATA] Fetching REAL data for {sym}...")
    rows = get_ohlcv(info, months=9, use_real=True)
    if not rows or len(rows) < 30:
        print(f"[WARN] No Zerodha data for {sym}: got {len(rows) if rows else 0} rows")
        return None, None
    try:
        inds = compute_indicators(rows)
    except Exception as e:
        print(f"[ERROR] Indicators failed for {sym}: {e}")
        return None, None
    _cache[sym] = (rows, inds)
    _cache_time[sym] = now
    print(f"[DATA] Got {len(rows)} rows for {sym}, latest: {rows[-1]['date'] if rows else 'N/A'}")
    return rows, inds

def clear_cache():
    global _cache, _cache_time
    _cache = {}
    _cache_time = {}
    print("[CACHE] Cleared")

def make_signal(sym, info, rows, inds, idx, sc, dr, fl, meta, entry_time=None):
    rnd = random.Random(hash(sym + rows[idx]["date"]))
    if entry_time is None:
        entry_time = rnd.choice(ENTRY_TIMES_OPEN if rnd.random() < 0.65 else ENTRY_TIMES_POWER)
    lv = compute_levels(rows[idx]["close"], inds[idx]["atr"], dr)
    
    # Determine swing vs intra suitability (enhanced)
    adx = meta.get("adx", 0)
    rsi = meta.get("rsi", 50)
    vr = meta.get("vr", 1)
    trend_q = meta.get("trend_quality", "WEAK")
    entry_q = meta.get("entry_quality", "NORMAL")
    
    # SWING: Strong trend (ADX >= 22) + moderate momentum (RSI 35-70) + good trend quality
    # INTRA: Weak trend (ADX < 18) + extreme RSI (>75 or <25)
    if adx >= 22 and 35 <= rsi <= 70 and trend_q in ["STRONG", "MODERATE"]:
        recommended = "SWING"
        confidence = "HIGH"
    elif adx >= 20 and 30 <= rsi <= 75 and trend_q != "WEAK":
        recommended = "SWING"
        confidence = "MEDIUM"
    elif adx < 15 or rsi > 80 or rsi < 20:
        recommended = "INTRA"
        confidence = "HIGH"
    elif adx >= 18 and (rsi >= 70 or rsi <= 30):
        recommended = "INTRA"
        confidence = "MEDIUM"
    elif entry_q == "IDEAL" and adx >= 18:
        recommended = "SWING"
        confidence = "HIGH"
    else:
        recommended = "SWING"  # Default to swing for more opportunities
        confidence = "MEDIUM"
    
    # Price change analysis (with safety checks)
    current_price = rows[idx]["close"]
    prev_close = rows[idx-1]["close"] if idx > 0 and rows[idx-1]["close"] > 0 else current_price
    day_change = ((current_price - prev_close) / prev_close * 100) if prev_close > 0 else 0
    
    # 52-week analysis (using last 60 days as proxy) - with safety
    recent = rows[max(0,idx-60):idx+1]
    if recent:
        high_60 = max(r["high"] for r in recent)
        low_60 = min(r["low"] for r in recent)
        pct_from_high = ((high_60 - current_price) / high_60 * 100) if high_60 > 0 else 0
        pct_from_low = ((current_price - low_60) / low_60 * 100) if low_60 > 0 else 0
    else:
        high_60 = low_60 = current_price
        pct_from_high = pct_from_low = 0
    
    # Volume analysis (with safety)
    vol_today = rows[idx]["volume"]
    vol_window = rows[max(0,idx-20):idx]
    vol_avg = sum(r["volume"] for r in vol_window) / len(vol_window) if vol_window else 1
    vol_spike = vol_today / vol_avg if vol_avg > 0 else 1
    
    return {
        "date":           rows[idx]["date"],
        "symbol":         sym,
        "company":        info[2],
        "sector":         info[3],
        "direction":      dr,
        "score":          sc,
        "filters":        fl,
        
        # Price data
        "entry":          lv["entry"],
        "current":       current_price,
        "prev_close":    prev_close,
        "day_change_pct": round(day_change, 2),
        "high":           rows[idx]["high"],
        "low":            rows[idx]["low"],
        "sl":             lv["sl"],
        "t1":             lv["t1"],
        "t2":             lv["t2"],
        "t3":             lv["t3"],
        "risk_per":       lv["risk_per"],
        "atr":            lv["atr"],
        
        # Current state
        "close":          current_price,
        "volume":         rows[idx]["volume"],
        
        # Indicators
        "adx":            meta["adx"],
        "rsi":            meta["rsi"],
        "vol_ratio":      meta["vr"],
        "vol_spike":      round(vol_spike, 2),
        "st":             meta["st"],
        
        # Technical levels
        "entry_time":     entry_time,
        "entry_datetime": f"{rows[idx]['date']} {entry_time} IST",
        "bb_up":          inds[idx]["bb_up"],
        "bb_lo":          inds[idx]["bb_lo"],
        "supertrend":     inds[idx]["st"],
        "e20":            inds[idx]["e20"],
        "e50":            inds[idx]["e50"],
        "e200":           inds[idx]["e200"],
        
        # Recommendations
        "recommended":    recommended,
        "confidence":     confidence,
        
        # Position analysis
        "pct_from_high": round(pct_from_high, 1),
        "pct_from_low":  round(pct_from_low, 1),
        "high_60d":      round(high_60, 2),
        "low_60d":       round(low_60, 2),
        
        # Quick entry flags
        "is_breakout":    adx >= 28 and vr >= 1.5,
        "is_volume_spike": vol_spike >= 2.0,
        "is_reversal":    rsi > 65 or rsi < 35,
        "is_momentum":   adx >= 25 and sc >= 6,
    }

# ── CORS ─────────────────────────────────
@app.route("/api/signals/intra")
def get_intra_signals():
    """Get high-accuracy intraday signals from DB."""
    conn = get_db()
    signals = [dict(r) for r in conn.execute(
        "SELECT * FROM signal_log WHERE trade_type = 'INTRA' AND score >= 7 ORDER BY logged_at DESC LIMIT 50"
    ).fetchall()]
    
    current = [dict(r) for r in conn.execute("""
        SELECT symbol, company, sector, direction, score, live_price, entry_price as entry, 
               sl, t1, t2, t3, adx, rsi, vol_ratio, risk_pct, risk_per, atr, trade_type as recommended
        FROM sector_analysis 
        WHERE trade_type = 'INTRA' AND score >= 7
        ORDER BY score DESC, momentum_score DESC
    """).fetchall()]
    
    conn.close()
    return jsonify({"signals": signals, "current": current})

@app.route("/api/signals/swing")
def get_swing_signals():
    """Get high-accuracy swing signals from DB."""
    conn = get_db()
    # Return both signal_log history and current scanner analysis for swing
    signals = [dict(r) for r in conn.execute(
        "SELECT * FROM signal_log WHERE trade_type = 'SWING' AND score >= 7 ORDER BY logged_at DESC LIMIT 50"
    ).fetchall()]
    
    current = [dict(r) for r in conn.execute("""
        SELECT symbol, company, sector, direction, score, live_price, entry_price as entry, 
               sl, t1, t2, t3, adx, rsi, vol_ratio, risk_pct, risk_per, atr, trade_type as recommended
        FROM sector_analysis 
        WHERE trade_type = 'SWING' AND score >= 7
        ORDER BY score DESC, momentum_score DESC
    """).fetchall()]
    
    conn.close()
    return jsonify({"signals": signals, "current": current})

@app.route("/api/sector-intelligence")
def sector_intelligence():
    """Get all stocks analyzed and ranked by AI probability score across all sectors."""
    conn = get_db()
    cur = conn.execute("""
        SELECT symbol, company, sector, direction, score, live_price, entry_price, sl, t1, t2,
               adx, rsi, vol_ratio, risk_pct, trade_type, ai_prediction, ai_confidence, 
               momentum_score, updated_at, atr, risk_per
        FROM sector_analysis 
        ORDER BY (ai_confidence * 0.5 + score * 11.1 * 0.3 + momentum_score * 0.2) DESC
    """)
    
    stocks = []
    for row in cur.fetchall():
        s = {
            "symbol": row[0], "company": row[1], "sector": row[2], "direction": row[3],
            "score": row[4], "live_price": row[5], "entry_price": row[6], "sl": row[7],
            "t1": row[8], "t2": row[9], "adx": row[10], "rsi": row[11],
            "vol_ratio": row[12], "risk_pct": row[13], "trade_type": row[14],
            "ai_target": row[15], "ai_confidence": row[16], "momentum_score": row[17],
            "updated_at": row[18], "atr": row[19], "risk_per": row[20]
        }
        # Calculate a unified probability score for the UI
        s["prob_score"] = round((s["ai_confidence"] * 0.5) + (s["score"] * 11.1 * 0.3) + (s["momentum_score"] * 0.2), 1)
        stocks.append(s)
    
    conn.close()
    return jsonify({"stocks": stocks, "total": len(stocks)})

@app.route("/api/stock-detail/<symbol>")
def stock_detail(symbol):
    """Get comprehensive details for a specific stock."""
    conn = get_db()
    
    # 1. Current analysis from sector_analysis
    analysis = conn.execute("""
        SELECT * FROM sector_analysis WHERE symbol = ?
    """, (symbol,)).fetchone()
    
    if not analysis:
        conn.close()
        return jsonify({"error": "Stock analysis not found"}), 404
        
    analysis_dict = dict(analysis)
    
    # 2. Signal history from signal_log
    history = [dict(r) for r in conn.execute("""
        SELECT * FROM signal_log WHERE symbol = ? ORDER BY logged_at DESC LIMIT 10
    """, (symbol,)).fetchall()]
    
    # 3. Recent price history from price_cache (last 30 days)
    prices = [dict(r) for r in conn.execute("""
        SELECT * FROM price_cache WHERE symbol = ? ORDER BY date DESC LIMIT 30
    """, (symbol,)).fetchall()]
    
    # 4. Get latest AI predictions
    prediction = conn.execute("""
        SELECT * FROM ai_predictions WHERE symbol = ? ORDER BY created_at DESC LIMIT 1
    """, (symbol,)).fetchone()
    prediction_dict = dict(prediction) if prediction else None
    
    conn.close()
    
    return jsonify({
        "symbol": symbol,
        "analysis": analysis_dict,
        "history": history,
        "prices": prices,
        "prediction": prediction_dict
    })

@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"
    r.headers["Access-Control-Allow-Methods"] = "GET,POST,DELETE,OPTIONS"
    return r

# ── ROUTES ───────────────────────────────
@app.route("/", defaults={"p": ""})
@app.route("/<path:p>")
def root(p):
    fp = os.path.join(BASE, "../frontend/index.html")
    return send_file(fp) if os.path.exists(fp) else ("<h2>Frontend missing</h2>", 200)

@app.route("/api/status")
def status():
    cfg = gcfg()
    
    # Get total universe size
    total_universe = len(UNIVERSE)
    
    # Get stats from database
    prime_count = 0
    long_count = 0
    short_count = 0
    sector_stats = {}
    
    try:
        conn = get_db()
        # Prime: score >= 7
        prime_count = conn.execute("SELECT COUNT(*) FROM sector_analysis WHERE score >= 7").fetchone()[0]
        # Long: direction = 'LONG'
        long_count = conn.execute("SELECT COUNT(*) FROM sector_analysis WHERE direction = 'LONG'").fetchone()[0]
        # Short: direction = 'SHORT'
        short_count = conn.execute("SELECT COUNT(*) FROM sector_analysis WHERE direction = 'SHORT'").fetchone()[0]
        
        cur = conn.execute("""
            SELECT sector, COUNT(*) as count, AVG(score) as avg_score, AVG(momentum_score) as avg_momentum
            FROM sector_analysis 
            WHERE score >= 5
            GROUP BY sector
            ORDER BY avg_momentum DESC
        """)
        for row in cur.fetchall():
            sector_stats[row[0]] = {"count": row[1], "avg_score": round(row[2], 1), "avg_momentum": round(row[3], 1)}
        conn.close()
    except Exception as e:
        print(f"[STATUS ERROR] {e}")
    
    return jsonify({
        "ok":           True,
        "version":      "7.0",
        "python":       sys.version.split()[0],
        "capital":      cfg["capital"],
        "zerodha":      bool(os.environ.get("KITE_ACCESS_TOKEN")),
        "data_mode":    "REAL (Zerodha)" if cfg.get("use_real") else "SYNTHETIC",
        "universe":     total_universe,
        "prime_count":  prime_count,
        "long_count":   long_count,
        "short_count":  short_count,
        "cached":       len(_cache),
        "sectors":      SECTORS,
        "filter_names": FILTER_NAMES,
        "sector_stats": sector_stats,
        "ts":           datetime.datetime.now().isoformat(),
    })

@app.route("/api/sector/<sector>")
def get_sector_stocks(sector):
    """Get detailed analysis for all stocks in a sector with AI categorization."""
    conn = get_db()
    
    # 1. Get all stocks from UNIVERSE for this sector
    # Use fuzzy match for sector names (e.g. "Metal" vs "Metals")
    sector_norm = sector.strip().rstrip('s').lower() # "Metals" -> "metal"
    
    universe_stocks = [u for u in UNIVERSE if u[3].strip().rstrip('s').lower() == sector_norm]
    universe_symbols = [u[0] for u in universe_stocks]
    
    if not universe_stocks:
        conn.close()
        return jsonify({"error": f"No stocks found for sector: {sector}"}), 404

    # 2. Get existing analysis for these stocks
    placeholders = ",".join(["?"] * len(universe_symbols))
    cur = conn.execute(f"""
        SELECT symbol, company, direction, score, live_price, entry_price, sl, t1, t2,
               adx, rsi, vol_ratio, risk_pct, trade_type, ai_prediction, ai_confidence, 
               momentum_score, updated_at, atr, risk_per
        FROM sector_analysis 
        WHERE symbol IN ({placeholders})
        ORDER BY momentum_score DESC
    """, universe_symbols)
    
    analysis_map = {row[0]: dict(row) for row in cur.fetchall()}
    
    stocks = []
    already_moved = []
    yet_to_move = []
    
    for info in universe_stocks:
        sym = info[0]
        company = info[2]
        
        if sym in analysis_map:
            s = analysis_map[sym]
            # Ensure price/entry are valid
            price = s["live_price"] or 0
            entry = s["entry_price"] or price
            change = ((price - entry) / entry * 100) if entry > 0 else 0
            rsi = s["rsi"] or 50
            
            # Move Logic
            is_moved = False
            if s["direction"] == "LONG":
                if change > 2.0 or rsi > 60: is_moved = True
            else:
                if change < -2.0 or rsi < 40: is_moved = True
                
            if is_moved:
                s["move_status"] = "ALREADY MOVED"
                s["move_reason"] = f"{abs(change):.1f}% move from entry"
                already_moved.append(s)
            else:
                s["move_status"] = "YET TO MOVE"
                if s["ai_confidence"] >= 75: s["move_reason"] = "Primed (High AI Conviction)"
                elif s["vol_ratio"] >= 1.3: s["move_reason"] = "Accumulating (High Volume)"
                else: s["move_reason"] = "Consolidating near entry"
                yet_to_move.append(s)
        else:
            # Not analyzed yet - create placeholder
            s = {
                "symbol": sym, "company": company, "direction": "NEUTRAL", "score": 0,
                "live_price": 0, "entry_price": 0, "sl": 0, "t1": 0, "t2": 0,
                "adx": 0, "rsi": 50, "vol_ratio": 1.0, "risk_pct": 0,
                "trade_type": "PENDING", "ai_target": 0, "ai_confidence": 0,
                "momentum_score": 0, "updated_at": None, "atr": 0, "risk_per": 0,
                "move_status": "YET TO MOVE", "move_reason": "Pending Analysis"
            }
            yet_to_move.append(s)
            
        stocks.append(s)
    
    # Probable Mover Logic
    probable_mover = None
    if yet_to_move:
        # Prioritize yet_to_move
        yet_to_move.sort(key=lambda x: (x["ai_confidence"] * 0.5 + x["score"] * 5), reverse=True)
        if yet_to_move[0]["score"] > 0:
            probable_mover = yet_to_move[0]
            m = probable_mover
            reasoning = []
            if m["score"] >= 7: reasoning.append(f"Strong setup ({m['score']}/9)")
            if m["ai_confidence"] >= 75: reasoning.append(f"AI Conviction {m['ai_confidence']}%")
            if m["vol_ratio"] >= 1.3: reasoning.append("Volume rising")
            m["reasoning"] = " · ".join(reasoning) if reasoning else "Early breakout phase"

    # Sector Outlook
    bullish_count = len([s for s in stocks if s["direction"] == "LONG"])
    bearish_count = len([s for s in stocks if s["direction"] == "SHORT"])
    analyzed_stocks = [s for s in stocks if s["score"] > 0]
    avg_score = sum([s["score"] for s in analyzed_stocks]) / len(analyzed_stocks) if analyzed_stocks else 0
    
    outlook = "NEUTRAL"
    if avg_score >= 6.0: outlook = "BULLISH"
    elif avg_score <= 4.0 and avg_score > 0: outlook = "BEARISH"

    conn.close()
    return jsonify({
        "sector": sector, 
        "stocks": stocks,
        "already_moved": already_moved,
        "yet_to_move": yet_to_move,
        "count": len(stocks),
        "probable_mover": probable_mover,
        "outlook": outlook,
        "sentiment": {"bullish": bullish_count, "bearish": bearish_count}
    })

@app.route("/api/ai-predictions")
def get_ai_predictions():
    """Get AI predicted next movers."""
    conn = get_db()
    cur = conn.execute("""
        SELECT symbol, sector, direction, current_price, predicted_target, confidence, ai_score, reasoning, created_at
        FROM ai_predictions 
        ORDER BY confidence DESC LIMIT 20
    """)
    
    predictions = []
    for row in cur.fetchall():
        predictions.append({
            "symbol": row[0], "sector": row[1], "direction": row[2],
            "current_price": row[3], "predicted_target": row[4],
            "confidence": row[5], "ai_score": row[6], "reasoning": row[7], "created_at": row[8]
        })
    
    conn.close()
    return jsonify({"predictions": predictions, "count": len(predictions)})

@app.route("/api/config", methods=["GET", "POST"])
def config_route():
    if request.method == "POST":
        cfg = gcfg()
        cfg.update(request.json or {})
        cfg["use_real"] = True
        cfg["use_zerodha_ltp"] = True
        scfg(cfg)
        clear_cache()
        return jsonify({"ok": True})
    return jsonify(gcfg())

@app.route("/api/refresh", methods=["POST"])
def refresh_data():
    """Force refresh all data from Zerodha"""
    clear_cache()
    cfg = gcfg()
    cfg["use_real"] = True
    scfg(cfg)
    return jsonify({"ok": True, "message": "Cache cleared, using REAL data from Zerodha Kite"})

@app.route("/api/ltp-stream")
def ltp_stream():
    """
    Server-Sent Events (SSE) endpoint.
    Streams an array of {symbol, ltp, ts, fresh} about once per second.
    """
    def _gen():
        last_yield = 0.0
        while True:
            try:
                now_ts = time.time()
                # Throttle to ~1 update/sec
                if now_ts - last_yield < 1.0:
                    time.sleep(0.25)
                    continue
                last_yield = now_ts

                updates = []
                if _zerodha_stream_active():
                    updates = _fresh_ltps(now_ts=now_ts, max_age_sec=2.5)

                payload = {
                    "ts": datetime.datetime.now().isoformat(),
                    "updates": updates,
                }
                yield f"data: {json.dumps(payload)}\n\n"
            except GeneratorExit:
                return
            except Exception as e:
                # Avoid killing the SSE stream on transient errors.
                try:
                    payload = {"ts": datetime.datetime.now().isoformat(), "updates": [], "error": str(e)}
                    yield f"data: {json.dumps(payload)}\n\n"
                except Exception:
                    return

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return Response(stream_with_context(_gen()), mimetype="text/event-stream", headers=headers)

def get_live_price(symbol):
    """Get current/live price for a stock from Zerodha"""
    prices = get_zerodha_live_prices([symbol])
    return prices.get(symbol)

_zerodha_kite_instance = None

def _get_zerodha_kite():
    """Get or create a KiteConnect instance for REST API calls."""
    global _zerodha_kite_instance
    if _zerodha_kite_instance is not None:
        return _zerodha_kite_instance
    
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    access_token = os.environ.get("KITE_ACCESS_TOKEN", "").strip()
    if not api_key or not access_token:
        return None
    
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        _zerodha_kite_instance = kite
        return kite
    except Exception as e:
        print(f"[ZERODHA] Failed to create KiteConnect: {e}")
        return None

def get_zerodha_live_prices(symbols, retries=3):
    """
    Robust Zerodha LTP fetcher with retry + exponential backoff.
    Falls back to DB cache if Zerodha fails.
    """
    api_key = os.environ.get("KITE_API_KEY", "").strip()
    access_token = os.environ.get("KITE_ACCESS_TOKEN", "").strip()
    
    if not api_key or not access_token:
        print("[ZERODHA] No credentials found")
        return _get_from_db_cache(symbols)
    
    result = {}
    
    for attempt in range(retries):
        try:
            kite = _get_zerodha_kite()
            if not kite:
                break
            
            instruments = kite.instruments("NSE")
            inst_map = {i["tradingsymbol"]: i["instrument_token"] for i in instruments}
            
            tokens = []
            sym_map = {}
            for sym in symbols:
                for cand in [sym, sym.replace("&", "%26")]:
                    if cand in inst_map:
                        t = inst_map[cand]
                        tokens.append(t)
                        sym_map[t] = sym
                        break
            
            if not tokens:
                print("[ZERODHA] No tokens matched")
                return _get_from_db_cache(symbols)
            
            ltp_data = kite.ltp(tokens)
            for tok_str, data in ltp_data.items():
                tok = int(tok_str)
                sym = sym_map.get(tok)
                if sym and data.get("last_price"):
                    result[sym] = round(float(data["last_price"]), 2)
            
            print(f"[ZERODHA] Got prices for {len(result)}/{len(symbols)} symbols (attempt {attempt+1})")
            return result
            
        except Exception as e:
            err_str = str(e).lower()
            if "access token" in err_str or "incorrect" in err_str:
                print(f"[ZERODHA] AUTH FAILED: {e}")
                return _get_from_db_cache(symbols)
            wait = 2 ** attempt
            print(f"[ZERODHA] Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            if attempt < retries - 1:
                time.sleep(wait)
    
    print(f"[ZERODHA] All {retries} attempts failed. Using DB cache.")
    return _get_from_db_cache(symbols)

def _get_from_db_cache(symbols):
    """Fallback: get prices from local DB cache."""
    result = {}
    for sym in symbols:
        cached = get_cached_price(sym)
        if cached and cached.get("price", 0) > 0:
            result[sym] = round(float(cached["price"]), 2)
    print(f"[CACHE] Got {len(result)} prices from DB cache")
    return result

@app.route("/api/scanner-cached")
def scanner_cached():
    """Get cached scanner results from database (fast, no re-scan)"""
    ms = int(request.args.get("min_score", 0))
    sec = request.args.get("sector", "")
    dr = request.args.get("direction", "")
    rec = request.args.get("type", "")
    quality = request.args.get("quality", "all")  # all, prime, high
    
    conn = get_db()
    
    # Base query - use sector_analysis as the main source for scanned data
    query = """
        SELECT symbol, company, sector, direction, score, live_price, entry_price, 
               sl, t1, t2, t3, adx, rsi, vol_ratio, risk_pct, risk_per, atr, trade_type, 
               e20, e50, e200, supertrend, st_dir, vwap, poc, va_low, va_high,
               index_tag, index_weight,
               ai_prediction, ai_confidence, momentum_score, updated_at
        FROM sector_analysis 
        WHERE 1=1
    """
    params = []
    
    # Add quality filters for more trustworthy signals
    if quality == "prime":
        query += " AND score >= 7 AND adx >= 20 AND vol_ratio >= 1.0"
    elif quality == "high":
        query += " AND score >= 8 AND adx >= 25 AND vol_ratio >= 1.5"
    elif quality == "elite":
        query += """
            AND score >= 8
            AND adx >= 25
            AND vol_ratio >= 1.2
            AND ai_confidence >= 70
            AND entry_price > 0
            AND live_price > 0
            AND abs((live_price - entry_price) / entry_price) <= 0.012
            AND va_low > 0 AND va_high > 0
            AND live_price BETWEEN va_low AND va_high
            AND (
                (direction = 'LONG' AND st_dir = 1)
                OR
                (direction = 'SHORT' AND st_dir = -1)
            )
        """
    else:
        # Default behavior: show everything that has been scanned
        if ms > 0:
            query += " AND score >= ?"
            params.append(ms)
    
    if sec:
        query += " AND sector = ?"
        params.append(sec)
    if dr:
        query += " AND direction = ?"
        params.append(dr)
    if rec:
        query += " AND trade_type = ?"
        params.append(rec)
    
    query += " ORDER BY score DESC, momentum_score DESC"
    
    cur = conn.execute(query, params)
    out = []
    for row in cur.fetchall():
        out.append({
            "symbol": row[0], "company": row[1], "sector": row[2], "direction": row[3],
            "score": row[4], "live_price": row[5], "entry": row[6], "sl": row[7],
            "t1": row[8], "t2": row[9], "t3": row[10], "adx": row[11], "rsi": row[12],
            "vol_ratio": row[13], "risk_pct": row[14], "risk_per": row[15], "atr": row[16],
            "recommended": row[17],
            "e20": row[18], "e50": row[19], "e200": row[20],
            "supertrend": row[21], "st_dir": row[22],
            "vwap": row[23], "poc": row[24], "va_low": row[25], "va_high": row[26],
            "index_tag": row[27], "index_weight": row[28],
            "ai_target": row[29], "ai_confidence": row[30],
            "momentum_score": row[31], "updated_at": row[32]
        })
    conn.close()
    
    return jsonify({"stocks": out, "total": len(out), "cached": True})

@app.route("/api/scanner")
def scanner():
    """Default to cached scanner results for immediate loading."""
    refresh = request.args.get("refresh", "0") == "1"
    if not refresh:
        return scanner_cached()
    
    # Original scanner logic for force refresh
    ms  = int(request.args.get("min_score", 0))
    sec = request.args.get("sector", "")
    dr  = request.args.get("dir", "")
    rec = request.args.get("type", "")  # SWING, INTRA, or empty for all
    refresh = request.args.get("refresh", "0") == "1"
    cfg = gcfg()
    use_zerodha = cfg.get("use_zerodha_ltp", True) and bool(os.environ.get("KITE_API_KEY")) and bool(os.environ.get("KITE_ACCESS_TOKEN"))
    out = []
    skipped = []
    use_real = cfg.get("use_real", True)
    print(f"[SCANNER] Starting scan (min_score={ms}, type={rec}, use_real={use_real}, refresh={refresh})...")
    print(f"[SCANNER] Today's date: {datetime.date.today()}")
    
    # Pre-fetch live prices from Zerodha
    zerodha_live_prices = {}
    if use_zerodha:
        print(f"[SCANNER] use_zerodha=True, fetching live prices...")
        zerodha_live_prices = get_zerodha_live_prices([u[0] for u in UNIVERSE])
        print(f"[SCANNER] Got live prices for {len(zerodha_live_prices)} symbols")
        print(f"[SCANNER] TATAELXSI live: {zerodha_live_prices.get('TATAELXSI')}")
    else:
        print("[SCANNER] use_zerodha=False - Zerodha not available")
    
    for info in UNIVERSE:
        sym = info[0]
        yf_ticker = info[1]
        if sec and info[3] != sec: continue
        try:
            rows, inds = get_stock(sym, use_real=use_real, force_refresh=refresh)
            if not rows or len(rows) < 30:
                skipped.append((sym, "no data"))
                continue
            
            # Get live price from Zerodha
            live_price = zerodha_live_prices.get(sym)
            last_date = rows[-1]["date"]
            last_close = rows[-1]["close"]
            src = "ZERODHA" if live_price else "NONE"
            print(f"[LIVE] {sym}: {src}={live_price}, last_close={last_close}")
            
            sc, direction, fl, meta = score_candle(rows[-1], inds[-1])
            if sc < ms:
                skipped.append((sym, f"low score {sc}"))
                continue
            if dr and direction != dr:
                skipped.append((sym, f"wrong dir {direction}"))
                continue
            signal = make_signal(sym, info, rows, inds, -1, sc, direction, fl, meta)
            
            # Filter by recommendation type
            if rec and signal.get("recommended") != rec:
                skipped.append((sym, f"wrong type {signal.get('recommended')}"))
                continue
            
            # Determine trade type for proper level calculation
            trade_type = signal.get("recommended", "SWING")
            
            # Override with live price if available
            if live_price:
                lv = compute_levels(live_price, inds[-1]["atr"], direction, trade_type)
                risk_pct = round(lv["risk_per"] / live_price * 100, 2)
                prev_close = rows[-1]["close"]
                day_chg = round((live_price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0
                signal["current"] = live_price
                signal["live_price"] = live_price
                signal["entry"] = lv["entry"]
                signal["sl"] = lv["sl"]
                signal["t1"] = lv["t1"]
                signal["t2"] = lv["t2"]
                signal["t3"] = lv["t3"]
                signal["risk_per"] = lv["risk_per"]
                signal["atr"] = lv["atr"]
                signal["trade_type"] = trade_type
                signal["risk_pct"] = risk_pct
                signal["day_change_pct"] = day_chg
                signal["data_date"] = last_date + " + LIVE"
            else:
                # No live price — use historical close as base price
                base_price = rows[-1]["close"]
                atr_val = inds[-1]["atr"]
                lv = compute_levels(base_price, atr_val, direction, trade_type)
                risk_pct = round(lv["risk_per"] / base_price * 100, 2)
                signal["current"] = base_price
                signal["live_price"] = base_price
                signal["entry"] = lv["entry"]
                signal["sl"] = lv["sl"]
                signal["t1"] = lv["t1"]
                signal["t2"] = lv["t2"]
                signal["t3"] = lv["t3"]
                signal["risk_per"] = lv["risk_per"]
                signal["atr"] = lv["atr"]
                signal["trade_type"] = trade_type
                signal["risk_pct"] = risk_pct
                signal["day_change_pct"] = 0.0
                signal["data_date"] = last_date
            
            out.append(signal)
            
            # Store in sector_analysis for detailed stock analysis
            try:
                ind = inds[-1]
                _store_sector_analysis(info, sym, direction, sc, {
                    "entry": signal.get("entry", 0),
                    "sl": signal.get("sl", 0),
                    "t1": signal.get("t1", 0),
                    "t2": signal.get("t2", 0)
                }, live_price, signal.get("risk_pct", 0), trade_type, meta, ind)
            except Exception as e:
                print(f"[STORE ERROR] {sym}: {e}")
                
        except Exception as e:
            print(f"[ERROR] {sym}: {e}")
            skipped.append((sym, str(e)))
            continue
    
    out.sort(key=lambda x: (-x["score"], -x["adx"]))
    print(f"[SCANNER] Found {len(out)} signals, scanned {len(UNIVERSE)} stocks, skipped {len(skipped)}")
    return jsonify({"stocks": out, "total": len(out), "scanned": len(UNIVERSE), "skipped": skipped[:10]})

@app.route("/api/signals")
def signals():
    ms    = int(request.args.get("min_score", 5))
    today = str(datetime.date.today())
    cfg   = gcfg()
    out   = []
    skipped = []
    rnd   = random.Random(42)
    use_real = cfg.get("use_real", True)
    for info in UNIVERSE:
        sym = info[0]
        try:
            rows, inds = get_stock(sym, use_real=use_real)
            if not rows or len(rows) < 30:
                skipped.append((sym, "no data"))
                continue
            sc, dr, fl, meta = score_candle(rows[-1], inds[-1])
            if sc < ms:
                skipped.append((sym, f"low score {sc}"))
                continue
            t   = rnd.choice(ENTRY_TIMES_OPEN if rnd.random() < 0.65 else ENTRY_TIMES_POWER)
            sig = make_signal(sym, info, rows, inds, -1, sc, dr, fl, meta, t)
            out.append(sig)
        except Exception as e:
            skipped.append((sym, str(e)))
            continue
    out.sort(key=lambda x: (-x["score"], -x["adx"]))
    # Log signals with live_price
    conn = get_db()
    for s in out[:25]:
        conn.execute(
            "INSERT INTO signal_log(signal_date,symbol,sector,direction,score,"
            "entry,sl,t1,t2,t3,adx,rsi,vol_ratio,filters,entry_time,live_price,trade_type,atr,risk_pct) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (today, s["symbol"], s["sector"], s["direction"], s["score"],
             s["entry"], s["sl"], s["t1"], s["t2"], s["t3"],
             s["adx"], s["rsi"], s["vol_ratio"],
             json.dumps(s["filters"]), s["entry_time"], s.get("live_price", 0),
             s.get("trade_type", "SWING"), s.get("atr", 0), s.get("risk_pct", 0))
        )
    conn.commit(); conn.close()
    return jsonify({
        "signals":      out,
        "count":        len(out),
        "min_score":    ms,
        "date":         today,
        "filter_names": FILTER_NAMES,
        "skipped":      skipped[:10],
    })

@app.route("/api/history/<sym>")
def history(sym):
    ms  = int(request.args.get("min_score", 5))
    cfg = gcfg()
    rnd = random.Random(42)
    use_real = cfg.get("use_real", False)
    try:
        rows, inds = get_stock(sym, use_real=use_real)
        info = next((x for x in UNIVERSE if x[0] == sym), None)
        if not rows or not info:
            return jsonify({"error": "Symbol not found"}), 404
        sigs  = []
        ohlcv = []
        start = max(60, len(rows) - 180)
        for i in range(start, len(rows)):
            sc, dr, fl, meta = score_candle(rows[i], inds[i])
            if sc >= ms:
                t = rnd.choice(ENTRY_TIMES_OPEN if rnd.random() < 0.65 else ENTRY_TIMES_POWER)
                sigs.append(make_signal(sym, info, rows, inds, i, sc, dr, fl, meta, t))
            ohlcv.append({
                "date":      rows[i]["date"],
                "open":      rows[i]["open"],
                "high":      rows[i]["high"],
                "low":       rows[i]["low"],
                "close":     rows[i]["close"],
                "volume":    rows[i]["volume"],
                "adx":       inds[i]["adx"],
                "rsi":       inds[i]["rsi"],
                "e20":       inds[i]["e20"],
                "e50":       inds[i]["e50"],
                "e200":      inds[i]["e200"],
                "st":        inds[i]["st"],
                "st_dir":    inds[i]["st_dir"],
                "bb_up":     inds[i]["bb_up"],
                "bb_lo":     inds[i]["bb_lo"],
                "vr":        inds[i]["vr"],
                "macd_hist": inds[i]["macd_hist"],
                "dip":       inds[i]["dip"],
                "dim":       inds[i]["dim"],
                "vwap":      inds[i]["vwap"],
                "atr_rk":    inds[i]["atr_rk"],
            })
        return jsonify({"sym": sym, "signals": sigs, "ohlcv": ohlcv, "total": len(sigs)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/backtest", methods=["GET", "POST"])
def backtest_route():
    if request.method == "GET":
        # Return cached backtest results
        conn = get_db()
        row = conn.execute("SELECT * FROM backtest_results ORDER BY created_at DESC LIMIT 1").fetchone()
        conn.close()
        if row:
            return jsonify({
                "summary": json.loads(row["summary"]),
                "trades": json.loads(row["trades"]),
                "equity": json.loads(row["equity"]),
                "params": json.loads(row["params"]),
                "created_at": row["created_at"],
                "cached": True
            })
        return jsonify({"error": "No backtest data found"}), 404

    params  = request.json or {}
    cfg     = gcfg()
    months  = int(params.get("months", 6))
    syms    = params.get("symbols", [u[0] for u in UNIVERSE])
    trade_type = params.get("trade_type", "ALL")
    use_real = cfg.get("use_real", False)
    
    print(f"[BT] Loading {len(syms)} stocks (use_real={use_real}, type={trade_type})...")
    stock_data = []
    for info in UNIVERSE:
        if info[0] not in syms: continue
        try:
            rows, inds = get_stock(info[0], use_real=use_real)
            if not rows or len(rows) < 30: continue
            stock_data.append((info, rows, inds))
        except Exception as e:
            print(f"  skip {info[0]}: {e}")
            
    print(f"[BT] Running on {len(stock_data)} stocks...")
    result = run_backtest(stock_data, params)
    
    # Store in DB for retrieval
    try:
        conn = get_db()
        conn.execute("""
            INSERT INTO backtest_results (params, summary, trades, equity)
            VALUES (?, ?, ?, ?)
        """, (json.dumps(params), json.dumps(result["summary"]), 
              json.dumps(result["trades"]), json.dumps(result["equity"])))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[BT ERROR] Failed to cache: {e}")
        
    return jsonify(result)

@app.route("/api/oi-spikes")
def oi_spikes():
    """Default to cached results for performance."""
    refresh = request.args.get("refresh", "0") == "1"
    if not refresh:
        # Sort cached analysis by vol_ratio (lightweight)
        conn = get_db()
        cur = conn.execute("""
            SELECT
                sa.symbol,
                sa.company,
                sa.sector,
                COALESCE(lp.price, sa.live_price, 0) AS live_price,
                COALESCE(lp.day_change_pct, 0) AS price_change_pct,
                COALESCE(lp.vol_ratio, sa.vol_ratio, 0) AS vol_ratio,
                COALESCE(lp.avg_volume, 0) AS avg_volume,
                sa.score,
                sa.direction,
                sa.trade_type,
                sa.adx,
                sa.rsi,
                sa.momentum_score,
                sa.entry_price AS entry,
                sa.sl AS sl,
                sa.risk_per AS risk_per
            FROM sector_analysis sa
            LEFT JOIN live_prices lp ON lp.symbol = sa.symbol
            WHERE COALESCE(lp.vol_ratio, sa.vol_ratio, 0) >= 1.5
            ORDER BY COALESCE(lp.vol_ratio, sa.vol_ratio, 0) DESC
            LIMIT 50
        """)
        out = []
        for r in cur.fetchall():
            spike_type = "NORMAL"
            if (r["vol_ratio"] or 0) >= 2.0:
                if (r["price_change_pct"] or 0) > 0:
                    spike_type = "BUYING"
                elif (r["price_change_pct"] or 0) < 0:
                    spike_type = "SELLING"
                else:
                    spike_type = "HIGH VOL"
            out.append({
                "symbol": r["symbol"], "company": r["company"], "sector": r["sector"], "live_price": r["live_price"],
                "price_change_pct": r["price_change_pct"],
                "vol_ratio": r["vol_ratio"], "avg_volume": r["avg_volume"],
                "spike_type": spike_type,
                "score": r["score"], "direction": r["direction"], "trade_type": r["trade_type"],
                "adx": r["adx"], "rsi": r["rsi"], "momentum_score": r["momentum_score"],
                "entry": r["entry"] or 0, "sl": r["sl"] or 0, "risk_per": r["risk_per"] or 0,
                "is_spike": (r["vol_ratio"] or 0) >= 2.0
            })
        conn.close()
        return jsonify({"spikes": [x for x in out if x["is_spike"]], "all": out, "cached": True})

    # Original heavy logic only if refresh requested
    cfg = gcfg()
    use_real = cfg.get("use_real", True)
    zerodha_live = {}
    
    try:
        kite = _get_zerodha_kite()
        if kite:
            instruments = kite.instruments("NSE")
            inst_map = {i["tradingsymbol"]: i["instrument_token"] for i in instruments}
            tokens = []
            sym_map = {}
            for info in UNIVERSE:
                sym = info[0]
                for cand in [sym, sym.replace("&", "%26")]:
                    if cand in inst_map:
                        t = inst_map[cand]
                        tokens.append(t)
                        sym_map[t] = sym
                        break
            if tokens:
                ltp_data = kite.ltp(tokens)
                for tok_str, d in ltp_data.items():
                    sym = sym_map.get(int(tok_str))
                    if sym and d.get("last_price"):
                        zerodha_live[sym] = round(float(d["last_price"]), 2)
    except Exception as e:
        print(f"[OI] Zerodha live error: {e}")
    
    results = []
    
    for info in UNIVERSE:
        sym = info[0]
        try:
            rows = get_ohlcv(info, months=2, use_real=use_real)
            if not rows or len(rows) < 5:
                continue
            
            last = rows[-1]
            prev_close = rows[-2]["close"] if len(rows) >= 2 else last["close"]
            
            vol_window = [r["volume"] for r in rows[-21:-1]] if len(rows) >= 21 else [r["volume"] for r in rows[:-1]]
            avg_vol = sum(vol_window) / len(vol_window) if vol_window else 1
            vol_ratio = last["volume"] / avg_vol if avg_vol > 0 else 1
            
            live = zerodha_live.get(sym, last["close"])
            price_chg = round((live - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0
            
            inds = compute_indicators(rows)
            sc, direction, fl, meta = score_candle(last, inds[-1])
            atr_val = inds[-1].get("atr", last["close"] * 0.015)
            if atr_val <= 0:
                atr_val = last["close"] * 0.015
            
            trade_type = meta.get("recommended", "SWING")
            lv = compute_levels(live, atr_val, direction, trade_type)
            
            results.append({
                "symbol":         sym,
                "company":        info[2],
                "sector":         info[3],
                "live_price":    live,
                "prev_close":     prev_close,
                "price_change_pct": price_chg,
                "volume":         last["volume"],
                "avg_volume":     round(avg_vol),
                "vol_ratio":     round(vol_ratio, 2),
                "direction":      direction,
                "score":          sc,
                "trade_type":     trade_type,
                "atr":            lv["atr"],
                "entry":          lv["entry"],
                "sl":             lv["sl"],
                "t1":             lv["t1"],
                "risk_per":       lv["risk_per"],
                "risk_pct":       round(lv["risk_per"] / live * 100, 2),
                "adx":            round(meta.get("adx", 0), 1),
                "rsi":            round(meta.get("rsi", 50), 1),
                "is_spike":       vol_ratio >= 2.0,
                "spike_type":     "BUYING" if price_chg > 0 and vol_ratio >= 2.0 else ("SELLING" if price_chg < 0 and vol_ratio >= 2.0 else ("HIGH VOL" if vol_ratio >= 2.0 else "NORMAL")),
            })
        except Exception as e:
            continue
    
    results.sort(key=lambda x: (-x["vol_ratio"], -abs(x["price_change_pct"])))
    
    spikes = [r for r in results if r["is_spike"]]
    all_sorted = results[:50]
    
    return jsonify({
        "spikes": spikes[:30],
        "all": all_sorted,
        "total_scanned": len(results),
        "spike_count": len(spikes),
        "ts": datetime.datetime.now().isoformat(),
    })

@app.route("/api/options-hero-zero")
def options_hero_zero():
    index_name = request.args.get("index", "NIFTY").upper().strip()
    if index_name not in ("NIFTY", "BANKNIFTY"):
        return jsonify({"error": "index must be NIFTY or BANKNIFTY"}), 400

    refresh = request.args.get("refresh", "0") == "1"
    if refresh:
        try:
            out = _scan_index_options_once(index_name)
            return jsonify(out)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM option_oi_signal WHERE index_name=? ORDER BY id DESC LIMIT 200",
        (index_name,),
    ).fetchall()
    # Build CE/PE summary from cached signals
    ce_oi = 0; pe_oi = 0; ce_doi = 0; pe_doi = 0
    for r in rows:
        if (r["opt_type"] or "") == "CE":
            ce_oi += int(r["oi"] or 0); ce_doi += int(r["doi"] or 0)
        else:
            pe_oi += int(r["oi"] or 0); pe_doi += int(r["doi"] or 0)
    summary = {
        "ce_total_oi": ce_oi, "pe_total_oi": pe_oi,
        "ce_total_doi": ce_doi, "pe_total_doi": pe_doi,
        "oi_imbalance": ce_oi - pe_oi, "doi_imbalance": ce_doi - pe_doi,
        "momentum": "BULLISH" if (ce_doi - pe_doi) > 0 else "BEARISH"
    }
    conn.close()

    signals = [dict(r) for r in rows]
    signals.sort(key=lambda x: float(x.get("score") or 0), reverse=True)
    top = None
    for s in signals:
        if s.get("signal") == "HERO":
            top = s
            break
    if top is None and signals:
        top = signals[0]

    return jsonify({"index": index_name, "top": top, "signals": signals[:20], "summary": summary, "cached": True, "ts": datetime.datetime.now().isoformat()})

# ── WhatsApp Alerts (CallMeBot) ─────────────────────────────
def _load_whatsapp_config():
    cfg_path = os.path.join(BASE, "../data/whatsapp.json")
    default_cfg = {
        "send_on": {"alerts": True, "signals": True, "options": True},
        "thresholds": {"signal_min_score": 8, "alert_min_score": 7},
        "recipients": []
    }
    if not os.path.exists(cfg_path):
        return default_cfg
    try:
        with open(cfg_path) as f:
            data = json.load(f)
        if isinstance(data, list):
            default_cfg["recipients"] = data
            return default_cfg
        if isinstance(data, dict):
            cfg = default_cfg
            cfg.update({k: v for k, v in data.items() if k in ("send_on", "thresholds", "recipients")})
            if not isinstance(cfg.get("recipients"), list):
                cfg["recipients"] = []
            if not isinstance(cfg.get("send_on"), dict):
                cfg["send_on"] = default_cfg["send_on"]
            if not isinstance(cfg.get("thresholds"), dict):
                cfg["thresholds"] = default_cfg["thresholds"]
            return cfg
    except Exception:
        pass
    return default_cfg

def _send_whatsapp_callmebot(phone, apikey, text):
    try:
        import urllib.parse, urllib.request
        q = urllib.parse.urlencode({"phone": phone, "text": text, "apikey": apikey})
        url = f"https://api.callmebot.com/whatsapp.php?{q}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            _ = resp.read()
        return True
    except Exception as e:
        p = str(phone)
        masked = (p[:3] + "****" + p[-3:]) if len(p) >= 8 else "****"
        print(f"[WHATSAPP] Failed for {masked}: {e}")
        return False

def _enqueue_whatsapp(kind, event_key, message):
    with _whatsapp_event_lock:
        if event_key in _whatsapp_event_cache:
            return
        _whatsapp_event_cache[event_key] = time.time()
    def _worker():
        cfg = _load_whatsapp_config()
        targets = cfg.get("recipients") or []
        sent = 0
        for t in targets:
            if isinstance(t, dict) and t.get("enabled", True) is False:
                continue
            phone = str((t.get("phone") if isinstance(t, dict) else "") or "").strip()
            apikey = str((t.get("apikey") if isinstance(t, dict) else "") or "").strip()
            kinds = (t.get("kinds") if isinstance(t, dict) else None)
            if kinds and isinstance(kinds, list) and kind not in kinds:
                continue
            if not phone or not apikey:
                continue
            if _send_whatsapp_callmebot(phone, apikey, message):
                sent += 1
    threading.Thread(target=_worker, daemon=True).start()

def _maybe_send_whatsapp_signal(signal_data):
    cfg = _load_whatsapp_config()
    if not cfg.get("send_on", {}).get("signals", True):
        return
    thr = int(cfg.get("thresholds", {}).get("signal_min_score", 8))
    score = float(signal_data.get("score") or 0)
    adx = float(signal_data.get("adx") or 0)
    vr = float(signal_data.get("vol_ratio") or 0)
    if score < thr:
        return
    if adx < 25 or vr < 1.2:
        return
    event_key = f"signal:{signal_data.get('date')}:{signal_data.get('symbol')}:{signal_data.get('trade_type','SWING')}"
    msg = (
        f"HIGH ACCURACY SIGNAL: {signal_data.get('symbol')} | {signal_data.get('direction')} | "
        f"Score {score}/9 | ADX {adx:.1f} | Vol {vr:.2f}x | "
        f"Entry ₹{signal_data.get('entry')} | SL ₹{signal_data.get('sl')} | T1 ₹{signal_data.get('t1')} | T2 ₹{signal_data.get('t2')}"
    )
    _enqueue_whatsapp("signals", event_key, msg)

def _maybe_send_whatsapp_alert(alert_data, alert_id):
    cfg = _load_whatsapp_config()
    if not cfg.get("send_on", {}).get("alerts", True):
        return
    thr = int(cfg.get("thresholds", {}).get("alert_min_score", 7))
    score = float(alert_data.get("score") or 0)
    if score < thr:
        return
    event_key = f"alert:{alert_id}"
    msg = (
        f"ALERT: {alert_data.get('symbol')} | {alert_data.get('alert_type')} | {alert_data.get('direction')} | "
        f"Price ₹{alert_data.get('price')} | Chg {alert_data.get('change_pct')}% | Vol {alert_data.get('vol_ratio')}x | "
        f"Score {score}/9 | Entry ₹{alert_data.get('entry_price')} | SL ₹{alert_data.get('sl')} | T1 ₹{alert_data.get('t1')}"
    )
    _enqueue_whatsapp("alerts", event_key, msg)

@app.route("/api/whatsapp/send")
def whatsapp_send():
    index_name = request.args.get("index", "NIFTY").upper().strip()
    # Use latest hero-zero cached signals
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM option_oi_signal WHERE index_name=? ORDER BY id DESC LIMIT 10", (index_name,)
    ).fetchall()
    conn.close()
    signals = [dict(r) for r in rows if (r.get("signal") == "HERO")]
    if not signals:
        return jsonify({"ok": False, "error": "No HERO signals"}), 400
    top = sorted(signals, key=lambda x: float(x.get("score") or 0), reverse=True)[0]
    msg = f"{index_name} HERO: BUY {top.get('opt_type')} {top.get('tradingsymbol')} | LTP ₹{top.get('ltp')} | ΔOI {top.get('doi_pct')}% | ΔP {top.get('dltp_pct')}% | Exp {top.get('expiry')} | Score {top.get('score')}"
    _enqueue_whatsapp("options", f"options:{index_name}:{top.get('tradingsymbol')}:{top.get('ts')}", msg)
    return jsonify({"ok": True, "queued": True, "message": msg})

@app.route("/api/whatsapp/test")
def whatsapp_test():
    kind = request.args.get("kind", "alerts").strip().lower()
    if kind not in ("alerts", "signals", "options"):
        return jsonify({"error": "kind must be alerts/signals/options"}), 400
    msg = request.args.get("text", "Test message from Trade Smart v7").strip()
    event_key = f"test:{kind}:{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    _enqueue_whatsapp(kind, event_key, msg)
    return jsonify({"ok": True, "queued": True, "event_key": event_key})

@app.route("/api/whatsapp/config", methods=["GET", "POST"])
def whatsapp_config():
    cfg_path = os.path.join(BASE, "../data/whatsapp.json")
    if request.method == "GET":
        cfg = _load_whatsapp_config()
        return jsonify(cfg)

    data = request.json
    if not isinstance(data, dict):
        return jsonify({"error": "Config must be a JSON object"}), 400

    if "recipients" in data and not isinstance(data["recipients"], list):
        return jsonify({"error": "recipients must be a list"}), 400

    os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
    try:
        with open(cfg_path, "w") as f:
            json.dump(data, f, indent=2)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/alerts")
def get_alerts():
    """Get all unacknowledged momentum alerts."""
    alerts = get_unacknowledged_alerts()
    return jsonify({"alerts": alerts, "count": len(alerts)})

@app.route("/api/alerts/acknowledge", methods=["POST"])
def ack_alert():
    d = request.json or {}
    alert_id = d.get("id")
    if alert_id:
        acknowledge_alert(alert_id)
    return jsonify({"ok": True})

@app.route("/api/signals/log")
def get_signal_history():
    """Get historical signal log."""
    limit = int(request.args.get("limit", 100))
    signals = get_signal_log(limit)
    return jsonify({"signals": signals})

@app.route("/api/live-prices")
def live_prices():
    """Get all live prices from cache."""
    prices = get_all_live_prices_db()
    return jsonify({"prices": prices, "count": len(prices)})

@app.route("/api/calc", methods=["POST"])
def calc():
    d     = request.json or {}
    cap   = float(d.get("capital", 100000))
    rp    = float(d.get("risk_pct", 1.5)) / 100
    ep    = float(d.get("entry", 0))
    sl    = float(d.get("sl", 0))
    dr    = d.get("dir", "LONG")
    sym   = d.get("symbol", "STOCK")
    if not ep or not sl or ep == sl:
        return jsonify({"error": "Invalid entry or SL"}), 400
    risk  = round(cap * rp)
    rps   = round(abs(ep - sl), 2)
    qty   = max(1, int(risk / rps))
    pval  = round(qty * ep)
    brok  = 40
    stt   = round(pval * 0.001)
    nse   = round(pval * 0.0000345)
    gst   = round((brok + nse) * 0.18)
    stamp = round(pval * 0.00015)
    tc    = brok + stt + nse + gst + stamp
    m     = 1 if dr == "LONG" else -1
    t1    = round(ep + m*rps*1.5, 2)
    t2    = round(ep + m*rps*2.5, 2)
    t3    = round(ep + m*rps*4.0, 2)
    return jsonify({
        "symbol": sym, "direction": dr, "capital": cap,
        "risk_inr": risk, "risk_per": rps, "qty": qty, "pos_val": pval,
        "charges": {"brokerage": brok, "stt": stt, "nse": nse, "gst": gst, "stamp": stamp, "total": tc},
        "targets": {"t1": t1, "t2": t2, "t3": t3},
        "pnl_t1":  round(risk*1.5*0.35 - tc),
        "pnl_t2":  round(risk*1.5*0.35 + risk*2.5*0.35 - tc),
        "pnl_t3":  round(risk*1.5*0.35 + risk*2.5*0.35 + risk*4.0*0.30 - tc),
    })

@app.route("/api/signal-log")
def signal_log():
    conn = get_db()
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM signal_log ORDER BY signal_date DESC, score DESC LIMIT 500"
    ).fetchall()]
    conn.close()
    grouped = {}
    for r in rows:
        d = r["signal_date"]
        if d not in grouped: grouped[d] = []
        try: r["filters"] = json.loads(r["filters"])
        except Exception: r["filters"] = []
        grouped[d].append(r)
    return jsonify({"log": grouped, "dates": sorted(grouped.keys(), reverse=True)})

@app.route("/api/trades", methods=["GET", "POST"])
def trades_route():
    if request.method == "POST":
        d    = request.json or {}
        ep   = float(d.get("entry_price", 0))
        xp   = float(d.get("exit_price",  0))
        qty  = int(d.get("qty", 0))
        dr   = d.get("direction", "LONG")
        sl   = float(d.get("stop_loss", 0))
        pval = round(qty * xp)
        chrg = 40 + round(pval*0.001) + round(pval*0.0000345) + \
               round((40 + round(pval*0.0000345))*0.18) + round(pval*0.00015)
        gross = (xp - ep) * qty * (1 if dr == "LONG" else -1)
        net   = round(gross - chrg)
        rps   = abs(ep - sl)
        ar    = round(net / (qty * rps), 2) if rps > 0 and qty > 0 else 0
        hold  = 0
        try:
            ed   = datetime.datetime.strptime(d.get("exit_date",  ""), "%Y-%m-%d")
            nd   = datetime.datetime.strptime(d.get("entry_date", ""), "%Y-%m-%d")
            hold = (ed - nd).days
        except Exception:
            pass
        conn = get_db()
        conn.execute(
            "INSERT INTO trades(symbol,sector,direction,entry_date,exit_date,"
            "entry_price,exit_price,stop_loss,qty,risk_inr,charges,net_pnl,actual_r,"
            "exit_type,score,hold_days,notes,capital_before,capital_after) VALUES"
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (d.get("symbol",""), d.get("sector",""), dr, d.get("entry_date",""),
             d.get("exit_date",""), ep, xp, sl, qty, d.get("risk_inr",0),
             chrg, net, ar, d.get("exit_type",""), d.get("score",0),
             hold, d.get("notes",""), d.get("capital_before",0), d.get("capital_after",0))
        )
        conn.commit(); conn.close()
        return jsonify({"ok": True, "net_pnl": net, "actual_r": ar, "charges": chrg})

    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM trades ORDER BY entry_date DESC").fetchall()]
    conn.close()
    W  = [t for t in rows if t["net_pnl"] > 0]
    L  = [t for t in rows if t["net_pnl"] <= 0]
    tw = sum(t["net_pnl"] for t in W)
    tl = abs(sum(t["net_pnl"] for t in L)) or 1
    return jsonify({
        "trades": rows,
        "stats": {
            "total_pnl":     round(sum(t["net_pnl"] for t in rows)),
            "win_rate":      round(len(W)/len(rows)*100, 1) if rows else 0,
            "profit_factor": round(tw/tl, 2),
            "total":         len(rows),
            "wins":          len(W),
            "losses":        len(L),
            "total_charges": round(sum(t.get("charges",0) for t in rows)),
        }
    })

@app.route("/api/trades/<int:tid>", methods=["DELETE"])
def del_trade(tid):
    conn = get_db()
    conn.execute("DELETE FROM trades WHERE id=?", (tid,))
    conn.commit(); conn.close()
    return jsonify({"ok": True})

@app.route("/api/swing-trades")
def swing_trades():
    """Advanced swing trades with filtering, sorting, and accuracy metrics"""
    trade_type = request.args.get("type", "SWING")  # SWING, INTRA, ALL
    min_score = int(request.args.get("min_score", 0))
    direction = request.args.get("direction", "")  # LONG, SHORT
    limit = int(request.args.get("limit", 50))
    sort = request.args.get("sort", "date_desc")  # date_desc, date_asc, pnl_desc, r_desc
    
    conn = get_db()
    
    # Build query
    query = "SELECT * FROM trades WHERE 1=1"
    params = []
    if trade_type != "ALL":
        query += " AND trade_type = ?"
        params.append(trade_type)
    if min_score > 0:
        query += " AND score >= ?"
        params.append(min_score)
    if direction:
        query += " AND direction = ?"
        params.append(direction)
    
    # Sort
    if sort == "date_desc":
        query += " ORDER BY entry_date DESC"
    elif sort == "date_asc":
        query += " ORDER BY entry_date ASC"
    elif sort == "pnl_desc":
        query += " ORDER BY net_pnl DESC"
    elif sort == "r_desc":
        query += " ORDER BY actual_r DESC"
    else:
        query += " ORDER BY entry_date DESC"
    
    if limit > 0:
        query += f" LIMIT {limit}"
    
    rows = [dict(r) for r in conn.execute(query, params).fetchall()]
    conn.close()
    
    # Calculate advanced metrics
    W = [t for t in rows if t.get("net_pnl", 0) > 0]
    L = [t for t in rows if t.get("net_pnl", 0) <= 0]
    
    # Accuracy by score
    score_groups = {}
    for t in rows:
        sc = t.get("score", 0)
        if sc not in score_groups: score_groups[sc] = {"wins": 0, "total": 0}
        score_groups[sc]["total"] += 1
        if t.get("net_pnl", 0) > 0: score_groups[sc]["wins"] += 1
    
    accuracy_by_score = {}
    for sc, g in score_groups.items():
        accuracy_by_score[sc] = round(g["wins"] / g["total"] * 100, 1) if g["total"] > 0 else 0
    
    # Monthly stats
    monthly = {}
    for t in rows:
        if t.get("entry_date"):
            month = t["entry_date"][:7]
            if month not in monthly: monthly[month] = {"pnl": 0, "trades": 0, "wins": 0}
            monthly[month]["trades"] += 1
            monthly[month]["pnl"] += t.get("net_pnl", 0)
            if t.get("net_pnl", 0) > 0: monthly[month]["wins"] += 1
    
    # Sector performance
    sector_stats = {}
    for t in rows:
        sec = t.get("sector", "Unknown")
        if sec not in sector_stats: sector_stats[sec] = {"pnl": 0, "trades": 0, "wins": 0}
        sector_stats[sec]["trades"] += 1
        sector_stats[sec]["pnl"] += t.get("net_pnl", 0)
        if t.get("net_pnl", 0) > 0: sector_stats[sec]["wins"] += 1
    
    # Average R per score
    avg_r_by_score = {}
    for sc, g in score_groups.items():
        rs = [t.get("actual_r", 0) for t in rows if t.get("score", 0) == sc]
        avg_r_by_score[sc] = round(sum(rs) / len(rs), 2) if rs else 0
    
    tw = sum(t.get("net_pnl", 0) for t in W)
    tl = abs(sum(t.get("net_pnl", 0) for t in L)) or 1
    
    return jsonify({
        "trades": rows,
        "count": len(rows),
        "stats": {
            "total_pnl": round(sum(t.get("net_pnl", 0) for t in rows)),
            "win_rate": round(len(W) / len(rows) * 100, 1) if rows else 0,
            "profit_factor": round(tw / tl, 2) if tl > 0 else 0,
            "total_trades": len(rows),
            "wins": len(W),
            "losses": len(L),
            "avg_win": round(sum(t.get("net_pnl", 0) for t in W) / len(W), 0) if W else 0,
            "avg_loss": round(abs(sum(t.get("net_pnl", 0) for t in L) / len(L)), 0) if L else 0,
        },
        "accuracy_by_score": accuracy_by_score,
        "avg_r_by_score": avg_r_by_score,
        "monthly": monthly,
        "sector_performance": sector_stats
    })

if __name__ == "__main__":
    # Load .env.txt if .env doesn't exist
    if not os.path.exists(os.path.join(BASE, "../.env")):
        env_txt = os.path.join(BASE, "../.env.txt")
        if os.path.exists(env_txt):
            print(f"[ENV] Loading from {env_txt}")
            with open(env_txt) as f:
                for line in f:
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        os.environ[k] = v

    print("\n" + "="*55)
    print("  Trade Smart v7 — Trading Intelligence System")
    print("  Real-time data from ZERODHA API")
    print("  No numpy / No pandas required!")
    print(f"  Python {sys.version.split()[0]}")
    cfg = gcfg()
    has_zerodha = bool(os.environ.get("KITE_API_KEY")) and bool(os.environ.get("KITE_ACCESS_TOKEN"))
    if has_zerodha:
        print("  OK Zerodha: CONNECTED -> LIVE NSE DATA")
    else:
        print("  WARN Zerodha: NOT CONNECTED")
    print(f"  Data mode: {'REAL (Zerodha)' if cfg.get('use_real') else 'SYNTHETIC'}")
    print("  http://localhost:5000")
    print("="*55 + "\n")
    # If Zerodha streaming is enabled, run the WS connection on the main thread
    # (avoids Twisted signal-handler issues on Windows), while Flask runs in a worker thread.
    use_stream = cfg.get("use_zerodha_ltp", True) and bool(os.environ.get("KITE_API_KEY")) and bool(os.environ.get("KITE_ACCESS_TOKEN"))
    
    # Start background scanner
    # _start_background_scanner()  # Moved inside _maybe_start_zerodha_ltp_stream for better sync
    
    if use_stream:
        print("  Zerodha LTP streaming: START")
        flask_thread = threading.Thread(
            target=lambda: app.run(host="0.0.0.0", port=5000, debug=False, threaded=True, use_reloader=False),
            daemon=True,
        )
        flask_thread.start()
        # The background scanner will be started by _maybe_start_zerodha_ltp_stream once ready
        _maybe_start_zerodha_ltp_stream(run_in_main_thread=True)
    else:
        _start_background_scanner()
        app.run(host="0.0.0.0", port=5000, debug=False, threaded=True, use_reloader=False)
