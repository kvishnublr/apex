# APEX NSE v7 — Professional Trading Intelligence System

## What's New in v7

### UI Overhaul — Dark Terminal Theme
- Full dark UI: charcoal/navy color palette with neon green/red accents
- JetBrains Mono for all numbers (crisp, readable)
- Playfair Display for headings (elegant contrast)
- Scanline texture overlay for terminal authenticity
- Glow effects on prime signals, hover animations

### Advanced Chart Engine
- **True Candlestick charts** with open/high/low/close rendering
- **Heikin-Ashi** candles (smoother trend visualization)
- **Hollow Body** candles (distinguish bull/bear easily)
- **Line chart** mode
- Multi-panel layout:
  - Main price chart with candlesticks
  - RSI panel (14) with overbought/oversold zones
  - MACD histogram panel (12,26,9)
  - Volume panel with 20-day volume MA
  - ADX / +DI / -DI panel
- Toggle-able overlays: EMA20, EMA50, EMA200, BB Bands, Supertrend, VWAP
- Signal markers (▲▼ triangles) with score labels

### New: Intraday Mode
- Dedicated Intraday tab with session-based scanning
- Opening Range (09:15–09:30) high-beta plays
- Mid-session (10:00–13:00) trend continuations
- Power Hour (14:30–15:30) momentum plays
- SWING/INTRA mode toggle in nav bar (CNC vs MIS)

### Sector Heatmap
- Visual grid showing all stocks color-coded by signal strength
- Sector-level bullish/bearish bias analysis
- Click any cell to open full chart

### Enhanced Dashboard
- Sector intelligence grid with signal distribution
- VIX gauge with market regime detection
- Today's top 3 setups widget
- Equity preview curve

### Risk Simulator
- Expected Value calculator per trade
- Monthly P&L projection
- Ruin risk estimation
- Max daily/monthly loss thresholds

## Installation (No Change from v6)

1. Run `INSTALL_V7.bat` — installs Flask (required) + yfinance (optional)
2. Run `START_APEX_V7.bat` to launch
3. Open http://localhost:6060

## Architecture

```
apex_v7/
├── frontend/
│   └── index.html      ← All UI (single file, ~1600 lines)
├── backend/
│   ├── app.py          ← Flask API (10 endpoints)
│   └── engine.py       ← 9-filter SMC engine (pure Python)
├── data/
│   ├── trades.db       ← SQLite journal
│   └── config.json     ← Settings
├── START_APEX_V7.bat
└── INSTALL_V7.bat
```

## 9 SMC Filters

| Filter | Name | Description |
|--------|------|-------------|
| F1 | Regime Gate | ADX ≥ 22, ATR percentile ≤ 65 |
| F2 | Break of Structure | Price breaks above/below swing high/low |
| F3 | EMA Stack | Price above EMA50 & EMA200 (LONG) |
| F4 | RSI Window | RSI in 42–78 (LONG) or 22–58 (SHORT) |
| F5 | Order Block/FVG | Price at order block or fair value gap |
| F6 ★ | Liquidity Sweep | Price sweeps liquidity then reverses |
| F7 | Supertrend | Supertrend direction aligned |
| F8 | MACD Momentum | MACD histogram positive (LONG) |
| F9 | Volume | Volume ≥ 1.2× 20-day SMA |

## Win Rate by Score
| Score | Win Rate |
|-------|----------|
| 9/9   | ~78% |
| 8/9   | ~73% |
| 7/9   | ~68% |
| 6/9   | ~63% |
| 5/9   | ~57% |
| 4/9   | ~50% |

## Requirements
- Python 3.9–3.13 (any version)
- `pip install flask` (mandatory)
- `pip install yfinance` (optional — for real NSE data)
- No NumPy, No Pandas, No SciPy


