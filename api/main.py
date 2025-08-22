from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os, io
import duckdb, pandas as pd
from dateutil.relativedelta import relativedelta

# Absolute project paths (work both locally and on Render)
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
DB_PATH = os.getenv("DB_PATH", os.path.join(PROJECT_ROOT, "data", "market.duckdb"))
API_KEY = os.getenv("API_KEY")  # if unset, auth is OFF (handy for testing)

app = FastAPI(title="Trading Data API")

# Optional: serve /charts later if you decide to pre-render
charts_dir = os.path.join(PROJECT_ROOT, "charts")
if os.path.isdir(charts_dir):
    app.mount("/charts", StaticFiles(directory=charts_dir), name="charts")

def require_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# Sanity routes
@app.get("/")
def root():
    return {"ok": True, "paths": ["/docs", "/health", "/quote", "/chart"]}

@app.get("/health")
def health():
    return {"ok": True, "db_path": DB_PATH}

# --- internal helpers ---
def _render_price_with_emas(df: pd.DataFrame, symbol: str) -> bytes:
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.plot(df["dt"], df["close"], label="Close")
    for col in [c for c in df.columns if c.startswith("ema")]:
        ax.plot(df["dt"], df[col], label=col.upper())
    ax.set_title(f"{symbol} — Close with EMAs")
    ax.legend()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    return buf.read()

def _load(symbol: str, period: str):
    s = (period or "6mo").strip().lower()
    n, unit = 6, "mo"
    if s.endswith("mo"):
        n, unit = int(s[:-2]), "mo"
    elif s.endswith("yr") or s.endswith("y"):
        n, unit = int(s[:-2] if s.endswith("yr") else s[:-1]), "yr"

    try:
        con = duckdb.connect(DB_PATH)
        df = con.execute("SELECT * FROM ohlcv WHERE symbol = ? ORDER BY dt", [symbol.upper()]).df()
        con.close()
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df
    end = pd.to_datetime(df["dt"].max())
    start = end - (relativedelta(years=n) if unit == "yr" else relativedelta(months=n))
    return df[df["dt"] >= start]

# --- public endpoints ---
@app.get("/quote", dependencies=[Depends(require_key)])
def quote(symbol: str):
    df = _load(symbol, "1mo")
    if df.empty:
        raise HTTPException(404, "No data — ETL may not have run yet or bad symbol")
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last
    pct = 0.0 if float(prev["close"]) == 0 else (float(last["close"]) - float(prev["close"])) / float(prev["close"]) * 100
    return JSONResponse({
        "symbol": symbol.upper(),
        "dt": str(last["dt"]),
        "close": float(last["close"]),
        "volume": int(last["volume"]),
        "pct_change": round(pct, 3),
    })

@app.get("/chart", dependencies=[Depends(require_key)])
def chart(symbol: str, period: str = "6mo"):
    df = _load(symbol, period)
    if df.empty:
        raise HTTPException(404, "No data — ETL may not have run yet or bad symbol/period")
    png = _render_price_with_emas(df, symbol.upper())
    return StreamingResponse(io.BytesIO(png), media_type="image/png")

