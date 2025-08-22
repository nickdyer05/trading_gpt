import os
import datetime as dt
import pandas as pd
import duckdb
import yfinance as yf

# Ensure data dir + absolute DB path
PROJECT_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
DB_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.getenv("DB_PATH", os.path.join(DB_DIR, "market.duckdb"))

# Your tickers (edit this one line to change universe)
TICKERS = [s.strip().upper() for s in os.getenv("TICKERS", "ARKK,SPY,STRL,WAY,XBI").split(",") if s.strip()]
EMA_WINDOWS = [8, 21, 50]

def fetch(symbol: str, start: str = "2022-01-01") -> pd.DataFrame:
    print(f"[fetch] {symbol} starting {start}")
    df = yf.download(symbol, start=start, auto_adjust=True, progress=False, group_by="column")
    if df is None or len(df) == 0:
        print(f"[fetch] {symbol} returned empty from yfinance")
        return pd.DataFrame(columns=["dt","open","high","low","close","volume","symbol"])
    try:
        df = df.tz_localize(None)
    except Exception:
        pass
    df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "date" in df.columns:
        df = df.rename(columns={"date":"dt"})
    elif "datetime" in df.columns:
        df = df.rename(columns={"datetime":"dt"})
    required = ["dt","open","high","low","close","volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing expected columns: {missing}. Got columns: {list(df.columns)}")
    df = df[required]
    df["symbol"] = symbol.upper()
    print(f"[fetch] {symbol} rows: {len(df)}")
    return df

def add_emas(df: pd.DataFrame, windows) -> pd.DataFrame:
    if df.empty:
        return df
    for w in windows:
        df[f"ema{w}"] = df["close"].ewm(span=w, adjust=False).mean()
    return df

def upsert_duckdb(df: pd.DataFrame) -> None:
    print(f"[duckdb] connecting to {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            dt TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            symbol TEXT,
            ema8 DOUBLE,
            ema21 DOUBLE,
            ema50 DOUBLE
        )
    """)
    con.execute("CREATE TEMP TABLE stage AS SELECT * FROM df")
    con.execute("""
        DELETE FROM ohlcv USING stage
        WHERE ohlcv.symbol = stage.symbol AND ohlcv.dt = stage.dt
    """)
    con.execute("INSERT INTO ohlcv SELECT * FROM stage")
    con.close()
    print(f"[duckdb] upsert complete")

def main():
    print(f"[config] DB_PATH={DB_PATH}")
    print(f"[config] TICKERS={TICKERS}")
    for sym in TICKERS:
        try:
            d = fetch(sym, start="2022-01-01")
        except Exception as e:
            print(f"[error] fetching {sym}: {e}")
            continue
        if d.empty:
            print(f"[warn] no data for {sym}, skipping")
            continue
        d = add_emas(d, EMA_WINDOWS)
        try:
            upsert_duckdb(d)
            print(f"[ok] {sym}: upserted {len(d)} rows")
        except Exception as e:
            print(f"[error] upserting {sym}: {e}")
    print("[done] ETL complete at", dt.datetime.utcnow(), "UTC")

if __name__ == "__main__":
    main()

