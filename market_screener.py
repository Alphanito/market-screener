"""
Market Screener — data pipeline for the delivery/fundamentals dashboard
=========================================================================

Run this locally (not inside the dashboard) to refresh the numbers.
It produces `screener_data.js`, which `dashboard.html` loads directly —
so the workflow is: run script -> screener_data.js updates -> reload
the dashboard in your browser.

WHY IT WORKS THIS WAY (read this before assuming the dashboard should
just "fetch live" on its own):
  1. NSE actively blocks scripted requests without browser-like session
     cookies, and blocks cross-origin JS calls from a page entirely —
     a static HTML file cannot pull NSE data by itself from inside the
     browser. It has to happen from a script like this one, run on your
     machine.
  2. Fundamentals (Sales / Trade Receivables) aren't in NSE or BSE's
     market-data feeds at all — those live in quarterly financial
     statements. This script pulls them from yfinance, which is free
     but patchy for small/micro-caps. Expect gaps for the smallest names.
  3. BSE-side delivery data is not reliably scriptable any more (the
     old public endpoints are largely dead/undocumented). This version
     is NSE-only. If combining NSE+BSE delivery matters a lot to you,
     that needs a paid data vendor (or manual BSE download merged by ISIN)
     — flag it and we can add that path.

Metrics computed, per symbol:
  - Net Delivery % of Market Cap  = (DELIV_QTY x Close) / Market Cap x 100
  - Price-to-Sales                = Market Cap / TTM Revenue
  - Sales-to-Market-Cap %         = TTM Revenue / Market Cap x 100
  - Trade Receivables % of Sales  = Trade Receivables / TTM Revenue x 100
    (lower generally = healthier collections; very high can flag
     revenue quality issues)

Install: pip install pandas requests yfinance
"""

import json
import time
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests

try:
    import yfinance as yf
except ImportError:
    raise SystemExit("Missing dependency: pip install yfinance")


NSE_HOME = "https://www.nseindia.com"
BHAVDATA_URL_TMPL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

DELIVERY_THRESHOLD_PCT = 1.0   # headline screen: net delivery >= 1% of market cap
SLEEP_BETWEEN_CALLS = 0.35     # be polite to yfinance


def get_nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get(NSE_HOME, timeout=10)
    return session


def fetch_delivery_bhavcopy(date_ddmmyyyy: str, session: requests.Session) -> pd.DataFrame:
    dt = pd.to_datetime(date_ddmmyyyy, format="%d-%m-%Y")
    url = BHAVDATA_URL_TMPL.format(ddmmyyyy=dt.strftime("%d%m%Y"))
    resp = session.get(url, timeout=20)
    if resp.status_code != 200 or b"<html" in resp.content[:200].lower():
        raise RuntimeError(f"Could not fetch bhavcopy for {date_ddmmyyyy} (url: {url}).")

    df = pd.read_csv(BytesIO(resp.content))
    df.columns = [c.strip() for c in df.columns]
    df["SERIES"] = df["SERIES"].astype(str).str.strip()
    df = df[df["SERIES"] == "EQ"].copy()

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip()
    df["CLOSE_PRICE"] = pd.to_numeric(df["CLOSE_PRICE"], errors="coerce")
    df["PREV_CLOSE"] = pd.to_numeric(df.get("PREV_CLOSE"), errors="coerce")
    df["DELIV_QTY"] = pd.to_numeric(df["DELIV_QTY"], errors="coerce")
    df["DELIVERY_VALUE"] = df["DELIV_QTY"] * df["CLOSE_PRICE"]
    df["PCT_CHANGE"] = (df["CLOSE_PRICE"] - df["PREV_CLOSE"]) / df["PREV_CLOSE"] * 100

    return df[["SYMBOL", "CLOSE_PRICE", "PREV_CLOSE", "PCT_CHANGE", "DELIV_QTY", "DELIVERY_VALUE"]].dropna(
        subset=["SYMBOL", "CLOSE_PRICE", "DELIV_QTY", "DELIVERY_VALUE"]
    )


def get_fundamentals(symbol: str) -> dict:
    """Market cap, TTM revenue, trade receivables, and sector/industry via yfinance."""
    out = {"market_cap": None, "revenue": None, "receivables": None,
           "sector": None, "industry": None}
    try:
        t = yf.Ticker(f"{symbol}.NS")

        # .info is the only place yfinance exposes sector/industry; it's a
        # slower call than fast_info, which is why this whole pipeline
        # takes a while for a full market scan. Keep --limit small while testing.
        try:
            info = t.info
            out["sector"] = info.get("sector")
            out["industry"] = info.get("industry")
        except Exception:
            pass

        fi = t.fast_info
        out["market_cap"] = float(fi.get("market_cap")) if fi.get("market_cap") else None

        try:
            income = t.get_income_stmt(freq="trailing")
            if income is not None and not income.empty and "TotalRevenue" in income.index:
                out["revenue"] = float(income.loc["TotalRevenue"].iloc[0])
        except Exception:
            pass

        try:
            bs = t.get_balance_sheet(freq="quarterly")
            if bs is not None and not bs.empty:
                for key in ("Receivables", "AccountsReceivable", "GrossAccountsReceivable"):
                    if key in bs.index:
                        out["receivables"] = float(bs.loc[key].iloc[0])
                        break
        except Exception:
            pass
    except Exception:
        pass
    return out


def screen(date_ddmmyyyy: str, delivery_threshold_pct: float = DELIVERY_THRESHOLD_PCT,
           limit: int | None = None) -> dict:
    session = get_nse_session()
    delivery_df = fetch_delivery_bhavcopy(date_ddmmyyyy, session)

    if limit:
        delivery_df = delivery_df.head(limit)

    rows = []
    skipped = []

    for _, row in delivery_df.iterrows():
        symbol = row["SYMBOL"]
        fnd = get_fundamentals(symbol)
        time.sleep(SLEEP_BETWEEN_CALLS)

        mcap = fnd["market_cap"]
        if not mcap:
            skipped.append(symbol)
            continue

        net_delivery_pct = row["DELIVERY_VALUE"] / mcap * 100

        price_to_sales = None
        sales_to_mcap_pct = None
        receivables_pct_sales = None

        if fnd["revenue"]:
            price_to_sales = mcap / fnd["revenue"]
            sales_to_mcap_pct = fnd["revenue"] / mcap * 100
            if fnd["receivables"]:
                receivables_pct_sales = fnd["receivables"] / fnd["revenue"] * 100

        rows.append({
            "symbol": symbol,
            "close": round(row["CLOSE_PRICE"], 2),
            "pctChange": round(row["PCT_CHANGE"], 2) if pd.notna(row.get("PCT_CHANGE")) else None,
            "sector": fnd["sector"] or "Unclassified",
            "industry": fnd["industry"],
            "deliveryQty": int(row["DELIV_QTY"]),
            "deliveryValue": round(row["DELIVERY_VALUE"], 0),
            "marketCap": round(mcap, 0),
            "netDeliveryPct": round(net_delivery_pct, 3),
            "priceToSales": round(price_to_sales, 2) if price_to_sales else None,
            "salesToMcapPct": round(sales_to_mcap_pct, 2) if sales_to_mcap_pct else None,
            "receivablesPctSales": round(receivables_pct_sales, 2) if receivables_pct_sales else None,
            "flagged": net_delivery_pct >= delivery_threshold_pct,
        })

    return {
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "tradingDate": date_ddmmyyyy,
        "threshold": delivery_threshold_pct,
        "rows": rows,
        "skippedSymbols": skipped,
        "isSampleData": False,
    }


def find_latest_trading_date(max_lookback_days: int = 10) -> str:
    """
    Walk backward from today (IST-ish; good enough for a daily cron) until
    a date with a published bhavcopy is found. Handles weekends/holidays
    automatically so a scheduled job doesn't need a human to pick the date.
    """
    session = get_nse_session()
    d = datetime.now()
    for _ in range(max_lookback_days):
        candidate = d.strftime("%d-%m-%Y")
        try:
            fetch_delivery_bhavcopy(candidate, session)
            return candidate
        except Exception:
            d = d - pd.Timedelta(days=1)
    raise RuntimeError(f"No bhavcopy found in the last {max_lookback_days} days.")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None,
                         help="Trading date DD-MM-YYYY. Omit to auto-detect the most recent trading day "
                              "(use this for scheduled/cron runs).")
    parser.add_argument("--threshold", type=float, default=DELIVERY_THRESHOLD_PCT)
    parser.add_argument("--limit", type=int, default=None,
                         help="Cap number of symbols processed (useful for a fast test run)")
    parser.add_argument("--out", default="screener_data.js")
    args = parser.parse_args()

    date = args.date or find_latest_trading_date()
    print(f"Fetching NSE delivery data for {date} ...")
    data = screen(date, args.threshold, args.limit)

    with open(args.out, "w") as f:
        f.write("// Auto-generated by market_screener.py — do not edit by hand\n")
        f.write("const SCREENER_DATA = ")
        json.dump(data, f, indent=2)
        f.write(";\n")

    flagged = sum(1 for r in data["rows"] if r["flagged"])
    print(f"Done. {len(data['rows'])} symbols processed, {flagged} flagged >= {args.threshold}% "
          f"net delivery, {len(data['skippedSymbols'])} skipped (no market cap).")
    print(f"Wrote {args.out} — reload dashboard.html to see it.")


if __name__ == "__main__":
    main()
