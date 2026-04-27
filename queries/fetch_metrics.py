"""
dYdX Metrics Dashboard — BigQuery data fetcher
================================================
Queries Numia / dYdX BigQuery tables and writes data/metrics.json,
which is consumed by dydx_metrics_dashboard.html.

USAGE
-----
  Local:   python queries/fetch_metrics.py
  CI:      Called automatically by .github/workflows/refresh_data.yml

AUTHENTICATION
--------------
  Option A — Key file on disk (local dev):
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

  Option B — Raw JSON in env var (GitHub Actions):
    Set the secret GCP_SA_KEY to the full JSON content of a service account
    key that has BigQuery Data Viewer on all datasets below.

BIGQUERY DATASETS USED
-----------------------
  cs-host-1e442ec0baa34148b93f88.historical_volumes   → total market volumes
  dydx-ce5e3.numia                                     → fills, staked snapshots
  numia-data.dydx_mainnet                              → match (fees), validators

DEPENDENCIES
------------
  pip install google-cloud-bigquery db-dtypes
"""

import csv
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery
from google.oauth2 import service_account

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

LOOKBACK_WEEKS = 52   # 12 months of weekly data

# ─────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────

def get_client() -> bigquery.Client:
    """Return an authenticated BigQuery client."""
    raw = os.environ.get("GCP_SA_KEY")
    if raw:
        info = json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(credentials=creds, project=info["project_id"])
    # Falls back to GOOGLE_APPLICATION_CREDENTIALS / Application Default Credentials
    return bigquery.Client()

# ─────────────────────────────────────────────────────────────
# SQL Queries
# ─────────────────────────────────────────────────────────────

SQL_TOTAL_DEX_VOLUME = f"""
-- Total perpetual DEX market weekly volume (all venues combined)
-- Source: cs-host-1e442ec0baa34148b93f88.historical_volumes.daily_perps_volume
-- HAVING COUNT = 7 ensures only fully-populated weeks are included — this handles
-- both the current partial week and any lagged weeks where the source table is
-- not yet fully loaded (e.g. the last 1-2 rows may have incomplete daily data).
SELECT
  DATE_TRUNC(date, WEEK(MONDAY))   AS week_start,
  SUM(total_volume) / 1e9          AS total_dex_volume_bn
FROM `cs-host-1e442ec0baa34148b93f88.historical_volumes.daily_perps_volume`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_WEEKS} WEEK)
GROUP BY 1
HAVING COUNT(DISTINCT date) = 7
ORDER BY 1
"""

SQL_DYDX_VOLUME = f"""
-- dYdX weekly notional trading volume
-- Source: dydx-ce5e3.numia.fills
-- Uses volume_usd column directly (matches Mode query approach).
-- Divided by 2 because each trade is recorded for both the maker and taker side.
-- DATE(block_timestamp, 'UTC') is explicit about timezone before WEEK(MONDAY)
-- truncation — without this, implicit timezone conversion returns Sundays instead
-- of Mondays as week_start, misaligning with the Python-generated spine.
SELECT
  DATE_TRUNC(DATE(block_timestamp, 'UTC'), WEEK(MONDAY))   AS week_start,
  SUM(volume_usd) / 2 / 1e9                                AS dydx_volume_bn
FROM `dydx-ce5e3.numia.fills`
WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_WEEKS * 7} DAY)
GROUP BY 1
ORDER BY 1
"""

SQL_FEES = f"""
-- dYdX weekly gross and net trading fees, in millions of USD.
-- Columns are in quote quantums where 1 USDC = 1e6 quantums.
-- Dividing by 1e12 converts quantums → millions of USD (÷1e6 for USDC, ÷1e6 for millions).
-- Gross = taker fees collected; Net = taker + maker (maker_order_fee is negative when a rebate).
-- DATE(..., 'UTC') prevents implicit timezone shift that causes Sunday week_starts.
-- Source: numia-data.dydx_mainnet.dydx_match
SELECT
  DATE_TRUNC(DATE(block_timestamp, 'UTC'), WEEK(MONDAY))               AS week_start,
  SUM(taker_order_fee_quote_quantums) / 1e12                           AS gross_fees_usd,
  (SUM(taker_order_fee_quote_quantums)
   + SUM(COALESCE(maker_order_fee_quote_quantums, 0))) / 1e12          AS net_fees_usd
FROM `numia-data.dydx_mainnet.dydx_match`
WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_WEEKS * 7} DAY)
GROUP BY 1
ORDER BY 1
"""

# Token holders are sourced from a manually maintained CSV file.
# dydx-ce5e3.numia.token_holder_snapshots returns 403 (no service-account access).
# SmartStake data is downloaded manually and committed to data/token_holders.csv.
# Format: date (YYYY-MM-DD), holders (integer) — one row per day or per week.
TOKEN_HOLDERS_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "token_holders.csv")

# Market share data (total DEX volume, dYdX volume, market share %) is sourced from
# a manually maintained CSV file. DeFiLlama's derivatives API is fully paywalled.
# Format: week_end (Sunday YYYY-MM-DD), total_dex_volume_bn, dydx_volume_bn, market_share_pct
# New rows are added manually each Monday after the previous week closes.
MARKET_SHARE_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "market_share.csv")

# Active stakers data is sourced from a manually maintained CSV file.
# The BigQuery source (numia.staked_snapshots_with_last_timestamp) requires access
# that is not yet granted to the service account. Fill values from the Mode dashboard.
# Format: week_end (Sunday M/D/YY), active_stakers (integer count)
# New rows are added manually each Monday after the previous week closes.
ACTIVE_STAKERS_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "active_stakers.csv")

SQL_STAKED_TOKENS = f"""
-- Total DYDX tokens staked per week (single latest snapshot per week, sum bonded).
-- Source: numia-data.dydx_mainnet.dydx_validators
-- tokens column is a STRING in adydx units (1 DYDX = 1e18 adydx).
-- Only BOND_STATUS_BONDED validators are counted (active set).
-- Dividing by 1e18 converts adydx → DYDX; then /1e6 gives millions of DYDX.
--
-- IMPORTANT: We pick the SINGLE latest snapshot in each week and sum its bonded
-- validators — mirroring the Mode query that groups by snapshot_time. An earlier
-- version used ROW_NUMBER() OVER (PARTITION BY week, operator_address) to pick
-- the latest BONDED snapshot per validator per week, but that double-counts
-- active-set churn: a validator jailed mid-week keeps an early-week BONDED row,
-- AND the validator that replaced them keeps a late-week BONDED row, so both
-- occupants of the same active-set slot land in the same week's sum. That bug
-- inflated the 2026-04-20 week to 237.7M vs. Mintscan's 233.2M.
WITH weekly_latest AS (
  SELECT
    DATE_TRUNC(DATE(TIMESTAMP(snapshot_time), 'UTC'), WEEK(MONDAY)) AS week_start,
    MAX(snapshot_time)                                              AS max_snapshot
  FROM `numia-data.dydx_mainnet.dydx_validators`
  WHERE snapshot_time >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {LOOKBACK_WEEKS * 7} DAY)
  GROUP BY 1
)
SELECT
  w.week_start,
  SUM(CAST(v.tokens AS NUMERIC) / 1e18) / 1e6 AS staked_dydx_m
FROM `numia-data.dydx_mainnet.dydx_validators` v
JOIN weekly_latest w
  ON v.snapshot_time = w.max_snapshot
WHERE v.status = 'BOND_STATUS_BONDED'
GROUP BY 1
ORDER BY 1
"""

SQL_ACTIVE_STAKERS = f"""
-- Weekly count of unique active DYDX stakers.
-- Mirrors the Mode query (dydx-ce5e3.numia.staked_snapshots_with_last_timestamp).
-- ds is a DATE column, no timezone conversion needed.
SELECT
  DATE_TRUNC(ds, WEEK(MONDAY))                                                    AS week_start,
  COUNT(DISTINCT CASE WHEN staked_balance > 0        THEN address ELSE NULL END)  AS active_stakers,
  COUNT(DISTINCT CASE WHEN liquid_staked_balance > 0 THEN address ELSE NULL END)  AS liquid_stakers,
  COUNT(DISTINCT CASE WHEN fe_staked_balance > 0     THEN address ELSE NULL END)  AS fe_stakers
FROM `numia-data.numia.staked_snapshots_with_last_timestamp`
WHERE ds >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_WEEKS} WEEK)
GROUP BY 1
ORDER BY 1
"""

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def run_query(client: bigquery.Client, sql: str, label: str) -> list[dict]:
    """Run a BigQuery query, return list of row dicts. Logs errors but doesn't crash."""
    try:
        print(f"  → {label} …", end=" ", flush=True)
        rows = list(client.query(sql).result())
        print(f"{len(rows)} rows")
        return [dict(r) for r in rows]
    except Exception as exc:
        print(f"FAILED ({exc})")
        return []


def align(rows: list[dict], date_col: str, val_col: str,
          spine: list[str]) -> list[float | None]:
    """Align a query result to a reference week spine, filling gaps with None."""
    lookup = {str(r[date_col]): r[val_col] for r in rows}
    return [
        round(float(lookup[w]), 6) if w in lookup and lookup[w] is not None else None
        for w in spine
    ]


def wow_pct(series: list, idx: int = -1) -> float | None:
    """Week-over-week percentage change."""
    try:
        curr, prev = series[idx], series[idx - 1]
        if curr is None or prev is None or prev == 0:
            return None
        return round((curr - prev) / abs(prev) * 100, 2)
    except (IndexError, TypeError):
        return None


def wow_pp(series: list, idx: int = -1) -> float | None:
    """Week-over-week percentage-point change (for ratios already in %)."""
    try:
        curr, prev = series[idx], series[idx - 1]
        if curr is None or prev is None:
            return None
        return round(curr - prev, 3)
    except (IndexError, TypeError):
        return None


def load_token_holders_csv(csv_path: str, spine: list[str]) -> list[int | None]:
    """Read data/token_holders.csv and align to the week spine.

    Accepts the SmartStake export format ("title","activeAccounts") as well as
    a generic format (date, holders). Comma- or tab-separated, quoted or unquoted.

    Supported date formats: YYYY-MM-DD, M/D/YY, M/D/YYYY.
    Daily rows are aggregated to Monday-based weeks (MAX per week).
    Missing weeks return None.
    """
    if not os.path.exists(csv_path):
        print(f"  ⚠ token_holders.csv not found at {csv_path} — holders will be null")
        return [None] * len(spine)

    # Column name candidates: first match wins
    DATE_COLS    = ["title", "date", "ds", "week"]
    HOLDER_COLS  = ["activeAccounts", "holders", "holder_count", "count"]
    DATE_FMTS    = ["%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y"]

    def parse_date(s: str) -> date | None:
        for fmt in DATE_FMTS:
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except ValueError:
                continue
        return None

    weekly: dict[str, int] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            sample = f.read(4096); f.seek(0)
            delimiter = "\t" if "\t" in sample else ","
            reader = csv.DictReader(f, delimiter=delimiter)

            # Strip any surrounding quotes from header names
            fields = [k.strip('"').strip() for k in (reader.fieldnames or [])]
            date_col   = next((c for c in DATE_COLS   if c in fields), None)
            holder_col = next((c for c in HOLDER_COLS if c in fields), None)

            if not date_col or not holder_col:
                print(f"  ⚠ token_holders.csv: unrecognised columns {fields} — holders will be null")
                return [None] * len(spine)

            for row in reader:
                # Strip quotes that some exporters leave on values
                raw_date = row.get(date_col, "").strip().strip('"')
                raw_val  = row.get(holder_col, "").strip().strip('"').replace(",", "")
                if not raw_date or not raw_val:
                    continue
                d = parse_date(raw_date)
                if d is None:
                    continue
                monday = (d - timedelta(days=d.weekday())).isoformat()
                val = int(float(raw_val))
                weekly[monday] = max(weekly.get(monday, 0), val)

    except Exception as exc:
        print(f"  ⚠ Could not read token_holders.csv: {exc}")
        return [None] * len(spine)

    print(f"  → Token holders (CSV) … {len(weekly)} weeks loaded")
    return [weekly.get(w) for w in spine]


def load_market_share_csv(csv_path: str, spine: list[str]) -> dict[str, list[float | None]]:
    """Read data/market_share.csv and align to the week spine.

    Format: week_end (Sunday), total_dex_volume_bn, dydx_volume_bn, market_share_pct.
    Accepts date formats: YYYY-MM-DD, M/D/YY, M/D/YYYY.
    market_share_pct may include a trailing '%' (e.g. '1.24%') — stripped automatically.
    Column names are stripped of whitespace.
    Week-end (Sunday) dates are converted to Monday by subtracting 6 days to align with spine.
    Returns a dict with keys: total_dex_volume_bn, dydx_volume_bn, market_share_pct.
    """
    empty = {
        "total_dex_volume_bn": [None] * len(spine),
        "dydx_volume_bn":      [None] * len(spine),
        "market_share_pct":    [None] * len(spine),
    }

    if not os.path.exists(csv_path):
        print(f"  ⚠ market_share.csv not found at {csv_path} — market share data will be null")
        return empty

    DATE_FMTS = ["%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y"]

    def parse_sunday(s: str):
        for fmt in DATE_FMTS:
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _float(v: str) -> float | None:
        v = v.strip().rstrip('%')
        try:
            return float(v) if v else None
        except ValueError:
            return None

    rows: dict[str, dict] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            # Normalise header names by stripping whitespace
            reader.fieldnames = [k.strip() for k in (reader.fieldnames or [])]
            for row in reader:
                sunday_str = row.get("week_end", "").strip()
                if not sunday_str:
                    continue
                d = parse_sunday(sunday_str)
                if d is None:
                    continue
                # Convert Sunday → Monday (spine uses Monday week-starts)
                monday = (d - timedelta(days=6)).isoformat()
                rows[monday] = {
                    "total_dex_volume_bn": _float(row.get("total_dex_volume_bn", "")),
                    "dydx_volume_bn":      _float(row.get("dydx_volume_bn", "")),
                    "market_share_pct":    _float(row.get("market_share_pct", "")),
                }
    except Exception as exc:
        print(f"  ⚠ Could not read market_share.csv: {exc}")
        return empty

    filled = sum(1 for v in rows.values() if any(x is not None for x in v.values()))
    print(f"  → Market share (CSV) … {filled} weeks loaded")

    return {
        "total_dex_volume_bn": [rows.get(w, {}).get("total_dex_volume_bn") for w in spine],
        "dydx_volume_bn":      [rows.get(w, {}).get("dydx_volume_bn")      for w in spine],
        "market_share_pct":    [rows.get(w, {}).get("market_share_pct")    for w in spine],
    }


def load_active_stakers_csv(csv_path: str, spine: list[str]) -> list[int | None]:
    """Read data/active_stakers.csv and align to the week spine.

    Format: week_end (Sunday M/D/YY or YYYY-MM-DD), active_stakers (integer).
    Week-end (Sunday) dates are converted to Monday by subtracting 6 days to align with spine.
    Missing or empty values return None.
    """
    if not os.path.exists(csv_path):
        print(f"  ⚠ active_stakers.csv not found at {csv_path} — active stakers will be null")
        return [None] * len(spine)

    DATE_FMTS = ["%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y"]

    def parse_sunday(s: str):
        for fmt in DATE_FMTS:
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except ValueError:
                continue
        return None

    rows: dict[str, int] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [k.strip() for k in (reader.fieldnames or [])]
            for row in reader:
                sunday_str = row.get("week_end", "").strip()
                val_str    = row.get("active_stakers", "").strip().replace(",", "")
                if not sunday_str or not val_str:
                    continue
                d = parse_sunday(sunday_str)
                if d is None:
                    continue
                # Convert Sunday → Monday (spine uses Monday week-starts)
                monday = (d - timedelta(days=6)).isoformat()
                try:
                    rows[monday] = int(float(val_str))
                except ValueError:
                    continue
    except Exception as exc:
        print(f"  ⚠ Could not read active_stakers.csv: {exc}")
        return [None] * len(spine)

    print(f"  → Active stakers (CSV) … {len(rows)} weeks loaded")
    return [rows.get(w) for w in spine]


def build_week_spine(lookback_weeks: int) -> list[str]:
    """Return YYYY-MM-DD strings for the last N Monday-based weeks.

    The spine is generated purely from the current date so it is never limited
    by data availability in any upstream table. Each data source aligns to this
    spine; weeks without data get None (shown as gaps / trend-only in the UI).

    The current (potentially partial) week is included as the final entry so
    that in-progress data is visible. Sources with a HAVING COUNT = 7 filter
    (e.g. total_dex_volume) will naturally return null for the current week.

    Example (today = Thursday 2026-04-16):
      current week start = Monday 2026-04-13  ← included (partial week)
      spine[-1]          = '2026-04-13'
    """
    today = date.today()
    days_since_monday = today.weekday()          # Monday=0, Sunday=6
    current_week_monday = today - timedelta(days=days_since_monday)
    return [
        (current_week_monday - timedelta(weeks=i)).isoformat()
        for i in range(lookback_weeks - 1, -1, -1)
    ]

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("─" * 60)
    print("dYdX Metrics — BigQuery Fetch")
    print(f"Lookback: {LOOKBACK_WEEKS} weeks")
    print("─" * 60)

    client = get_client()
    print("Connected to BigQuery ✓\n")

    print("Running queries:")
    fees_rows        = run_query(client, SQL_FEES,           "Trading fees")
    staked_rows      = run_query(client, SQL_STAKED_TOKENS,  "Staked DYDX")
    # Active stakers are loaded from a manually maintained CSV while BigQuery access
    # to numia.staked_snapshots_with_last_timestamp is being arranged.
    # stakers_rows = run_query(client, SQL_ACTIVE_STAKERS, "Active stakers")

    # Build the canonical week spine from the current date — independent of any
    # data source so the time axis always extends to last Monday regardless of
    # upstream table lag. Data sources with lag will produce nulls for recent weeks.
    spine: list[str] = build_week_spine(LOOKBACK_WEEKS)

    print(f"\nWeek range: {spine[0]} → {spine[-1]}")

    # Align all series to the spine
    mkt            = load_market_share_csv(MARKET_SHARE_CSV, spine)  # manually maintained CSV
    total_dex_vol  = mkt["total_dex_volume_bn"]
    dydx_vol       = mkt["dydx_volume_bn"]
    market_share_pct = mkt["market_share_pct"]
    gross_fees     = align(fees_rows,    "week_start", "gross_fees_usd",  spine)
    net_fees       = align(fees_rows,    "week_start", "net_fees_usd",    spine)
    token_holders  = load_token_holders_csv(TOKEN_HOLDERS_CSV, spine)
    staked_dydx    = align(staked_rows,  "week_start", "staked_dydx_m",  spine)
    active_stakers = load_active_stakers_csv(ACTIVE_STAKERS_CSV, spine)

    # Derived series — other DEX volume = total minus dYdX
    other_dex_vol = [
        round(t - d, 4) if t is not None and d is not None else None
        for t, d in zip(total_dex_vol, dydx_vol)
    ]

    # KPI snapshot — use the last COMPLETE week (spine[-2]), not the current
    # partial week (spine[-1]). The chart shows the partial week visually;
    # the KPI cards should reflect a full 7-day period to avoid misleading drops.
    K = -2
    kpi = {
        "week":                    spine[K],
        "dydx_volume_bn":          dydx_vol[K],
        "dydx_volume_wow_pct":     wow_pct(dydx_vol,       idx=K),
        "total_dex_volume_bn":     total_dex_vol[K],
        "total_dex_wow_pct":       wow_pct(total_dex_vol,  idx=K),
        "market_share_pct":        market_share_pct[K],
        "market_share_wow_pp":     wow_pp(market_share_pct, idx=K),
        "gross_fees_usd":          gross_fees[K],
        "gross_fees_wow_pct":      wow_pct(gross_fees,     idx=K),
        "net_fees_usd":            net_fees[K],
        "net_fees_wow_pct":        wow_pct(net_fees,       idx=K),
        "token_holders":           token_holders[K],
        "token_holders_wow_pct":   wow_pct(token_holders,  idx=K),
        "staked_dydx_m":           staked_dydx[K],
        "staked_dydx_wow_pct":     wow_pct(staked_dydx,   idx=K),
        "active_stakers":          active_stakers[K],
        "active_stakers_wow_pct":  wow_pct(active_stakers, idx=K),
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "weeks":  spine,
        "series": {
            "total_dex_volume_bn":  total_dex_vol,
            "dydx_volume_bn":       dydx_vol,
            "other_dex_volume_bn":  other_dex_vol,
            "market_share_pct":     market_share_pct,
            "gross_fees_usd":       gross_fees,
            "net_fees_usd":         net_fees,
            "token_holders":        token_holders,
            "staked_dydx_m":        staked_dydx,
            "active_stakers":       active_stakers,
        },
        "kpi": kpi,
    }

    out = os.path.join(os.path.dirname(__file__), "..", "data", "metrics.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    print(f"\n✅ Written → {os.path.abspath(out)}")
    print(f"   Latest week: {kpi['week']}")
    if kpi['dydx_volume_bn']:
        print(f"   dYdX volume: ${kpi['dydx_volume_bn']:.2f}B")
    if kpi['market_share_pct']:
        print(f"   Market share: {kpi['market_share_pct']:.2f}%")
    if kpi['net_fees_usd']:
        print(f"   Net fees:    ${kpi['net_fees_usd']:.3f}M")
    print("─" * 60)


if __name__ == "__main__":
    main()
