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

SQL_STAKED_TOKENS = f"""
-- Total DYDX tokens staked per week (latest snapshot per bonded validator per week).
-- Source: numia-data.dydx_mainnet.dydx_validators
-- tokens column is a STRING in adydx units (1 DYDX = 1e18 adydx).
-- Only BOND_STATUS_BONDED validators are counted (active set).
-- Dividing by 1e18 converts adydx → DYDX; then /1e6 gives millions of DYDX.
SELECT
  DATE_TRUNC(DATE(TIMESTAMP(snapshot_time), 'UTC'), WEEK(MONDAY))   AS week_start,
  SUM(CAST(tokens AS NUMERIC) / 1e18) / 1e6                         AS staked_dydx_m
FROM (
  SELECT
    snapshot_time,
    operator_address,
    tokens,
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE_TRUNC(DATE(TIMESTAMP(snapshot_time), 'UTC'), WEEK(MONDAY)),
        operator_address
      ORDER BY snapshot_time DESC
    ) AS rn
  FROM `numia-data.dydx_mainnet.dydx_validators`
  WHERE status = 'BOND_STATUS_BONDED'
    AND snapshot_time >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {LOOKBACK_WEEKS * 7} DAY)
)
WHERE rn = 1
GROUP BY 1
ORDER BY 1
"""

SQL_ACTIVE_STAKERS = f"""
-- Weekly count of unique active DYDX stakers (addresses with staked_balance > 0).
-- Column names (ds, address, staked_balance) confirmed from Mode query.
-- Using numia-data.dydx_mainnet project (service account has access there).
-- dydx-ce5e3.numia version of this table returns 403 Access Denied.
SELECT
  DATE_TRUNC(DATE(ds, 'UTC'), WEEK(MONDAY))                                   AS week_start,
  COUNT(DISTINCT CASE WHEN staked_balance > 0 THEN address ELSE NULL END)     AS active_stakers
FROM `numia-data.dydx_mainnet.staked_snapshots_with_last_timestamp`
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

    Accepts comma- or tab-separated files with a header row.
    Expected columns (any order):
      date     — YYYY-MM-DD  OR  M/D/YY  (e.g. 1/1/25)
      holders  — integer holder count

    Daily rows are aggregated to Monday-based weeks by taking the MAX
    holder count within each week. Missing weeks return None.
    """
    if not os.path.exists(csv_path):
        print(f"  ⚠ token_holders.csv not found at {csv_path} — holders will be null")
        return [None] * len(spine)

    DATE_FMTS = ["%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y"]

    def parse_date(s: str) -> date | None:
        for fmt in DATE_FMTS:
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    weekly: dict[str, int] = {}
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            # Auto-detect delimiter (comma or tab) from whole sample, not just header
            sample = f.read(2048); f.seek(0)
            delimiter = "\t" if "\t" in sample else ","
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                raw_date = row.get("date", "").strip()
                raw_val  = row.get("holders", "").strip().replace(",", "")
                if not raw_date or not raw_val:
                    continue
                d = parse_date(raw_date)
                if d is None:
                    continue
                # Snap to Monday of the week
                monday = (d - timedelta(days=d.weekday())).isoformat()
                val = int(float(raw_val))
                weekly[monday] = max(weekly.get(monday, 0), val)
    except Exception as exc:
        print(f"  ⚠ Could not read token_holders.csv: {exc}")
        return [None] * len(spine)

    print(f"  → Token holders (CSV) … {len(weekly)} weeks loaded")
    return [weekly.get(w) for w in spine]


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
    total_dex_rows   = run_query(client, SQL_TOTAL_DEX_VOLUME,  "Total DEX volume")
    dydx_vol_rows    = run_query(client, SQL_DYDX_VOLUME,       "dYdX volume")
    fees_rows        = run_query(client, SQL_FEES,              "Trading fees")
    staked_rows      = run_query(client, SQL_STAKED_TOKENS,     "Staked DYDX")
    stakers_rows     = run_query(client, SQL_ACTIVE_STAKERS,    "Active stakers")

    # Build the canonical week spine from the current date — independent of any
    # data source so the time axis always extends to last Monday regardless of
    # upstream table lag. Data sources with lag will produce nulls for recent weeks.
    spine: list[str] = build_week_spine(LOOKBACK_WEEKS)
    if not dydx_vol_rows and not total_dex_rows:
        print("\n❌ ERROR: both primary queries returned no rows. Check table access and permissions.")
        sys.exit(1)

    print(f"\nWeek range: {spine[0]} → {spine[-1]}")

    # Align all series to the spine
    total_dex_vol  = align(total_dex_rows,  "week_start", "total_dex_volume_bn", spine)
    dydx_vol       = align(dydx_vol_rows,   "week_start", "dydx_volume_bn",      spine)
    gross_fees     = align(fees_rows,        "week_start", "gross_fees_usd",      spine)
    net_fees       = align(fees_rows,        "week_start", "net_fees_usd",        spine)
    token_holders  = load_token_holders_csv(TOKEN_HOLDERS_CSV, spine)
    staked_dydx    = align(staked_rows,     "week_start", "staked_dydx_m",       spine)
    active_stakers = align(stakers_rows,    "week_start", "active_stakers",      spine)

    # Derived series
    other_dex_vol  = [
        round(t - d, 4) if t is not None and d is not None else None
        for t, d in zip(total_dex_vol, dydx_vol)
    ]
    market_share_pct = [
        round(d / t * 100, 4) if d is not None and t and t > 0 else None
        for d, t in zip(dydx_vol, total_dex_vol)
    ]

    # KPI latest-week snapshot
    kpi = {
        "week":                    spine[-1],
        "dydx_volume_bn":          dydx_vol[-1],
        "dydx_volume_wow_pct":     wow_pct(dydx_vol),
        "total_dex_volume_bn":     total_dex_vol[-1],
        "total_dex_wow_pct":       wow_pct(total_dex_vol),
        "market_share_pct":        market_share_pct[-1],
        "market_share_wow_pp":     wow_pp(market_share_pct),
        "gross_fees_usd":          gross_fees[-1],
        "gross_fees_wow_pct":      wow_pct(gross_fees),
        "net_fees_usd":            net_fees[-1],
        "net_fees_wow_pct":        wow_pct(net_fees),
        "token_holders":           token_holders[-1],
        "token_holders_wow_pct":   wow_pct(token_holders),
        "staked_dydx_m":           staked_dydx[-1],
        "staked_dydx_wow_pct":     wow_pct(staked_dydx),
        "active_stakers":          active_stakers[-1],
        "active_stakers_wow_pct":  wow_pct(active_stakers),
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
