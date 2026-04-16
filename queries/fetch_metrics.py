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

import json
import os
import sys
from datetime import datetime, timezone

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
SELECT
  DATE_TRUNC(date, WEEK(MONDAY))   AS week_start,
  SUM(volume_usd) / 1e9            AS total_dex_volume_bn
FROM `cs-host-1e442ec0baa34148b93f88.historical_volumes.daily_perps_volume`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_WEEKS} WEEK)
GROUP BY 1
ORDER BY 1
"""

SQL_DYDX_VOLUME = f"""
-- dYdX weekly notional trading volume
-- Source: dydx-ce5e3.numia.fills
-- Note: each fill records one side; use ABS to avoid double-counting if needed
SELECT
  DATE_TRUNC(block_timestamp, WEEK(MONDAY))   AS week_start,
  SUM(size * price) / 1e9                     AS dydx_volume_bn
FROM `dydx-ce5e3.numia.fills`
WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_WEEKS * 7} DAY)
  AND side = 'BUY'   -- count one side only to avoid double-counting
GROUP BY 1
ORDER BY 1
"""

SQL_FEES = f"""
-- dYdX weekly gross and net trading fees
-- Gross = total taker_fee collected
-- Net   = taker_fee minus maker_rebate (what the protocol retains)
-- Source: numia-data.dydx_mainnet.dydx_match
SELECT
  DATE_TRUNC(block_timestamp, WEEK(MONDAY))     AS week_start,
  SUM(taker_fee)  / 1e6                         AS gross_fees_usd,
  SUM(taker_fee - COALESCE(maker_rebate, 0)) / 1e6 AS net_fees_usd
FROM `numia-data.dydx_mainnet.dydx_match`
WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_WEEKS * 7} DAY)
GROUP BY 1
ORDER BY 1
"""

SQL_TOKEN_HOLDERS = f"""
-- Weekly DYDX token holder count
-- Source: dydx-ce5e3.numia.token_holder_snapshots
-- ⚠ Update table/column names below if your holder data lives in a different table.
--   If this query fails, holders will be omitted from the JSON (null values).
SELECT
  DATE_TRUNC(snapshot_date, WEEK(MONDAY))   AS week_start,
  MAX(holder_count)                         AS token_holders
FROM `dydx-ce5e3.numia.token_holder_snapshots`
WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_WEEKS} WEEK)
GROUP BY 1
ORDER BY 1
"""

SQL_STAKED_TOKENS = f"""
-- Total DYDX tokens staked per week (latest snapshot per validator per week)
-- Source: numia-data.dydx_mainnet.dydx_validators
SELECT
  week_start,
  SUM(tokens) / 1e6   AS staked_dydx_m   -- millions of DYDX
FROM (
  SELECT
    DATE_TRUNC(DATE(block_timestamp), WEEK(MONDAY))   AS week_start,
    operator_address,
    tokens,
    ROW_NUMBER() OVER (
      PARTITION BY
        DATE_TRUNC(DATE(block_timestamp), WEEK(MONDAY)),
        operator_address
      ORDER BY block_timestamp DESC
    ) AS rn
  FROM `numia-data.dydx_mainnet.dydx_validators`
  WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_WEEKS * 7} DAY)
)
WHERE rn = 1
GROUP BY 1
ORDER BY 1
"""

SQL_ACTIVE_STAKERS = f"""
-- Weekly count of unique active DYDX stakers (wallets with tokens > 0)
-- Source: dydx-ce5e3.numia.staked_snapshots_with_last_timestamp
SELECT
  DATE_TRUNC(snapshot_date, WEEK(MONDAY))   AS week_start,
  COUNT(DISTINCT delegator_address)         AS active_stakers
FROM `dydx-ce5e3.numia.staked_snapshots_with_last_timestamp`
WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_WEEKS} WEEK)
  AND tokens > 0
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
    holders_rows     = run_query(client, SQL_TOKEN_HOLDERS,     "Token holders")
    staked_rows      = run_query(client, SQL_STAKED_TOKENS,     "Staked DYDX")
    stakers_rows     = run_query(client, SQL_ACTIVE_STAKERS,    "Active stakers")

    # Build the canonical week spine from the total DEX data (most complete source)
    spine: list[str] = [str(r["week_start"]) for r in total_dex_rows]
    if not spine:
        print("\n❌ ERROR: total DEX query returned no rows. Check table access and permissions.")
        sys.exit(1)

    print(f"\nWeek range: {spine[0]} → {spine[-1]}")

    # Align all series to the spine
    total_dex_vol  = align(total_dex_rows,  "week_start", "total_dex_volume_bn", spine)
    dydx_vol       = align(dydx_vol_rows,   "week_start", "dydx_volume_bn",      spine)
    gross_fees     = align(fees_rows,        "week_start", "gross_fees_usd",      spine)
    net_fees       = align(fees_rows,        "week_start", "net_fees_usd",        spine)
    token_holders  = align(holders_rows,    "week_start", "token_holders",       spine)
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
