import os
import time
import requests
import pandas as pd
from google.cloud import bigquery

# Currencies not supported by Frankfurter API
UNSUPPORTED = {"TWD"}

# Target currency
TARGET_CCY = "EUR"

# Project ID from environment variable (GitHub Secret)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "").strip()
if not PROJECT_ID:
    raise ValueError("Missing env var GCP_PROJECT_ID (GitHub Secret).")

# Dataset and tables (your current BigQuery source)
BQ_DATASET = "patek_data"
SOURCE_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.patek"
DEST_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.fx_rates"

# BigQuery client (auth handled by the environment in GitHub Actions)
client = bigquery.Client(project=PROJECT_ID)


def fx_rate(date: str, base: str, target: str = "EUR") -> float | None:
    """Fetch historical FX rate for a given date and currency pair from Frankfurter."""
    url = f"https://api.frankfurter.app/{date}"
    params = {"from": base, "to": target}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    return data.get("rates", {}).get(target)


def main():
    # --- Debug prints (so you can confirm in GitHub Actions logs) ---
    print("PROJECT_ID:", PROJECT_ID)
    print("SOURCE_TABLE:", SOURCE_TABLE)
    print("DEST_TABLE:", DEST_TABLE)

    # 1) Get unique (date, currency) pairs from BigQuery
    query = f"""
    SELECT DISTINCT
      DATE(life_span_date) AS date,
      currency
    FROM `{SOURCE_TABLE}`
    WHERE life_span_date IS NOT NULL
      AND currency IS NOT NULL
    """
    pairs = client.query(query).to_dataframe()
    print("Pairs fetched:", len(pairs))

    # If nothing to process, stop early (prevents creating an empty table silently)
    if pairs.empty:
        print("No (date, currency) pairs found. Nothing to write.")
        return

    # 2) Call the FX API for each pair
    rows = []
    for i, row in pairs.iterrows():
        date_str = row["date"].strftime("%Y-%m-%d")
        base = row["currency"]

        if base in UNSUPPORTED:
            rate = None
        elif base == TARGET_CCY:
            rate = 1.0
        else:
            try:
                rate = fx_rate(date_str, base, TARGET_CCY)
            except Exception as e:
                print(f"[WARNING] FX failed for {date_str} {base}->{TARGET_CCY}: {e}")
                rate = None

        rows.append(
            {
                "date": date_str,
                "base_currency": base,
                "target_currency": TARGET_CCY,
                "rate": rate,
            }
        )

        # Light throttling to reduce the chance of API rate limits
        if i % 50 == 0 and i > 0:
            time.sleep(0.2)

    df = pd.DataFrame(rows)
    print("Rows to write:", len(df))

    # 3) Load into BigQuery (overwrite each run)
    job = client.load_table_from_dataframe(
        df,
        DEST_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()

    print(f"✅ FX table updated successfully: {DEST_TABLE} ({len(df)} rows)")


if __name__ == "__main__":
    main()
