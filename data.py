# crypto mm/data/data.py

import os
import requests
import gzip
import shutil
import pandas as pd
from datetime import datetime, timedelta
import time

# Updated save path
DATA_DIR = "testing/crypto mm/data/raw"
os.makedirs(DATA_DIR, exist_ok=True)

# Latest available BitMEX dump (YYYYMMDD) until 2025 files are published
BITMEX_LATEST_DATE = "20241231"

# BitMEX S3 endpoint for trade dumps
BITMEX_ENDPOINT = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/{date}.csv.gz"


def bitmex_file_exists(date_str: str) -> bool:
    """
    Check if BitMEX S3 file exists. `date_str` must be YYYYMMDD.
    """
    url = BITMEX_ENDPOINT.format(date=date_str)
    r = requests.head(url)
    return r.status_code == 200


def download_bitmex_xbtusd(date_hyphen: str) -> str:
    """
    Download and filter BitMEX trade data for XBTUSD.
    `date_hyphen` is YYYY-MM-DD.
    Falls back to BITMEX_LATEST_DATE if not yet published.
    Returns the path to the filtered CSV.
    """
    dt_obj = datetime.strptime(date_hyphen, "%Y-%m-%d")
    date_str = dt_obj.strftime("%Y%m%d")

    if not bitmex_file_exists(date_str):
        print(f"BitMEX data for {date_hyphen} not published yet. Falling back to {BITMEX_LATEST_DATE}.")
        date_str = BITMEX_LATEST_DATE

    url = BITMEX_ENDPOINT.format(date=date_str)
    gz_path = os.path.join(DATA_DIR, f"bitmex_{date_str}.csv.gz")
    csv_path = gz_path[:-3]
    filtered_path = os.path.join(DATA_DIR, f"bitmex_XBTUSD_{date_str}.csv")

    if os.path.exists(filtered_path):
        print(f"BitMEX XBTUSD data for {date_str} already exists.")
        return filtered_path

    print(f"Downloading BitMEX dump {date_str}...")
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception(f"Failed to download BitMEX data: HTTP {r.status_code}")

    # Save and decompress
    with open(gz_path, "wb") as f:
        f.write(r.content)
    with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(gz_path)

    # Filter for XBTUSD trades
    print(f"Filtering for XBTUSD trades...")
    df = pd.read_csv(csv_path)
    df = df[df["symbol"] == "XBTUSD"]
    df.to_csv(filtered_path, index=False)
    os.remove(csv_path)

    print(f"Saved filtered data to {filtered_path}")
    return filtered_path


def download_binance_btcusdt(date_hyphen: str) -> str:
    """
    Download Binance aggTrades for BTCUSDT for a single day.
    `date_hyphen` is YYYY-MM-DD.
    Returns the path to the CSV.
    """
    dt_obj = datetime.strptime(date_hyphen, "%Y-%m-%d")
    start = int(dt_obj.timestamp() * 1000)
    end = int((dt_obj + timedelta(days=1)).timestamp() * 1000)

    url = "https://api.binance.com/api/v3/aggTrades"
    symbol = "BTCUSDT"
    trades = []

    print(f"Downloading Binance aggTrades for {date_hyphen}...")

    while start < end:
        params = {"symbol": symbol, "startTime": start, "endTime": end, "limit": 1000}
        r = requests.get(url, params=params)
        if r.status_code != 200:
            raise Exception(f"Binance API error: HTTP {r.status_code}")

        batch = r.json()
        if not batch:
            break

        trades.extend(batch)
        start = batch[-1]["T"] + 1
        time.sleep(0.2)

    df = pd.DataFrame(trades)
    df["timestamp"] = pd.to_datetime(df["T"], unit="ms")
    output_path = os.path.join(DATA_DIR, f"binance_{dt_obj.strftime('%Y%m%d')}.csv")
    df.to_csv(output_path, index=False)

    print(f"Saved Binance data to {output_path}")
    return output_path


def download_data_for_range(start_date_hyphen: str, end_date_hyphen: str):
    """
    Download BitMEX and Binance data for each day in a date range (inclusive).
    """
    start_dt = datetime.strptime(start_date_hyphen, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_hyphen, "%Y-%m-%d")
    current_dt = start_dt

    while current_dt <= end_dt:
        day_str = current_dt.strftime("%Y-%m-%d")
        print(f"\n=== Processing {day_str} ===")
        download_bitmex_xbtusd(day_str)
        download_binance_btcusdt(day_str)
        current_dt += timedelta(days=1)


def main():
    # Full month of January 2025
    start_date = "2025-01-03"
    end_date = "2025-01-31"
    download_data_for_range(start_date, end_date)
    print(f"\nAll done for range: {start_date} to {end_date}")


if __name__ == "__main__":
    main()
