# testing/crypto mm/data/resample.py

import os
import pandas as pd
from glob import glob

# Paths
DIR = os.path.join("testing", "crypto mm")
RAW_DIR = os.path.join("testing", "crypto mm", "data", "raw")
PROCESSED_DIR = os.path.join("testing", "crypto mm", "data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)


def process_and_save_daily_basis():
    """
    For each day's raw BitMEX and Binance data,
    resample to 1-second intervals, compute futures-spot basis,
    and save each day's result as a separate CSV in PROCESSED_DIR.
    Returns a list of saved file paths.
    """
    bitmex_files = sorted(glob(os.path.join(RAW_DIR, 'bitmex_XBTUSD_*.csv')))
    binance_files = sorted(glob(os.path.join(RAW_DIR, 'binance_*.csv')))

    if len(bitmex_files) != len(binance_files):
        raise ValueError("Mismatch in number of BitMEX and Binance files")

    saved_paths = []

    for bmx_path, bnc_path in zip(bitmex_files, binance_files):
        # Extract date string YYYYMMDD from filename
        date_str = os.path.basename(bmx_path).split('_')[-1].split('.')[0]

        # Load and resample BitMEX futures
        df_bmx = pd.read_csv(bmx_path)
        # Parse BitMEX timestamps: replace 'D' delimiter with space
        df_bmx['timestamp'] = pd.to_datetime(df_bmx['timestamp'].str.replace('D', ' '), format='%Y-%m-%d %H:%M:%S.%f', utc=True)
        df_bmx.set_index('timestamp', inplace=True)
        futures = df_bmx['price'].resample('1s').last().ffill()
        futures.name = 'futures_price'

        # Load and resample Binance spot
        df_bnc = pd.read_csv(bnc_path)
        # Parse Binance timestamps stored as ISO strings
        df_bnc['timestamp'] = pd.to_datetime(df_bnc['timestamp'], utc=True)
        df_bnc.set_index('timestamp', inplace=True)
        df_bnc['price'] = df_bnc['p'].astype(float)
        spot = df_bnc['price'].resample('1s').last().ffill()
        spot.name = 'spot_price'

        # Combine and compute basis
        daily_df = pd.concat([futures, spot], axis=1).dropna()
        daily_df['basis'] = daily_df['futures_price'] - daily_df['spot_price']

        # Save to CSV per date
        out_path = os.path.join(PROCESSED_DIR, f"basis_{date_str}.csv")
        daily_df.to_csv(out_path)
        saved_paths.append(out_path)
        print(f"Saved daily basis for {date_str} to {out_path}")

    return saved_paths


def load_full_basis() -> pd.DataFrame:
    """
    Load all per-day basis CSVs from PROCESSED_DIR into one DataFrame.
    """
    files = sorted(glob(os.path.join(PROCESSED_DIR, 'basis_*.csv')))
    dfs = []
    for f in files:
        df = pd.read_csv(f, parse_dates=['timestamp'], index_col='timestamp')
        dfs.append(df)
    if dfs:
        return pd.concat(dfs)
    else:
        return pd.DataFrame()



# Process and save each day's basis
saved = process_and_save_daily_basis()
print(f"\nProcessed and saved {len(saved)} files to {PROCESSED_DIR}")
# Optionally, load the full basis
full_basis = load_full_basis()
print(f"Loaded full basis with {len(full_basis)} rows")

# Plot histogram
import matplotlib.pyplot as plt
plt.figure()
plt.hist(full_basis['basis'].values, bins=500)
plt.title('Histogram of Futures-Spot Basis')
plt.xlabel('Basis (USD)')
plt.ylabel('Frequency')
plt.tight_layout()
plt.savefig(f'{DIR}/basis histogram.png')