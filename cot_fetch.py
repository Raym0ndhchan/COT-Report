# cot_fetch.py
# Fetches Disaggregated Futures-Only COT (current year ZIP), selects the latest report week,
# saves date-stamped CSV/Parquet into data/YYYY/, and refreshes a "latest" file for that year.

import io, zipfile, re, os
from pathlib import Path
from datetime import datetime
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import zoneinfo

HIST_URL_PATTERN = "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"
OUT_DIR = Path(os.environ.get("OUT_DIR", "data")).resolve()
YEARS_BACK = int(os.environ.get("YEARS_BACK", "0"))  # 0 = current year only
WRITE_PARQUET = True  # change to False if you don't want parquet

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/csv, text/plain;q=0.9, */*;q=0.8",
}

class HttpError(Exception): pass

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.8, min=1, max=10),
    retry=retry_if_exception_type(HttpError),
)
def http_get_bytes(url: str, timeout=90) -> bytes:
    r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
    if r.status_code >= 400:
        raise HttpError(f"{r.status_code} for {url}")
    return r.content

def read_csv_keep_schema(b: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(b), low_memory=False)

def fetch_year(year: int) -> pd.DataFrame:
    url = HIST_URL_PATTERN.format(year=year)
    print(f"[FETCH] {year} ← {url}")
    zbytes = http_get_bytes(url, timeout=90)
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        names = zf.namelist()
        if not names:
            raise RuntimeError("Zip archive empty.")
        member = next((n for n in names if n.lower().endswith((".txt", ".csv"))), names[0])
        print(f"[FETCH] {year} → reading: {member}")
        with zf.open(member) as f:
            b = f.read()
    df = read_csv_keep_schema(b)
    print(f"[FETCH] {year} ✔ rows={len(df):,} cols={len(df.columns)}")
    return df

def find_date_col(df: pd.DataFrame) -> str:
    # Robust match for the official date column
    candidates = ["Report_Date_as_YYYY-MM-DD", "Report_Date_as_YYYY_MM_DD", "As_of_Date_In_Form_YYMMDD"]
    def norm(s): return re.sub(r"[^0-9a-z]+", "", s.lower())
    cmap = {norm(c): c for c in df.columns}
    for cand in candidates:
        k = norm(cand)
        if k in cmap:
            return cmap[k]
    for c in df.columns:
        if "report" in c.lower() and "date" in c.lower():
            return c
    raise RuntimeError("Could not locate a report date column.")

def main():
    ny_now = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    print(f"[INFO] New York time now: {ny_now:%Y-%m-%d %H:%M:%S}")
    years = [ny_now.year - i for i in range(YEARS_BACK, -1, -1)]
    frames = [fetch_year(y) for y in years]
    df_all = pd.concat(frames, ignore_index=True)

    date_col = find_date_col(df_all)
    dates = pd.to_datetime(df_all[date_col], errors="coerce")
    latest_date = dates.max()
    if pd.isna(latest_date):
        raise RuntimeError("All report dates are NaT; cannot proceed.")
    print(f"[RUN] Latest report_date: {latest_date.date()}")

    latest_week = df_all.loc[dates == latest_date].copy()

    # Save into data/YYYY/
    year_dir = OUT_DIR / str(latest_date.year)
    year_dir.mkdir(parents=True, exist_ok=True)

    stamp = latest_date.strftime("%Y-%m-%d")
    hist_base = year_dir / f"cot_disagg_fut_{stamp}"
    latest_base = year_dir / f"cot_disagg_fut_latest_{latest_date.year}"

    # Date-stamped history file
    latest_week.to_csv(hist_base.with_suffix(".csv"), index=False)
    print(f"[SAVE] CSV: {hist_base.with_suffix('.csv')}")
    if WRITE_PARQUET:
        latest_week.to_parquet(hist_base.with_suffix(".parquet"), index=False)
        print(f"[SAVE] Parquet: {hist_base.with_suffix('.parquet')}")

    # Convenience "latest" file (overwritten each run)
    latest_week.to_csv(latest_base.with_suffix(".csv"), index=False)
    print(f"[SAVE] CSV: {latest_base.with_suffix('.csv')}")
    if WRITE_PARQUET:
        latest_week.to_parquet(latest_base.with_suffix(".parquet"), index=False)
        print(f"[SAVE] Parquet: {latest_base.with_suffix('.parquet')}")

    print("[DONE] Success.")

if __name__ == "__main__":
    main()
