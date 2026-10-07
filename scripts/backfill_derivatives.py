import io
import time
import boto3
import hashlib
import zipfile
import traceback
import pandas as pd
import xml.etree.ElementTree as ET
import sys
from datetime import timedelta, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from utils import upsert_to_s3, calculate_period_ms, make_session
from endpoints import ENDPOINTS as API_ENDPOINTS 

aws_lambda = boto3.client("lambda", region_name="ap-southeast-1")
s3 = boto3.client("s3", region_name="ap-southeast-1")

lambda_function = "binance-futures-collector"
BUCKET_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
BASE_URL = "https://data.binance.vision/data/futures/um/daily/metrics"

def get_variables(lambda_function):
    """
    Fetch symbols and S3_bucket name from environment variables in lambda function
    """
    config = aws_lambda.get_function_configuration(FunctionName=lambda_function)
    symbols = config["Environment"]["Variables"]["SYMBOLS"].split(',')
    s3_bucket = config["Environment"]["Variables"]["S3_BUCKET"]
    s3_prefix = config["Environment"]["Variables"]["S3_PREFIX"]
    period = config["Environment"]["Variables"]["PERIOD"]
    return symbols, s3_bucket, s3_prefix, period

def get_oldest_archive_date(symbol, session):
    """
    Return the date of the oldest daily metrics file for a symbol,
    or None if the archive has no files for it.
    """
    prefix = f"data/futures/um/daily/metrics/{symbol}/"

    params = {"delimiter": "/", "prefix": prefix}
    response = session.get(BUCKET_URL, params=params, timeout=30)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    keys = [key.text for key in root.findall("s3:Contents/s3:Key", S3_NS)]

    try:
        oldest_key = min(keys).split('/')[-1].split('.')[0]
        oldest_date = oldest_key.split('-metrics-')[-1]
        oldest_date = date.fromisoformat(oldest_date)
    except ValueError:
        return None

    return oldest_date

def get_oldest_S3_file(endpoint, symbol, s3_bucket, s3_prefix):
    response = s3.list_objects_v2(
        Bucket=s3_bucket, 
        Prefix=f"{s3_prefix}/endpoint={endpoint}/symbol={symbol}/"
        )
    if "Contents" in response:
        min_year, key = None, None
        for obj in response["Contents"]:
            if not obj['Key'].endswith('/data.parquet'):
                continue
            year = obj['Key'].split('/')[-2].split('=')[-1]
            if min_year is None or year < min_year:
                min_year = year 
                key = obj['Key']
        return key
    else:
        return None

def download(session, url):
    """
    Return the response for a URL, or None if the file doesn't exist (404).
    Any other bad status raises, because that's unexpected (server error, etc.).
    """
    response = session.get(url, timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response

def verify_checksum(zip_bytes, checksum_text, expected_filename):
    """
    True if the SHA-256 of zip_bytes matches the hash in the .CHECKSUM file.
    """
    # The checksum file is one line: "<hash>  <filename>"
    parts = checksum_text.split()
    expected_hash, checksum_filename = parts[0], parts[1]
 
    # Guard against comparing with the wrong day's checksum file.
    if checksum_filename != expected_filename:
        raise ValueError(
            f"Checksum is for {checksum_filename}, expected {expected_filename}"
        )
 
    actual_hash = hashlib.sha256(zip_bytes).hexdigest()
    return actual_hash.lower() == expected_hash.lower()

def fetch_verified_zip(session, symbol, day):
    """
    Download one day's zip and verify it.
 
    Returns:
        bytes -> the verified zip
        None  -> the file doesn't exist for this day (404)
    Raises:
        RuntimeError if the checksum still fails after MAX_ATTEMPTS.
    """
    MAX_ATTEMPTS = 3

    zip_url = f"{BASE_URL}/{symbol}/{symbol}-metrics-{day.isoformat()}.zip"
    checksum_url = f"{zip_url}.CHECKSUM"
    expected_filename = zip_url.split("/")[-1]

    checksum_response = download(session, checksum_url)
    if checksum_response is None:
        return None
    checksum_text = checksum_response.text

    for attempt in range(1, MAX_ATTEMPTS + 1):
        zip_response = download(session, zip_url)
        if zip_response is None:
            return None

        zip_bytes = zip_response.content  # bytes, not .text
        if verify_checksum(zip_bytes, checksum_text, expected_filename):
            return zip_bytes

        print(f"Checksum mismatch for {expected_filename} (attempt {attempt})")
        time.sleep(2)
 
    raise RuntimeError(
        f"Checksum failed {MAX_ATTEMPTS} times for {expected_filename}"
    )

def unzip_to_df(zip_bytes, expected_csv_name):
    """
    Unzip the bytes and return a DataFrame for the expected CSV file.
    Raises if the CSV isn't found in the zip.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        if expected_csv_name not in zf.namelist():
            raise ValueError(f"{expected_csv_name} not found in zip")
        with zf.open(expected_csv_name) as csv_file:
            return pd.read_csv(csv_file)

def read_parquet_from_s3(bucket, key):
    """
    Read a Parquet file from S3 into a DataFrame.
    Returns None if the file doesn't exist.
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    data = obj["Body"].read()
    return pd.read_parquet(io.BytesIO(data))

ARCHIVE_COLUMNS = {
    "openInterestHist": {
        "sum_open_interest": "sumOpenInterest",
        "sum_open_interest_value": "sumOpenInterestValue",
    },
    "topLongShortAccountRatio": {
        "count_toptrader_long_short_ratio": "longShortRatio",
    },
    "topLongShortPositionRatio": {
        "sum_toptrader_long_short_ratio": "longShortRatio",
    },
    "globalLongShortAccountRatio": {
        "count_long_short_ratio": "longShortRatio",
    },
}

SCHEMAS = {ep["name"]: ep["schema"] for ep in API_ENDPOINTS}

if __name__ == "__main__":
    symbols, s3_bucket, s3_prefix, period = get_variables(lambda_function)
    try:
        interval_ms = calculate_period_ms(period)
    except Exception as e:
        raise ValueError(f"Error calculating interval_ms for period '{period}': {e}")

    with make_session() as session:
        failed = []  
        for symbol in symbols:
            try:
                start_date = get_oldest_archive_date(symbol, session)
                if start_date is None:
                    print(f"No archive found for {symbol}")
                    continue

                oldest_ts = {}
                for endpoint in ARCHIVE_COLUMNS:
                    key = get_oldest_S3_file(endpoint, symbol, s3_bucket, s3_prefix)
                    if key is None:
                        continue
                    oldest_ts[endpoint] = read_parquet_from_s3(s3_bucket, key)["timestamp"].min()

                if not oldest_ts:
                    print(f"{symbol}: no S3 files for any endpoint, skipping")
                    continue
                end_date = pd.to_datetime(max(oldest_ts.values()), unit="ms", utc=True).date()

                if start_date > end_date:
                    print(f"{symbol}: S3 already covers the archive range, nothing to backfill")
                    continue

                frames = []
                missing_days = []
                current = start_date
                print(f"{symbol} start date: {start_date}")
                print(f"{symbol} end date: {end_date}")

                while current <= end_date:
                    zip_bytes = fetch_verified_zip(session, symbol, current)
                    if zip_bytes is None:
                        missing_days.append(current)
                    else:
                        expected_csv_name = f"{symbol}-metrics-{current.isoformat()}.csv"
                        df = unzip_to_df(zip_bytes, expected_csv_name)
                        frames.append(df)
                    time.sleep(0.3)
                    current += timedelta(days=1)

                print(f"{symbol} missing days: {len(missing_days)} {missing_days[:10]}")

                if not frames:
                    print(f"No data found for {symbol} in the range {start_date} to {end_date}")
                    continue

                day_df = pd.concat(frames, ignore_index=True)
                day_df = day_df.drop_duplicates(subset=["create_time", "symbol"])
                day_df = day_df.sort_values(by="create_time", ascending=True)
                day_df["create_time"] = pd.to_datetime(day_df["create_time"], utc=True)
                day_df["timestamp"] = day_df["create_time"].dt.as_unit("ms").astype("int64")

                archive_df = day_df[day_df["timestamp"] % interval_ms == 0]

                for endpoint, mapping in ARCHIVE_COLUMNS.items():
                    endpoint_df = archive_df[["symbol", "timestamp", *mapping]].rename(columns=mapping)
                    key = get_oldest_S3_file(endpoint, symbol, s3_bucket, s3_prefix)
                    if key is None:
                        print(f"{symbol}: no S3 files for {endpoint}, skipping")
                        continue

                    existing = read_parquet_from_s3(s3_bucket, key)
                    endpoint_df = endpoint_df[endpoint_df["timestamp"] < oldest_ts[endpoint]]

                    endpoint_schema = SCHEMAS[endpoint]
                    endpoint_df = endpoint_df.reindex(columns=endpoint_schema.keys())

                    for col, (dtype, _) in endpoint_schema.items():
                        endpoint_df[col] = endpoint_df[col].astype(dtype)

                    if endpoint_df.empty:
                        print(f"{symbol}:{endpoint}: nothing before cutoff, skipping")
                        continue

                    shared = list(endpoint_df.columns)
                    assert all(c in existing.columns for c in shared), shared
                    assert (endpoint_df[shared].dtypes == existing[shared].dtypes).all(), (
                            endpoint_df[shared].dtypes, existing[shared].dtypes
                            )
                    
                    endpoint_df = endpoint_df.copy()
                    endpoint_df["_year"] = pd.to_datetime(endpoint_df["timestamp"], unit="ms", utc=True).dt.year
                    for year, group in sorted(endpoint_df.groupby("_year"), reverse=True):
                        df_group = group.drop(columns=["_year"]).sort_values("timestamp")
                        file_key = f"{s3_prefix}/endpoint={endpoint}/symbol={symbol}/year={year}/data.parquet"
                        total = upsert_to_s3(s3, s3_bucket, file_key, df_group, "timestamp")

                        first = pd.to_datetime(df_group["timestamp"].min(), unit="ms", utc=True)
                        last = pd.to_datetime(df_group["timestamp"].max(), unit="ms", utc=True)
                        print(f"{symbol} {endpoint} {year}: wrote {len(df_group)} rows "
                            f"({first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M}), file now {total} rows")
            except Exception as e:
                print(f"{symbol}: FAILED — {e}")
                traceback.print_exc()
                failed.append(symbol)

    print(f"Failed symbols: {failed}")  







