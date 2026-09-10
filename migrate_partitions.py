import io
import os
import boto3
import pandas as pd

BUCKET = os.environ["BUCKET"]
SRC_PREFIX = "binance-futures/"
DST_PREFIX = "binance-futures-v2/"

TIME_FIELDS = {
    "klines":                       "open_time",
    "indexPriceKlines":             "open_time",
    "fundingRate":                  "fundingTime",
    "openInterestHist":             "timestamp",
    "globalLongShortAccountRatio":  "timestamp",
    "topLongShortAccountRatio":     "timestamp",
    "topLongShortPositionRatio":    "timestamp",
}

s3 = boto3.client("s3")

def list_parquet_keys():
    """Yield every .parquet key under SRC_PREFIX, skipping watermarks."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=SRC_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and "_watermark" not in key:
                yield key


def parse_key(key):
    """
    'binance-futures/endpoint=klines/symbol=BTCUSDT/BTCUSDT-klines-period=1h.parquet'
    -> ('klines', 'BTCUSDT')
    """
    parts = key.split("/")
    endpoint = next(p.split("=", 1)[1] for p in parts if p.startswith("endpoint="))
    symbol   = next(p.split("=", 1)[1] for p in parts if p.startswith("symbol="))
    return endpoint, symbol

def write_parquet(bucket, key, df):
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(),
                  ContentType="application/octet-stream")

def migrate_one(key, dry_run=True):
    endpoint, symbol = parse_key(key)
    time_field = TIME_FIELDS[endpoint]

    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    # TODO: derive _year from time_field (UTC), group, drop _year, write each group
    df["_year"] = pd.to_datetime(df[time_field], unit="ms").dt.year
    print(f"  {endpoint}/{symbol}: {dict(df['_year'].value_counts().sort_index())}")
    rows_out = 0
    for year, group in df.groupby("_year"):
        out = group.drop(columns=["_year"])
        if not dry_run:
            file_key = f"{DST_PREFIX}endpoint={endpoint}/symbol={symbol}/year={year}/data.parquet"
            write_parquet(BUCKET, file_key, out)
        rows_out += len(out)

    return len(df), rows_out  # (rows_in, rows_out)

if __name__ == "__main__":
    for key in list_parquet_keys():
        rows_in, rows_out = migrate_one(key, dry_run=False)
        status = "OK" if rows_in == rows_out else "MISMATCH"
        print(f"{status:8} {rows_in:>7} -> {rows_out:>7}  {key}")