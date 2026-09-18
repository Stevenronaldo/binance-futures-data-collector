import requests
import io
import os
import json
import time
import boto3
import pandas as pd

# Read from Lambda environment variables
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "binance-futures")
SYMBOLS = [s.strip() for s in os.environ["SYMBOLS"].split(",") if s.strip()]
PERIOD = os.environ.get("PERIOD", "1h")
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

# boto3 client created once at module level — reused across warm invocations
s3 = boto3.client("s3")
sns = boto3.client("sns")

def clean_df(df, time_field):
    """Helper: deduplicate and convert to numeric. Shared by all fetch functions."""
    df = df.drop_duplicates(subset=time_field, keep='last')
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col], errors='raise')
        except (ValueError, TypeError):
            pass  #leave non-numeric columns (like 'symbol') untouched
    return df

def error_check(data, status_code):
    if status_code != 200:
        raise Exception(f"HTTP {status_code} - {data.get('msg', 'no message')}")

def calculate_period_ms(period):
    if period is None:
        return None

    unit = period[-1]        # last character: 'm', 'h', 'd', 'w'
    value = int(period[:-1]) # everything before it: '15', '1', '4'

    unit_ms = {
        'm': 60 * 1000,           # minute
        'h': 60 * 60 * 1000,      # hour
        'd': 24 * 60 * 60 * 1000, # day
        'w': 7 * 24 * 60 * 60 * 1000,  # week
    }
    return value * unit_ms[unit]

def quality_check(endpoint, symbol, df, time_field, period_ms, prev_watermark):
    issues = []

    def round_ms(ts, unit_ms):
        # keep in sync with feature_matrix view
        return (ts + unit_ms // 5) // unit_ms * unit_ms

    if period_ms is None:            # fundingRate: interval can change
        round_unit = 3_600_000         # settlements are always on whole hours
        max_gap = 8 * round_unit        # no interval is longer than 8h
        max_age = 9 * round_unit
    else:
        round_unit = period_ms
        max_gap = period_ms
        max_age = 2 * period_ms

    col = df[time_field] if time_field in df.columns else pd.Series(dtype="int64")
    ts = round_ms(col.dropna().astype("int64"), round_unit)

    if prev_watermark is not None:
        wm = pd.Series([round_ms(int(prev_watermark), round_unit)])
        ts = pd.concat([wm, ts], ignore_index=True)
    ts = ts.sort_values().drop_duplicates().reset_index(drop=True)

    if ts.empty:
        return [f"[{symbol}-{endpoint}] Quality: no data"]

    gaps = ts.diff()
    bad = gaps[gaps.notna() & (gaps > max_gap)]
    for i in bad.index:
        issues.append(f"[{symbol}-{endpoint}] Quality: gap {pd.to_datetime(ts[i-1], unit ='ms')} -> {pd.to_datetime(ts[i], unit ='ms')}")

    now_ms = int(time.time() * 1000)
    if now_ms - ts.iloc[-1] > max_age:
        issues.append(f"[{symbol}-{endpoint}] Quality: stale, latest {pd.to_datetime(ts.iloc[-1], unit ='ms')}")

    return issues


def fetch_derivative(url, symbol, period, startTime=None, limit=500):
    """
    fetch derivative data from binance API [/futures/data/]
    Return data as dataframe
    """
    period_ms = calculate_period_ms(period)

    if startTime is None:
        now_ms = int(time.time() * 1000)
        thirty_days_ms = 30 * 24 * 60 * 60 * 1000
        startTime = now_ms - thirty_days_ms

    record = []
    print(f"----Fetching {symbol}_{period}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    while True:
        endTime = startTime + period_ms * limit
        now_ms = int(time.time() * 1000)
        page_limit = limit
        if endTime > now_ms:
              endTime = now_ms
              page_limit = int((endTime - startTime)/period_ms)
              if page_limit < 1:
                  break

        params = {'symbol': symbol, 'period': period, 'limit': page_limit, 'endTime': endTime}
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        error_check(data, response.status_code)
        if not data:
            break

        record.extend(data)
        print(
            f"Fetched {len(record)} records so far... (up to {pd.to_datetime(data[-1]['timestamp'], unit='ms').date()})")

        if len(data) < limit:
            break

        startTime = data[-1]["timestamp"] + 1
        time.sleep(0.3)

    if record:
        df = pd.DataFrame(record)
        df = clean_df(df, time_field='timestamp')
        return df
    else:
        return pd.DataFrame()

def fetch_fundingrate(url, symbol, period=None, startTime=None, limit=1000):
    """
    fetch fundingrate data from binance API [/fapi/v1/fundingRate]
    Return data as dataframe
    """
    if startTime is None:
        startTime = 1567641600000  #2019-09-05 (Binance futures launch)

    print(f"----Fetching {symbol}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    record = []
    while True:
        params = {"symbol": symbol, "startTime": startTime, "limit": limit}
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        error_check(data, response.status_code)
        if not data:
            break

        record.extend(data)
        print(
            f"Fetched {len(record)} records so far... (up to {pd.to_datetime(data[-1]['fundingTime'], unit='ms').date()})")

        if len(data) < limit:
            break

        startTime = data[-1]["fundingTime"] + 1
        time.sleep(0.3)

    if record:
        df = pd.DataFrame(record)
        df = clean_df(df, time_field='fundingTime')
        return df
    else:
        return pd.DataFrame()

def fetch_klines(url, symbol, period=None, startTime=None, limit=1500):
    """
    fetch kline data from binance API [/fapi/v1/klines]
    Return data as dataframe
    """
    if startTime is None:
        startTime = 1567641600000  #2019-09-05 (Binance futures launch)

    print(f"----Fetching {symbol}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    record = []
    while True:
        params = {'symbol': symbol, 'interval': period, 'startTime': startTime, 'limit': limit}
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        error_check(data, response.status_code)
        if not data:
            break

        record.extend(data)
        print(f"  Fetched {len(record)} records so far... (up to {pd.to_datetime(data[-1][0], unit='ms').date()})")

        if len(data) < limit:
            break

        startTime = data[-1][0] + 1
        time.sleep(0.3)

    klines_columns = ['open_time', 'open', 'high', 'low', 'close', 'volume',
                      'close_time', 'quote_volume', 'num_trades',
                      'taker_buy_base', 'taker_buy_quote', 'ignore']

    if record:
        df = pd.DataFrame(record, columns=klines_columns)
        df = df.drop(columns=['ignore'])
        now_ms = int(time.time() * 1000)
        df = df[df['close_time'] <= now_ms]
        df = clean_df(df, time_field='open_time')
    else:
        return pd.DataFrame()

    return df

def fetch_indexprice(url, symbol, period=None, startTime=None, limit=1500):
    """
    fetch index price klines data from binance API [/fapi/v1/indexPriceKlines]
    Return data as dataframe
    """
    if startTime is None:
        startTime = 1567641600000  #2019-09-05 (Binance futures launch)

    print(f"----Fetching {symbol}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    record = []
    while True:
        params = {'pair': symbol, 'interval': period, 'startTime': startTime, 'limit': limit}
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        error_check(data, response.status_code)
        if not data:
            break

        record.extend(data)
        print(f"  Fetched {len(record)} records so far... (up to {pd.to_datetime(data[-1][0], unit='ms').date()})")

        if len(data) < limit:
            break

        startTime = data[-1][0] + 1
        time.sleep(0.3)

    if record:
        df = pd.DataFrame(record).iloc[:, [0, 1, 2, 3, 4, 6]]
        df.columns = ['open_time', 'open', 'high', 'low', 'close', 'close_time']
        now_ms = int(time.time() * 1000)
        df = df[df['close_time'] <= now_ms]
        df = clean_df(df, time_field='open_time')
        return df
    else:
        return pd.DataFrame()

# check watermark
def read_watermark(bucket, key):
    """
    read watermark of endpoint from S3 (JSON file)
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read())
    except s3.exceptions.NoSuchKey:
        return None

# write watermark
def write_watermark(bucket, key, endpoint, symbol, period, last_startTime):
    """
    write watermark of endpoint to S3 (JSON file)
    each file for each symbol and endpoint
    """
    new_watermark = {
        'symbol': symbol,
        'period': period,
        'endpoint': endpoint,
        'last_startTime': last_startTime,
        'update_time': int(time.time() * 1000),
        'update_time_UTC': pd.Timestamp.now(tz='UTC').isoformat()
    }

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(new_watermark).encode(),
        ContentType='application/json',
    )
    return new_watermark


# upsert df into S3
def upsert_to_s3(bucket, key, new_df, time_field):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        combined = pd.concat([existing, new_df])
        combined = combined.drop_duplicates(subset=time_field, keep="last").sort_values(time_field)

    except s3.exceptions.NoSuchKey:
        combined = new_df  # First write — no existing file

    buf = io.BytesIO()
    combined.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    return len(combined)

ENDPOINTS = [
    ("open_interest",        "https://fapi.binance.com/futures/data/openInterestHist",             fetch_derivative,  "timestamp",   PERIOD),
    ("global_long_short",    "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",  fetch_derivative,  "timestamp",   PERIOD),
    ("top_trader_accounts",  "https://fapi.binance.com/futures/data/topLongShortAccountRatio",     fetch_derivative,  "timestamp",   PERIOD),
    ("top_trader_positions", "https://fapi.binance.com/futures/data/topLongShortPositionRatio",    fetch_derivative,  "timestamp",   PERIOD),
    ("fundingRate",          "https://fapi.binance.com/fapi/v1/fundingRate",                       fetch_fundingrate, "fundingTime", None),
    ("klines",               "https://fapi.binance.com/fapi/v1/klines",                            fetch_klines,      "open_time",   PERIOD),
    ("indexPriceKlines",     "https://fapi.binance.com/fapi/v1/indexPriceKlines",                  fetch_indexprice,  "open_time",   PERIOD),
]

def fetch_process(url, symbol, fetch_func, time_field, period):
    endpoint = url.split('/')[-1]
    watermark_key = f'{S3_PREFIX}/_watermark/{symbol}-{endpoint}-period={period}.json'
    error_issues = []
    quality_issues = []

    try:
        current_watermark = read_watermark(S3_BUCKET, watermark_key)

        if current_watermark is None:
            print(f'---first run {symbol}-{endpoint}-period={period}---')
            startTime = None
        else:
            startTime = current_watermark['last_startTime'] + 1
            print(f'---fetching {symbol}-{endpoint}-period={period} from {pd.to_datetime(startTime, unit="ms")}---')

        df = fetch_func(url, symbol, period=period, startTime=startTime)

        period_ms = calculate_period_ms(period)
        prev_ts = current_watermark["last_startTime"] if current_watermark else None
        try:
            quality_issues = quality_check(endpoint, symbol, df, time_field, period_ms, prev_ts)
        except Exception as e:
            error_issues.append(f"[{symbol}-{endpoint}] Error: quality check failed: {e}")

        if df.empty:
            print(f"[{symbol}-{endpoint}] no new data")
            return {"status": "no_new_data"}, quality_issues, error_issues

        df["_year"] = pd.to_datetime(df[time_field], unit="ms").dt.year
        total = 0
        for year, group in df.groupby("_year"):
            df_group = group.drop(columns=["_year"])
            file_key = f"{S3_PREFIX}/endpoint={endpoint}/symbol={symbol}/year={year}/data.parquet"
            total += upsert_to_s3(S3_BUCKET, file_key, df_group, time_field)
            print(f"[{symbol}-{endpoint}-{year}] wrote {len(df_group)} new rows -> {total} total")

        last_startTime = int(df[time_field].max())
        write_watermark(S3_BUCKET, watermark_key, endpoint, symbol, period, last_startTime)

        return ({
            "status": "ok",
            "new_rows": len(df),
            "total_rows": total,
            "last_startTime": last_startTime,
        }
            , quality_issues, error_issues
        )

    except Exception as e:
        error_issues.append(f"[{symbol}-{endpoint}] FAILED: {e}")
        return {"status": "error", "error": str(e)}, quality_issues, error_issues


# ─── Lambda entry point ──────────────────────────────────────────────────────
def lambda_handler(event, context):
    print(f"Start: symbols={SYMBOLS} period={PERIOD}")
    summary = {}
    stopped_early = False
    all_quality_issues = []
    all_error_issues = []

    for symbol in SYMBOLS:
        summary[symbol] = {}
        for name, url, fetch_func, time_field, period in ENDPOINTS:
            if context.get_remaining_time_in_millis() < 30_000:
                print("Time budget exhausted — stopping cleanly")
                stopped_early = True
                all_error_issues.append(f"[{symbol}-{name}] Error: stopped early, remaining {context.get_remaining_time_in_millis()} ms")
                break
            summary[symbol][name], quality_issues, error_issues = fetch_process(url, symbol, fetch_func, time_field, period)
            all_quality_issues.extend(quality_issues)
            all_error_issues.extend(error_issues)
        if stopped_early:
            break

    if all_quality_issues or all_error_issues:
        sns.publish(TopicArn=SNS_TOPIC_ARN,
                    Subject=f"Binance collector: {len(all_error_issues)} error & {len(all_quality_issues)} quality issues",
                    Message="\n".join(all_error_issues + all_quality_issues)
                    )

    print(f"Done: {summary}")
    return {
        "statusCode": 200,
        "body": json.dumps({"summary": summary, "stopped_early": stopped_early})
    }