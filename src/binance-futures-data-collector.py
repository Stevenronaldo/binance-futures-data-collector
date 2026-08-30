import requests
import io
import os
import json
import time
import boto3
import pandas as pd

# Read from Lambda environment variables
S3_BUCKET = os.environ["S3_BUCKET"]
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
            pass  # leave non-numeric columns (like 'symbol') untouched
    return df

def error_check(data, status_code):
    if status_code != 200:
        error_msg = f"Error: {status_code} - {data.get('msg', 'no message')}"
        print(error_msg)
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="Binance collector — endpoint failure",
                Message= error_msg
            )
        except Exception as e:
            print(f"alert failed: {e}")
        return True
    else:
        return False

def fetch_derivative(url, symbol, period, startTime=None, limit=500):
    """
    fetch derivative data from binance API [/futures/data/]
    Return data as dataframe
    """
    unit = period[-1]        # last character: 'm', 'h', 'd', 'w'
    value = int(period[:-1]) # everything before it: '15', '1', '4'

    unit_ms = {
        'm': 60 * 1000,           # minute
        'h': 60 * 60 * 1000,      # hour
        'd': 24 * 60 * 60 * 1000, # day
        'w': 7 * 24 * 60 * 60 * 1000,  # week
    }
    period_ms = value * unit_ms[unit]

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

        if error_check(data, response.status_code) is True:
            return None
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

        if error_check(data, response.status_code) is True:
            return None
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
        if error_check(data, response.status_code) is True:
            return None
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
        if error_check(data, response.status_code) is True:
            return None
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
    watermark_key = f'binance-futures/_watermark/{symbol}-{endpoint}-period={period}.json'

    try:
        current_watermark = read_watermark(S3_BUCKET, watermark_key)

        if current_watermark is None:
            print(f'---first run {symbol}-{endpoint}-period={period}---')
            startTime = None
        else:
            startTime = current_watermark['last_startTime'] + 1
            print(f'---fetching {symbol}-{endpoint}-period={period} from {pd.to_datetime(startTime, unit="ms")}---')

        df = fetch_func(url, symbol, period=period, startTime=startTime)

        if df is None:
            print(f"[{symbol}-{endpoint}] fetch failed — skipping")
            return {"status": "fetch_failed"}
        if df.empty:
            print(f"[{symbol}-{endpoint}] no new data")
            return {"status": "no_new_data"}

        file_key = f"binance-futures/endpoint={endpoint}/symbol={symbol}/{symbol}-{endpoint}-period={period}.parquet"
        total = upsert_to_s3(S3_BUCKET, file_key, df, time_field)
        print(f"[{symbol}-{endpoint}-{period}] wrote {len(df)} new rows -> {total} total")

        last_startTime = int(df[time_field].iloc[-1])
        write_watermark(S3_BUCKET, watermark_key, endpoint, symbol, period, last_startTime)

        return {
            "status": "ok",
            "new_rows": len(df),
            "total_rows": total,
            "last_startTime": last_startTime,
        }

    except Exception as e:
        print(f"[{symbol}-{endpoint}] FAILED: {e}")
        return {"status": "error", "error": str(e)}


# ─── Lambda entry point ──────────────────────────────────────────────────────
def lambda_handler(event, context):
    print(f"Start: symbols={SYMBOLS} period={PERIOD}")
    summary = {}
    stopped_early = False

    for symbol in SYMBOLS:
        summary[symbol] = {}
        for name, url, fetch_func, time_field, period in ENDPOINTS:
            if context.get_remaining_time_in_millis() < 30_000:
                print("Time budget exhausted — stopping cleanly")
                stopped_early = True
                break
            summary[symbol][name] = fetch_process(url, symbol, fetch_func, time_field, period)
        if stopped_early:
            break

    print(f"Done: {summary}")
    return {
        "statusCode": 200,
        "body": json.dumps({"summary": summary, "stopped_early": stopped_early}),
    }