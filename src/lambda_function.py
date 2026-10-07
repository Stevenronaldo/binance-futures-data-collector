import os
import json
import time
import math
import boto3
import pandas as pd
from utils import calculate_period_ms, upsert_to_s3, make_session
from endpoints import ENDPOINTS

# Read from Lambda environment variables
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "binance-futures")
SYMBOLS = [s.strip() for s in os.environ["SYMBOLS"].split(",") if s.strip()]
PERIOD = os.environ.get("PERIOD", "1h")
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

# boto3 client created once at module level — reused across warm invocations
s3 = boto3.client("s3")
sns = boto3.client("sns")

session = make_session()

def clean_df(df, time_field, schema):
    """Helper: deduplicate and convert dtype according to schema"""
    df = df.drop_duplicates(subset=time_field, keep='last')

    missing = set(schema) - set(df.columns)
    if missing:
        raise ValueError(f"missing columns: {missing}")
    
    for col, (dtype, nullable) in schema.items():
        if nullable:
            df[col] = df[col].replace("", None)

        if dtype == "string":
            df[col] = df[col].astype("string")
        else:
            df[col] = pd.to_numeric(df[col], errors='raise').astype(dtype)

        if not nullable and df[col].isnull().any():
            raise ValueError(f"column {col} has null values but is not nullable")
    df = df[list(schema)]
    return df

def get_json(session, url, params=None):
    response = session.get(url, params=params, timeout=10)

    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code} - {response.text[:200]}")
    
    return response.json()

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

def fetch_derivative(session, url, symbol, period, startTime=None, limit=500):
    """
    fetch derivative data from binance API [/futures/data/]
    Return data as dataframe
    """
    period_ms = calculate_period_ms(period)
    now_ms = int(time.time() * 1000)
    earliest_allowed_start = now_ms - 30 * 24 * 60 * 60 * 1000 + period_ms

    if startTime is None:
        startTime = earliest_allowed_start
    
    # Clamp startTime to the earliest allowed value
    startTime = max(startTime, earliest_allowed_start)

    record = []
    print(f"----Fetching {symbol}_{period}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    page_limit = limit
    
    # Page by fixed time windows using endTime only:
    # - Binance ignores startTime alone (returns newest rows), and rejects any time
    #   older than ~30 days with HTTP 400 (-1130) -> startTime is clamped above.
    # - Binance returns "latest N rows <= endTime", so a page can include rows from
    #   before the window (harmless, deduped in upsert).
    # - Next window starts at endTime + 1, never at data[-1], so the loop always
    #   moves forward even on empty/short pages (no infinite loop).
    # - The only stop condition is the window reaching now.
    while True:
        endTime = startTime + period_ms * limit
        if endTime > now_ms:
              endTime = now_ms
              # ceil, not int: a window like 218.99 periods can still hold 219 rows;
              # int() would drop the oldest row -> permanent gap.
              page_limit = math.ceil((endTime - startTime)/period_ms)
              if page_limit < 1:
                  break

        params = {'symbol': symbol, 'period': period, 'limit': page_limit, 'endTime': endTime}
        data = get_json(session, url, params=params)

        if data:
            record.extend(data)
            print(
                f"Fetched {len(record)} records so far... (up to {pd.to_datetime(data[-1]['timestamp'], unit='ms').date()})")

        startTime = endTime + 1
        time.sleep(0.3)

    if record:
        df = pd.DataFrame(record)
        return df
    else:
        return pd.DataFrame()

def fetch_fundingrate(session, url, symbol, period=None, startTime=None, limit=1000):
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
        data = get_json(session, url, params=params)

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
        return df
    else:
        return pd.DataFrame()

def fetch_kline_like(session, url, symbol, period=None, startTime=None, limit=1500):
    """
    fetch kline like data from binance API [/fapi/v1/klines] & [/fapi/v1/indexPriceKlines]
    Return data as dataframe
    """
    KLINES_LIST = ['open_time', 'open', 'high', 'low', 'close', 'volume',
                      'close_time', 'quote_volume', 'num_trades',
                      'taker_buy_base', 'taker_buy_quote', 'ignore']

    INDEX_PRICE_KLINES_LIST = ['open_time', 'open', 'high', 'low', 'close', 
                               'ignore', 'close_time', 'ignore', 'ignore',
                                'ignore', 'ignore', 'ignore']
    
    if startTime is None:
        startTime = 1567641600000  #2019-09-05 (Binance futures launch)

    endpoint = url.split('/')[-1]
    if endpoint == "klines":
        columns_name, symbol_param = KLINES_LIST, "symbol"
    elif endpoint == "indexPriceKlines":
        columns_name, symbol_param = INDEX_PRICE_KLINES_LIST, "pair"
    else:
        raise ValueError(f"Unknown endpoint: {endpoint}")
    
    print(f"----Fetching {symbol}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    
    record = []
    while True:
        params = {symbol_param : symbol, 'interval': period, 'startTime': startTime, 'limit': limit}
        data = get_json(session, url, params=params)

        if not data:
            break

        record.extend(data)
        print(f"  Fetched {len(record)} records so far... (up to {pd.to_datetime(data[-1][0], unit='ms').date()})")

        if len(data) < limit:
            break

        startTime = data[-1][0] + 1
        time.sleep(0.3)

    if record:
        df = pd.DataFrame(record, columns=columns_name)
        df = df.drop(columns=['ignore'])
        now_ms = int(time.time() * 1000)
        df = df[df['close_time'] <= now_ms].copy()
    else:
        return pd.DataFrame()

    return df

def read_watermark(bucket, key):
    """
    read watermark of endpoint from S3 (JSON file)
    """
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read())
    except s3.exceptions.NoSuchKey:
        return None

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

# Maps the "fetcher" key of each entry in endpoints.ENDPOINTS to its function
FETCHERS = {
    "derivative":  fetch_derivative,
    "fundingrate": fetch_fundingrate,
    "fetch_kline_like": fetch_kline_like
}

def fetch_process(session, url, symbol, fetch_func, time_field, period, schema):
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

        df = fetch_func(session, url, symbol, period=period, startTime=startTime)

        extra_cols = [c for c in df.columns if c not in schema]
        if extra_cols:
            quality_issues.append(f"[{symbol}-{endpoint}] Quality: extra columns {extra_cols}")

        if not df.empty:
            df = clean_df(df, time_field= time_field, schema=schema)

        period_ms = calculate_period_ms(period)
        prev_ts = current_watermark["last_startTime"] if current_watermark else None
        try:
            quality_issues  += quality_check(endpoint, symbol, df, time_field, period_ms, prev_ts)
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
            total += upsert_to_s3(s3, S3_BUCKET, file_key, df_group, time_field)
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
        for ep in ENDPOINTS:
            name, url, time_field, schema = ep["name"], ep["url"], ep["time_field"], ep["schema"]
            fetch_func = FETCHERS[ep["fetcher"]]
            period = PERIOD if ep["use_period"] else None
            if context.get_remaining_time_in_millis() < 30_000:
                print("Time budget exhausted — stopping cleanly")
                stopped_early = True
                all_error_issues.append(f"[{symbol}-{name}] Error: stopped early, remaining {context.get_remaining_time_in_millis()} ms")
                break
            summary[symbol][name], quality_issues, error_issues = fetch_process(session, url, symbol, fetch_func, time_field, period, schema)
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