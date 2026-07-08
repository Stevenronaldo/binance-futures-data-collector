import requests
import io
import os
import json
import time
import boto3
import pandas as pd

futuresdata_url = {
    'open_interest'       :'https://fapi.binance.com/futures/data/openInterestHist',
    'global_long_short'   :'https://fapi.binance.com/futures/data/globalLongShortAccountRatio',
    'top_trader_accounts' :'https://fapi.binance.com/futures/data/topLongShortAccountRatio',
    'top_trader_positions':'https://fapi.binance.com/futures/data/topLongShortPositionRatio',
    'taker_volume'        :'https://fapi.binance.com/futures/data/takerlongshortRatio',
    }

# Read from Lambda environment variables
S3_BUCKET = os.environ["S3_BUCKET"]
SYMBOLS   = [s.strip() for s in os.environ["SYMBOLS"].split(",") if s.strip()]
PERIOD    = os.environ.get("PERIOD", "1h")

# boto3 client created once at module level — reused across warm invocations
s3 = boto3.client("s3")

def clean_df(df, time_field, exclude=['symbol']):
    """Helper: deduplicate and convert to numeric. Shared by all fetch functions."""
    df = df.drop_duplicates(subset=time_field, keep='last')
    for col in df.columns:
        if col not in exclude:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def fetch_derivative(url, symbol, period, limit = 500, startTime = None):
    """
    fetch derivative data from binance API [/futures/data/]
    Return data as dataframe
    """
    params = {'symbol' : symbol, 'period' : period, 'limit': limit, 'startTime' : startTime}
    print(f"----Fetching {symbol}_{period}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    response = requests.get(url, params = params, timeout = 10)
    data = response.json()
    if response.status_code == 200:
        df = pd.DataFrame(data)
        df = clean_df(df, time_field = 'timestamp', exclude =['symbol'])
        return df
    else:
      print(f"Error: {response.status_code} - {data.get('msg', 'no message')}")
      return None

def fetch_basis(url, symbol, period, contractType = 'PERPETUAL',  limit = 500, startTime = None):
    """
    fetch basis data from binance API [/futures/data/basis]
    Return data as dataframe
    """
    endTime = int(time.time() * 1000) if startTime is not None else None
    params = {'pair' : symbol, 'contractType': contractType, 'period' : period, 'limit': limit, 'startTime' : startTime, 'endTime': endTime}
    print(f"----Fetching {symbol}_{period}_{contractType}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    response = requests.get(url, params = params, timeout = 10)
    data = response.json()
    if response.status_code == 200:
        df = pd.DataFrame(data)
        df = clean_df(df, time_field = 'timestamp', exclude =['pair', 'contractType'])
        return df
    else:
      print(f"Error: {response.status_code} - {data.get('msg', 'no message')}")
      return None

def fetch_fundingrate(url, symbol, limit = 1000, startTime = 1567641600000): # 2019-09-05 (Binance futures launch)
    """
    fetch fundingrate data from binance API [/fapi/v1/fundingRate]
    Return data as dataframe
    """
    print(f"----Fetching {symbol}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    record = []
    while True:
        params = {"symbol": symbol, "startTime": startTime, "limit": limit}
        response = requests.get(url, params=params, timeout = 10)
        data = response.json()

        if response.status_code != 200:
            print(f"Error: {response.status_code} - {data.get('msg', 'no message')}")
            return None
        if not data:
            break

        record.extend(data)
        print(f"  Fetched {len(record)} records so far... (up to {pd.to_datetime(data[-1]['fundingTime'], unit='ms').date()})")

        if len(data) < limit:
            break

        startTime = data[-1]["fundingTime"] + 1
        time.sleep(0.3)

    df = pd.DataFrame(record)
    df = clean_df(df, time_field = 'fundingTime', exclude =['symbol'])
    return df

def fetch_klines(url, symbol, interval, limit = 1500, startTime = 1567641600000): # 2019-09-05 (Binance futures launch)
    """
    fetch kline data from binance API [/fapi/v1/klines]
    Return data as dataframe
    """
    print(f"----Fetching {symbol}_URL:{url} [startTime: {pd.to_datetime(startTime, unit='ms')}]----")
    record = []
    while True:
        params = {'symbol': symbol, 'interval': interval, 'startTime': startTime, 'limit': limit}
        response = requests.get(url, params=params, timeout = 10)
        data = response.json()
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {data.get('msg', 'no message')}")
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

    df = pd.DataFrame(record, columns = klines_columns)
    df = df.drop(columns=['ignore'])
    now_ms = int(time.time() * 1000)
    df = df[df['close_time'] <= now_ms]
    df = clean_df(df, time_field = 'open_time')
    return df

#check watermark
def read_watermark(bucket, key):
    """
    read watermark of endpoint from S3 (json file)
    """
    try:
        obj = s3.get_object(Bucket = bucket, Key = key)
        return json.loads(obj['Body'].read())
    except s3.exceptions.NoSuchKey:
        return None

#write watermark
def write_watermark(bucket, key, endpoint, symbol, period, last_startTime):
    """
    write watermark of endpoint to S3 (json file)
    each file for each symbol and endpoint
    """
    new_watermark = {
        'symbol' : symbol,
        'period' : period,
        'endpoint' : endpoint,
        'last_startTime' : last_startTime,
        'update_time': int(time.time()* 1000),
        'update_time_UTC': pd.Timestamp.now(tz='UTC').isoformat()
        }

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(new_watermark).encode(),
        ContentType='application/json',
    )
    return new_watermark

#upsert df into S3
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

def fetch_process(url, symbol, fetch_func, time_field, period):
    endpoint = url.split('/')[-1]
    # read watermark in S3 Bucket
    watermark_key = f'binance-futures/_watermark/{symbol}-{endpoint}-period={period}.json'
    current_watermark = read_watermark(S3_BUCKET, watermark_key)
    if current_watermark is None:
        #First run fetch 500 row (derivative) or from the start of endpoint
        print(f'---first run {symbol}-{endpoint}-period={period}---')
        if period is None:
            df = fetch_func(url, symbol)
        else:
            df = fetch_func(url, symbol, period)
    else:
        #Fetch data start from last startTime
        startTime = current_watermark['last_startTime'] + 1
        print(f'---fetching {symbol}-{endpoint}-period={period} start from: {pd.to_datetime(startTime, unit ="ms")}---')
        if period is None:
            df = fetch_func(url, symbol, startTime = startTime)
        else:
            df = fetch_func(url, symbol, period, startTime = startTime)

    # ... cold-start vs incremental fetch ...
    if df is None:
        print(f"{symbol}-{endpoint} fetch failed — skipping")
        return "FAILED — fetch error"
    if df.empty:
        print(f"[{symbol}-{endpoint}] no new data")
        return "OK — no new data"

    #upsert file to S3
    file_key = f"binance-futures/endpoint={endpoint}/symbol={symbol}/{symbol}-{endpoint}-period={period}.parquet"
    total = upsert_to_s3(S3_BUCKET, file_key, df, time_field)
    print(f"[{symbol}-{endpoint}-{period}] wrote {len(df)} new rows → {total} total in {file_key}")
    #update watermark
    last_startTime = int(df[time_field].iloc[-1])
    new_watermark = write_watermark(S3_BUCKET, watermark_key, endpoint, symbol, period, last_startTime)
    print(f'update watermark: {new_watermark}')

    return {"new_rows": len(df), "total_rows": total, "last_startTime": int(last_startTime)}

# ─── Lambda entry point ──────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Triggered by EventBridge (daily schedule).
    Loops over SYMBOLS then fetches derivatives, fundingrate and kline writes one parquet per file per symbol.
    """
    print(f"Start: symbols={SYMBOLS} period={PERIOD}")
    summary = {}
    for symbol in SYMBOLS:
        summary[symbol] = {}
        try:
            #fetch futures derivative
            for urlKey in futuresdata_url:
                summary[symbol][urlKey] = fetch_process(futuresdata_url[urlKey], symbol, fetch_derivative, 'timestamp', PERIOD)
            
            #fetch basis
            basis_url = 'https://fapi.binance.com/futures/data/basis'
            summary[symbol]['basis'] = fetch_process(basis_url, symbol, fetch_basis, 'timestamp', PERIOD)
            
            #fetch fundingrate
            fundingrate_url = 'https://fapi.binance.com/fapi/v1/fundingRate'
            summary[symbol]['fundingrate'] = fetch_process(fundingrate_url, symbol, fetch_fundingrate, 'fundingTime', None)

            #fetch kline
            klines_url = 'https://fapi.binance.com/fapi/v1/klines'
            summary[symbol]['klines'] = fetch_process(klines_url, symbol, fetch_klines, 'open_time', PERIOD)
        except Exception as e:
            # One symbol failing shouldn't kill other symbols
            print(f"[{symbol}] FAILED: {e}")
            summary[symbol] = {"error": str(e)}
            
    print(f"Done: {summary}")
    return {
        "statusCode": 200,
        "body": json.dumps({"summary": summary}),
    }