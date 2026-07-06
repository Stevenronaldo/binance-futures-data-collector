# 📊 Binance Futures Data Collector

An AWS Lambda function that automatically fetches **klines, funding rate, and derivatives** data from the Binance USDⓈ-M Futures API and stores it as Hive-partitioned Parquet files in S3. Built for building time-series datasets for trading analysis and model development, queryable directly with Athena.

---

## Overview

This function runs on a scheduled trigger (EventBridge) and collects **7 datasets per symbol** from the Binance Futures API — klines, funding rate, and 5 derivatives metrics.

Incremental loading is driven by a **per-symbol, per-endpoint watermark** stored as a small JSON file in S3. On the first run, each endpoint backfills its full available history; on every subsequent run, fetching resumes from the last recorded `startTime`, and new rows are upserted — deduplicating by the endpoint's time field so no rows are ever duplicated.

**Collected datasets per symbol:**

| Dataset | Binance Endpoint | Time Field | First-Run Backfill |
| :---- | :---- | :---- | :---- |
| Klines (OHLCV) | `/fapi/v1/klines` | `open_time` | From 2019-09-05 (futures launch) |
| Funding Rate | `/fapi/v1/fundingRate` | `fundingTime` | From 2019-09-05 (futures launch) |
| Open Interest | `/futures/data/openInterestHist` | `timestamp` | Up to 500 rows (API limit) |
| Global Long/Short Account Ratio | `/futures/data/globalLongShortAccountRatio` | `timestamp` | Up to 500 rows (API limit) |
| Top Trader Account Ratio | `/futures/data/topLongShortAccountRatio` | `timestamp` | Up to 500 rows (API limit) |
| Top Trader Position Ratio | `/futures/data/topLongShortPositionRatio` | `timestamp` | Up to 500 rows (API limit) |
| Taker Buy/Sell Volume | `/futures/data/takerlongshortRatio` | `timestamp` | Up to 500 rows (API limit) |

**Note on backfill limits:** Binance's `/futures/data/*` endpoints only expose the most recent \~30 days of history, so derivatives first-runs are capped at 500 rows by the API. Klines and funding rate have no such limit and are paginated all the way back to the futures launch.

---

## Architecture

              EventBridge (cron schedule)

                         │
                         ▼

                 AWS Lambda Function

                         │

    ┌────────────────────┼──────────────────────┐

    │                    │                      │
    ▼                    ▼                      ▼

    klines          Funding Rate          5 Derivatives
    (/fapi/v1)       (/fapi/v1)          (/futures/data)

    │                    │                      │

    └────────────────────┬──────────────────────┘

                         │
                         ▼

                        Amazon S3

                            └── binance-futures/

                                ├── \_watermark/

                                │     └── {SYMBOL}-{endpoint}-period={PERIOD}.json

                                └── endpoint={endpoint}/

                                      └── symbol={SYMBOL}/

                                            └── {SYMBOL}-{endpoint}-period={PERIOD}.parquet

**Per endpoint, per symbol, each run:**

1. Read the watermark JSON (`last_startTime`). Missing file → treated as first run.  
2. **First run:** klines / funding rate paginate from 2019-09-05; derivatives fetch up to 500 rows.  
3. **Incremental run:** fetch from `last_startTime + 1`.  
4. Clean (deduplicate on time field, coerce to numeric).  
5. Upsert into the endpoint's Parquet file (concat → drop duplicates → sort).  
6. Write the new watermark.

One file is written **per endpoint per symbol** — datasets are *not* merged into a single wide table.

---

## Environment Variables

Configure these in your Lambda function's environment settings:

| Variable | Required | Default | Description |
| :---- | :---- | :---- | :---- |
| `S3_BUCKET` | ✅ | — | S3 bucket name where Parquet and watermark files are stored |
| `SYMBOLS` | ✅ | — | Comma-separated list of trading pairs (e.g. `BTCUSDT,ETHUSDT`) |
| `PERIOD` | ❌ | `1h` | Aggregation period for klines & derivatives (`5m`, `15m`, `1h`, `4h`, `1d`) |

Incremental fetching bounded by the watermark, not a fixed row count. Funding rate ignores `PERIOD` (it has fixed settlement times).

**Example:**

S3\_BUCKET \= my-trading-data-bucket

SYMBOLS   \= BTCUSDT,ETHUSDT,SOLUSDT

PERIOD    \= 1h

---

## S3 Output

### Layout

    s3://{S3\_BUCKET}/binance-futures/

    ├── \_watermark/

    │     └── {SYMBOL}-{endpoint}-period={PERIOD}.json

    └── endpoint={endpoint}/

      └── symbol={SYMBOL}/

            └── {SYMBOL}-{endpoint}-period={PERIOD}.parquet

Hive-style partition keys (`endpoint=`, `symbol=`) allow Athena to use **partition projection** for efficient, low-scan queries.

### Watermark file

Each `_watermark/*.json` tracks incremental state for one symbol+endpoint:

    {

    "symbol": "BTCUSDT",

    "period": "1h",

    "endpoint": "openInterestHist",

    "last\_startTime": 1718000000000,

    "update\_time": 1718003600000,

    "update\_time\_UTC": "2024-06-10T08:00:00+00:00"

    }

### Parquet schemas (raw Binance columns)

Each endpoint is stored in its own file with the raw columns Binance returns.

**Klines** (`endpoint=klines`) — `ignore` column dropped:

open\_time · open · high · low · close · volume ·

close\_time · quote\_volume · num\_trades ·

taker\_buy\_base · taker\_buy\_quote

**Funding rate** (`endpoint=fundingRate`):

symbol · fundingTime · fundingRate · markPrice

**Open interest** (`endpoint=openInterestHist`):

symbol · sumOpenInterest · sumOpenInterestValue · timestamp

**Global / Top-trader ratios** (`globalLongShortAccountRatio`, `topLongShortAccountRatio`, `topLongShortPositionRatio`):

symbol · longAccount · shortAccount · longShortRatio · timestamp

For `topLongShortPositionRatio`, `longAccount`/`shortAccount` represent position share rather than account share.

**Taker volume** (`endpoint=takerlongshortRatio`):

buySellRatio · buyVol · sellVol · timestamp

All timestamp/time fields are Unix epoch **milliseconds (UTC)**.

---

## IAM Permissions Required

The Lambda execution role needs read/write on the data prefix and list on the bucket:

    [

    {

    "Effect": "Allow",

    "Action": \[

      "s3:GetObject",

      "s3:PutObject"

    \],

    "Resource": "arn:aws:s3:::your-bucket-name/binance-futures/\*"

    },

    {

    "Effect": "Allow",

    "Action": "s3:ListBucket",

    "Resource": "arn:aws:s3:::your-bucket-name"

    }

    ]

---

## Dependencies

requests

pandas

pyarrow

boto3

`boto3` is pre-installed in the Lambda runtime. Package `requests`, `pandas`, and `pyarrow` into a Lambda layer or your deployment ZIP (a container image also works and avoids the layer size limit).

---

## Deployment

### Lambda Settings

| Setting | Recommended |
| :---- | :---- |
| Runtime | Python 3.12 |
| Memory | 512 MB (higher for first-run backfills / many symbols) |
| Timeout | 5 – 15 min (first-run klines/funding backfills are long-running) |
| Architecture | any |

After the initial backfill, incremental runs are light — you can lower memory/timeout if desired.

### EventBridge Schedule

| Setting | Value | Reason |
| :---- | :---- | :---- |
| `PERIOD` | `1h` | 1-hour candles / derivatives aggregation |
| Run frequency | **Once daily** | \~24 new klines rows/day; watermark guarantees no gaps regardless of cadence |
| Derivatives history | \~30 days | Binance only exposes recent derivatives; run at least monthly to avoid gaps |

Because incremental fetching is watermark-driven (resume from `last_startTime`), the pipeline self-heals after missed runs for klines and funding rate — it simply fetches everything since the last success. **Derivatives are the exception:** Binance only serves \~30 days of history, so a gap longer than that window is unrecoverable. Running at least daily keeps everything complete while minimizing Lambda cost.

---

## Error Handling

- **Per-symbol isolation:** if one symbol fails (API error, bad data), the others continue processing. The exception is caught in `lambda_handler` and captured in the return summary.  
- **Missing watermark (first run):** `read_watermark` catches `NoSuchKey` → returns `None` → triggers a full backfill for that endpoint.  
- **API non-200 response:** the fetch function returns `None`; `fetch_process` logs it and returns `"FAILED — fetch error"` without crashing the run.  
- **No new data:** returns `"OK — no new data"` when the incremental fetch is empty.  
- **Return body** always includes a per-symbol, per-endpoint summary:

      {

      "summary": {

                   "BTCUSDT": {

                                "open\_interest": {"new\_rows": 24, "total\_rows": 720, "last\_startTime": 1718000000000},

                                "fundingrate":  {"new\_rows": 3,  "total\_rows": 12960, "last\_startTime": 1718000000000},

                                "klines":       {"new\_rows": 24, "total\_rows": 42000, "last\_startTime": 1718000000000}

                               }

                 }

      }

---

## Querying with Athena

Create an external table over `s3://{S3_BUCKET}/binance-futures/endpoint={endpoint}/symbol={symbol}/` with **partition projection** enabled on `endpoint` and `symbol`. Because each endpoint has its own schema, define one table per endpoint (or per group of ratio endpoints that share columns). Parquet \+ partition pruning keeps scans small and cheap.

---

## Notes

- All timestamps are stored in **UTC** (Unix epoch milliseconds).  
- Each endpoint lives in its **own file** — no cross-metric outer join. Join across endpoints at query time on the time field if you need a wide view.  
- The upsert logic uses `keep="last"` deduplication, so re-running the function for an overlapping window is safe and idempotent.  
- Binance rate limits apply. Fetch loops `time.sleep(0.3)` between paginated pages; for large symbol lists consider batching across multiple Lambda invocations.  
- `PERIOD` affects klines and derivatives only; funding rate settles on Binance's fixed schedule.

---

## Files

- `binance-futures-data-collector.py` — collector logic and Lambda entry point (`lambda_handler`).
