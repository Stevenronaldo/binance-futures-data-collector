# Binance endpoints collected by the Lambda, and the columns each one stores.
# Pure data (no imports) so both lambda_function.py and scripts/ can use it.
#
# schema: {column: (pandas dtype, nullable)}
# Read from the Parquet files in S3 (BTCUSDT, all years); keep in sync with
# the Glue tables in terraform/athena.tf.
#
# nullable describes what the Binance API may send. Rows backfilled from the
# data.binance.vision archive (scripts/backfill_derivatives.py) only carry
# symbol, timestamp and the ratio/OI columns, so the other derivative columns
# are null for those rows in S3.

KLINES_SCHEMA = {
    "open_time":       ("int64",   False),
    "open":            ("float64", False),
    "high":            ("float64", False),
    "low":             ("float64", False),
    "close":           ("float64", False),
    "volume":          ("float64", False),
    "close_time":      ("int64",   False),
    "quote_volume":    ("float64", False),
    "num_trades":      ("int64",   False),
    "taker_buy_base":  ("float64", False),
    "taker_buy_quote": ("float64", False),
}

INDEX_PRICE_KLINES_SCHEMA = {
    "open_time":  ("int64",   False),
    "open":       ("float64", False),
    "high":       ("float64", False),
    "low":        ("float64", False),
    "close":      ("float64", False),
    "close_time": ("int64",   False),
}

FUNDING_RATE_SCHEMA = {
    "symbol":      ("string",  False),
    "fundingTime": ("int64",   False),
    "fundingRate": ("float64", False),
    "markPrice":   ("float64", True),   # empty for old records (e.g. 2019)
    "rateType":    ("string",  True),   # added by Binance in 2026
}

OPEN_INTEREST_SCHEMA = {
    "symbol":               ("string",  False),
    "sumOpenInterest":      ("float64", False),
    "sumOpenInterestValue": ("float64", False),
    "CMCCirculatingSupply": ("float64", True),
    "timestamp":            ("int64",   False),
}

# Shared by global / top-trader account / top-trader position ratios.
LONG_SHORT_RATIO_SCHEMA = {
    "symbol":         ("string",  False),
    "longAccount":    ("float64", False),
    "longShortRatio": ("float64", False),
    "shortAccount":   ("float64", False),
    "timestamp":      ("int64",   False),
}

# fetcher: key into FETCHERS in lambda_function.py
# use_period: False for fundingRate (fixed settlement times, ignores PERIOD)
ENDPOINTS = [
    {
        "name": "openInterestHist",
        "url": "https://fapi.binance.com/futures/data/openInterestHist",
        "fetcher": "derivative",
        "time_field": "timestamp",
        "use_period": True,
        "schema": OPEN_INTEREST_SCHEMA,
    },
    {
        "name": "globalLongShortAccountRatio",
        "url": "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
        "fetcher": "derivative",
        "time_field": "timestamp",
        "use_period": True,
        "schema": LONG_SHORT_RATIO_SCHEMA,
    },
    {
        "name": "topLongShortAccountRatio",
        "url": "https://fapi.binance.com/futures/data/topLongShortAccountRatio",
        "fetcher": "derivative",
        "time_field": "timestamp",
        "use_period": True,
        "schema": LONG_SHORT_RATIO_SCHEMA,
    },
    {
        "name": "topLongShortPositionRatio",
        "url": "https://fapi.binance.com/futures/data/topLongShortPositionRatio",
        "fetcher": "derivative",
        "time_field": "timestamp",
        "use_period": True,
        "schema": LONG_SHORT_RATIO_SCHEMA,
    },
    {
        "name": "fundingRate",
        "url": "https://fapi.binance.com/fapi/v1/fundingRate",
        "fetcher": "fundingrate",
        "time_field": "fundingTime",
        "use_period": False,
        "schema": FUNDING_RATE_SCHEMA,
    },
    {
        "name": "klines",
        "url": "https://fapi.binance.com/fapi/v1/klines",
        "fetcher": "klines",
        "time_field": "open_time",
        "use_period": True,
        "schema": KLINES_SCHEMA,
    },
    {
        "name": "indexPriceKlines",
        "url": "https://fapi.binance.com/fapi/v1/indexPriceKlines",
        "fetcher": "indexprice",
        "time_field": "open_time",
        "use_period": True,
        "schema": INDEX_PRICE_KLINES_SCHEMA,
    },
]
