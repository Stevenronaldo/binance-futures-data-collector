CREATE EXTERNAL TABLE `index_price_klines` (
    open_time               BIGINT,
    open                    DOUBLE,
    high                    DOUBLE,
    low                     DOUBLE,
    close                   DOUBLE,
    close_time              BIGINT
)
PARTITIONED BY (
    symbol STRING
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/binance-futures/endpoint=indexPriceKlines/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.symbol.type'   = 'enum',
    'projection.symbol.values' = <your-symbols>
);