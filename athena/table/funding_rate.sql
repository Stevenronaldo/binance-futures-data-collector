CREATE EXTERNAL TABLE `funding_rate` (
    `fundingtime` 	BIGINT,
    `fundingrate` 	DOUBLE,
    `markprice` 	DOUBLE,
)
PARTITIONED BY (
    `symbol` string
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/binance-futures/endpoint=fundingRate/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.symbol.type'='enum',
    'projection.symbol.values'=<your-symbols>
);