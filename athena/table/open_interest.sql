CREATE EXTERNAL TABLE `open_interest` (
  `timestamp`			BIGINT,
  `sumopeninterest` 	 	DOUBLE,
  `sumopeninterestvalue`	DOUBLE,
  `cmccirculatingsupply`	DOUBLE
)
PARTITIONED BY (
    `symbol` string
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/binance-futures/endpoint=openInterestHist/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.symbol.type'='enum',
    'projection.symbol.values'=<your-symbols>
);