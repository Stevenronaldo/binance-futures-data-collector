CREATE EXTERNAL TABLE `top_ls_account_ratio` (
  `timestamp` 		BIGINT, 
  `longaccount` 	DOUBLE,
  `longshortratio` 	DOUBLE,
  `shortaccount` 	DOUBLE
)
PARTITIONED BY (
    `symbol` string
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/binance-futures/endpoint=topLongShortAccountRatio/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.symbol.type'='enum',
    'projection.symbol.values'=<your-symbols>
);