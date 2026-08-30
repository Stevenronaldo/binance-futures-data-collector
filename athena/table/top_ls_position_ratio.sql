CREATE EXTERNAL TABLE top_ls_position_ratio (
    longaccount 	DOUBLE,
    longshortratio 	DOUBLE,
    shortaccount 	DOUBLE,
    timestamp 		BIGINT
)
PARTITIONED BY (
    symbol STRING
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/binance-futures/endpoint=topLongShortPositionRatio/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.symbol.type'='enum',
    'projection.symbol.values'=<your-symbols>
);