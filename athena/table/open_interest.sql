CREATE EXTERNAL TABLE open_interest (
    sumopeninterest 	 	DOUBLE,
    sumopeninterestvalue	DOUBLE,
    cmccirculatingsupply	DOUBLE,
    timestamp			BIGINT
)
PARTITIONED BY (
    symbol  STRING
)
PARTITIONED BY (
    year  INT
)

STORED AS PARQUET
LOCATION 's3://<your-bucket>/binance-futures/endpoint=openInterestHist/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.symbol.type'='enum',
    'projection.symbol.values'=<your-symbols>
    'projection.year.type'     = 'date'
    'projection.year.format'   = 'yyyy'
    'projection.year.range'    = '2019,NOW'
);