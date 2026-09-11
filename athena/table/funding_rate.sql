CREATE EXTERNAL TABLE funding_rate (
    fundingtime 	BIGINT,
    fundingrate 	DOUBLE,
    markprice 	    	DOUBLE,
    ratetype        	STRING
)
PARTITIONED BY (
    symbol  STRING
)
PARTITIONED BY (
    year  INT
)

STORED AS PARQUET
LOCATION 's3://<your-bucket>/binance-futures/endpoint=fundingRate/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.symbol.type'='enum',
    'projection.symbol.values'=<your-symbols>
    'projection.year.type'     = 'date'
    'projection.year.format'   = 'yyyy'
    'projection.year.range'    = '2019,NOW'
);