CREATE EXTERNAL TABLE klines (
  open_time		BIGINT,
  open			DOUBLE,
  high 			DOUBLE, 
  low 			DOUBLE,
  close			DOUBLE, 
  volume 		DOUBLE,
  close_time		BIGINT,
  quote_volume		DOUBLE,
  num_trades		BIGINT,
  taker_buy_base	DOUBLE,
  taker_buy_quote	DOUBLE
)
PARTITIONED BY (
    symbol STRING
)
PARTITIONED BY (
    year  INT
)

STORED AS PARQUET
LOCATION 's3://<your-bucket>/binance-futures/endpoint=klines/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.symbol.type'   = 'enum',
    'projection.symbol.values' = <your-symbols>
    'projection.year.type'     = 'date'
    'projection.year.format'   = 'yyyy'
    'projection.year.range'    = '2019,NOW'
);