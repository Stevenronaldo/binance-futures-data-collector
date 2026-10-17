select
  open_time,
  open,
  high,
  low,
  close,
  close_time,
  symbol,
  year

from {{ source('binance_futures', 'index_price_klines') }}