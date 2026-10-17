select
  open_time,
  open,
  high,
  low,
  close,
  volume,
  close_time,
  quote_volume,
  num_trades,
  taker_buy_base,
  taker_buy_quote,
  symbol,
  year

from {{ source('binance_futures', 'klines') }}  