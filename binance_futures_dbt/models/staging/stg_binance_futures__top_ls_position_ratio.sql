select
  longaccount,
  longshortratio,
  shortaccount,
  "timestamp",
  symbol,
  year

from {{ source('binance_futures', 'top_ls_position_ratio') }}