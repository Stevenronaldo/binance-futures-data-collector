select
  sumopeninterest,
  sumopeninterestvalue,
  cmccirculatingsupply,
  "timestamp",
  symbol,
  year

from {{ source('binance_futures', 'open_interest') }} 