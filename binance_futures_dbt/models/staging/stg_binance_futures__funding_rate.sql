select
  ((fundingtime + 3600000 / 5) / 3600000) * 3600000 as fundingtime,
  fundingrate,
  markprice,
  ratetype,
  symbol,
  year

from {{ source('binance_futures', 'funding_rate') }}
    