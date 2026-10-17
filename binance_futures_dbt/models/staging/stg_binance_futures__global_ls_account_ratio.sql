select
  longaccount,
  longshortratio,
  shortaccount,
  "timestamp",
  symbol,
  year

from {{ source('binance_futures', 'global_ls_account_ratio') }}

