WITH fr_data AS( 
    SELECT 
        symbol, 
        fundingtime,
        fundingrate 
    FROM {{ ref("stg_binance_futures__funding_rate")}}
),
gls_acc AS(
    SELECT 
        symbol, 
         "timestamp",
        longshortratio as global_ls_acc_ratio 
    FROM {{ ref("stg_binance_futures__global_ls_account_ratio")}}
),
index_data AS(
    SELECT 
        symbol, 
        open_time,
        close as index_close
    FROM {{ ref("stg_binance_futures__index_price_klines")}}
),
klines_data AS(
    SELECT 
        symbol, 
        open_time,
        open, high, low, close, volume, num_trades, 
        (taker_buy_base / NULLIF(volume - taker_buy_base, 0)) AS taker_ls_ratio
    FROM {{ ref("stg_binance_futures__klines")}}
),
oi_data AS(
    SELECT 
        symbol, 
        "timestamp",
        sumopeninterest
    FROM {{ ref("stg_binance_futures__open_interest")}}
),
tls_acc AS(
    SELECT 
        symbol, 
        "timestamp",
        longshortratio as top_ls_acc_ratio
    FROM {{ ref("stg_binance_futures__top_ls_account_ratio")}}
),
tls_pos AS(
    SELECT 
        symbol, 
        "timestamp",
        longshortratio as top_ls_pos_ratio
    FROM {{ ref("stg_binance_futures__top_ls_position_ratio")}}
),
joined AS(
    SELECT
        k.symbol,
        k.open_time AS ts,
        k.open, k.high, k.low, k.close, k.volume, k.num_trades,
        k.taker_ls_ratio,
        (k.close - i.index_close) / i.index_close AS basis_rate,
        oi.sumopeninterest,
        g.global_ls_acc_ratio,
        ta.top_ls_acc_ratio,
        tp.top_ls_pos_ratio,
        fr.fundingrate AS raw_funding
    FROM klines_data k
    LEFT JOIN index_data i
        ON k.symbol = i.symbol AND k.open_time = i.open_time
    LEFT JOIN oi_data oi
        ON k.symbol = oi.symbol AND k.open_time = oi."timestamp"
    LEFT JOIN gls_acc g
        ON k.symbol = g.symbol AND k.open_time = g."timestamp"
    LEFT JOIN tls_acc ta
        ON k.symbol = ta.symbol AND k.open_time = ta."timestamp"
    LEFT JOIN tls_pos tp
        ON k.symbol = tp.symbol AND k.open_time = tp."timestamp"
    LEFT JOIN fr_data fr 
        ON k.symbol = fr.symbol AND k.open_time = fr.fundingtime
)

SELECT
    symbol,
    ts,
    open, high, low, close, volume, num_trades,
    taker_ls_ratio,
    basis_rate,
    sumopeninterest,
    global_ls_acc_ratio,
    top_ls_acc_ratio,
    top_ls_pos_ratio,
    LAST_VALUE(raw_funding) IGNORE NULLS OVER (
        PARTITION BY symbol ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS funding_rate
FROM joined
