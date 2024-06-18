
SELECT * FROM  {{ source("stocks", "ohlc_staging") }} stg