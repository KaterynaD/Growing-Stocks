with data as (
select 
d.ticker,
d.change_point_l as GrowingStartDate
from  {{ref('growingstocks')}} d
where d.loaddate=(select max(t.loaddate) from {{ref('growingstocks')}} t) 
)
,ohlc_data as (
select 
data.ticker,
data.GrowingStartDate,
ohlc.Close as LatestClose
from  {{ref('ohlc')}} ohlc
join data
on data.ticker=ohlc.ticker
where ohlc.date=(select max(t.date) from {{ref('ohlc')}} t where t.ticker=data.ticker) 
)
select 
ticker,
GrowingStartDate,
LatestClose
from ohlc_data
order by ticker, GrowingStartDate