--Assuming data are coming into mytest_db.stocks.ohlc from airflow pipeline in Dags/GrowingStocks.py
-- Creates a Stored Procedure to extract stocks growing at least num_increases_days last month according 10 moving average
CREATE OR REPLACE PROCEDURE mytest_db.stocks.extract_growing_stocks(num_increases_days integer)
RETURNS TABLE ()
LANGUAGE sql 
AS
BEGIN
    
DECLARE res RESULTSET DEFAULT (
with data as (
    select ticker, date,adjclose,
         AVG(adjclose) OVER (
         PARTITION BY ticker
         ORDER BY date
         RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
       ) AS value,
    from mytest_db.stocks.ohlc
    where date>=DATEADD(month, -1, CURRENT_DATE())
)    
,growing_stocks as (
    SELECT * FROM data
  MATCH_RECOGNIZE(
    PARTITION BY ticker
    ORDER BY date
    MEASURES
      FIRST(date) AS start_date,
      LAST(date) AS end_date,
      MATCH_NUMBER() AS match_number,
      COUNT(row_with_price_increase.*) AS num_increases
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST row_with_price_increase
    PATTERN(row_before_increase   row_with_price_increase+)
    DEFINE
      row_with_price_increase AS value > LAG(value)
  )
WHERE num_increases>:num_increases_days
)
select
d.ticker,
ohlc.adjclose latest_adjclose,
d.start_date,
d.end_date,
d.num_increases
from growing_stocks d
join mytest_db.stocks.ohlc ohlc 
on d.ticker=ohlc.ticker
where ohlc.date=(select max(t.date) from mytest_db.stocks.ohlc t where t.ticker=d.ticker )
ORDER BY d.ticker, d.match_number
    );
BEGIN 
    RETURN table(res);
END;
END;


-- Create Snowpark Python Stored Procedure to format email and send it
CREATE OR REPLACE PROCEDURE mytest_db.stocks.send_growing_stocks()
RETURNS string
LANGUAGE python
runtime_version = 3.9
packages = ('snowflake-snowpark-python')
handler = 'send_email'
AS
$$
def send_email(session):
    session.call('mytest_db.stocks.extract_growing_stocks',5).collect()


    html_table = session.sql("select * from table(result_scan(last_query_id(-1)))").to_pandas().to_html()
    # https://codepen.io/labnol/pen/poyPejO?editors=1000
    html_table = html_table.replace('class="dataframe"', 'style="border: solid 2px #DDEEEE; border-collapse: collapse; border-spacing: 0; font: normal 14px Roboto, sans-serif;"')
    html_table = html_table.replace('<th>', '<th style="background-color: #DDEFEF; border: solid 1px #DDEEEE; color: #336B6B; padding: 10px; text-align: left; text-shadow: 1px 1px 1px #fff;">')
    html_table = html_table.replace('<td>', '<td style="    border: solid 1px #DDEEEE; color: #333; padding: 10px; text-shadow: 1px 1px 1px #fff;">')
      
    session.call('system$send_email',
        'my_email_int',
        'drogaieva@gmail.com',
        'Email Alert: Growing Stocks',
        html_table,
        'text/html')
$$;


-- Orchestrating the Tasks: 
CREATE OR REPLACE TASK mytest_db.stocks.send_growing_stocks
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 23 * * * America/Los_Angeles' -- Runs once a day
    AS CALL mytest_db.stocks.send_growing_stocks();

-- Steps to resume and then immediately execute the task DAG:  

ALTER TASK mytest_db.stocks.send_growing_stocks RESUME;
EXECUTE TASK mytest_db.stocks.send_growing_stocks;

ALTER TASK mytest_db.stocks.send_growing_stocks SUSPEND;