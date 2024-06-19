import pandas as pd

import psycopg2 as pg



#links
yf_url='<a href="https://finance.yahoo.com/quote/%s">%s</a>'
gurufocus_url= '<a href="https://www.gurufocus.com/stock/%s/summary">%s</a>'

def GetGrowingStocks(pg_conn,loaddate):
    #Links to more info
    def yf_url_ticker(x):
        return yf_url%(x,x)

    def gurufocus_url_ticker(x):
        return gurufocus_url%(x,x)

    buy_df=pd.read_sql_query('select * from stocks_data.growing_stocks where loaddate=''%s''', con=pg_conn,params=[loaddate])

    total=len(buy_df) 

    if total==0: 
        return '<H4>Nothing to buy today!</H4>'
    
    

   
    
    
    buy_df['YF']=buy_df['ticker'].apply(yf_url_ticker)
    buy_df['GF']=buy_df['ticker'].apply(gurufocus_url_ticker)
    
    body_html=f'<H3>Growing Stocks: {total} tickers</H3>'



    body_html=body_html+buy_df[['ticker','date','adjclose','changepointldt', 'pct_mean_chngl','YF','GF']].sort_values('pct_mean_chngl',ascending=True).to_html(index=False,render_links=True,escape=False,na_rep='')

    return body_html
