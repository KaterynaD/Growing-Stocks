import os,sys
import pandas as pd
import numpy as np



import yfinance as yf





sql_stocks_to_refresh="select ticker From stocks_data.stocks where loaddata=1"
sql_copy_ohlc="COPY stocks_data.stg_OHLC FROM STDIN WITH CSV"

def DownloadOHLCData(tickers_to_download_lst, period='5d'):
    #Download
    data = yf.download(tickers_to_download_lst, period=period)
    #Adjusting index
    data=data.reset_index()
    #Adjusting 2 levels of columns to separate ticker
    if len(tickers_to_download_lst)>1:
        data.columns = ['_'.join(col) for col in data.columns.values]
        data.columns = [c+'_' for c in data.columns]
    #converting to 'Ticker','Date','Adj Close','Close','High', 'Low', 'Open','Volume' and saving to a file
    ohlc_df=pd.DataFrame()
    for t in tickers_to_download_lst:
        #list of columns specifically for ticker
        if len(tickers_to_download_lst)>1:
            l = ['Date__']
            l=l+[x for x in data.columns if '_'+t+'_' in x]
        else:
            l = data.columns     
        #Column renaming, re-arranging and adding Ticker as a column
        ohlc_1d=data[l].dropna()
        ohlc_1d.columns=['Date','Adj Close','Close','High', 'Low', 'Open','Volume']
        ohlc_1d['Ticker']=t
        ohlc_1d=ohlc_1d[['Ticker','Date','Open','High', 'Low','Close', 'Adj Close','Volume']]   
        ohlc_1d['Date'] = pd.to_datetime(ohlc_1d['Date'])    
        ohlc_df=pd.concat([ohlc_df, ohlc_1d], ignore_index=True) 
    return ohlc_df






def OHLCDataToCSV(tickers_full_filename, OHLC_full_filename, period='5d'):
    #List of tickers to download
    tickers_to_download_lst = pd.read_csv(tickers_full_filename)['ticker'].tolist()
    #Download data
    ohlc_df=DownloadOHLCData(tickers_to_download_lst, period)     
    #Save to file
    ohlc_df.to_csv(OHLC_full_filename, header=True, index=False)  



#Data='StocksData/Packages/temp_data'

#HOME = os.environ["AIRFLOW_HOME"]
#HOME = '/home/kdlab1/Projects/DataEngineering/airflow'
#OHLC_filename='OHLC1.csv'
#OHLC_full_filename=os.path.join(HOME,'dags', Data,  OHLC_filename)

#tickers_filename='tickers.csv'
#tickers_full_filename=os.path.join(HOME,'dags', Data,  tickers_filename)

#print('----------------------------------')
#print(tickers_full_filename)

#GetOHLCData(tickers_full_filename, OHLC_full_filename, period='5d', loaddate=datetime.today())

#engine = pg.connect("dbname='airflow' user='kd' host='192.168.1.201' port='5432' password=''")

#OHLCDataToPg(engine, period='5d', loaddate=datetime.today())