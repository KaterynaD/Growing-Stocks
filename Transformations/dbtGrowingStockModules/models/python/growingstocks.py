import pandas as pd
import numpy as np
from math import isnan
from datetime import datetime

def model(dbt, session):

    LThreshold=int(dbt.config.get("LThreshold"))
    HThreshold=int(dbt.config.get("HThreshold"))

    ohlc_data=dbt.source("stocks", "ohlc_staging")  
   

    def ChangePoint(xClose, xDate):
        try:
            m=xClose.mean()
            Mean_Adj=xClose-m
            CumSum=Mean_Adj.cumsum()
            CumSum_pct_chng=CumSum.pct_change().abs()
            max_change=CumSum_pct_chng.max()
            chng_date=xDate[CumSum_pct_chng==max_change].values[0]
            idx_chng_date=xDate.index[CumSum_pct_chng==max_change].values[0]
            m1=xClose[xDate<chng_date].mean()
            m2=xClose[xDate>chng_date].mean()
            pct_mean_chng=round(100*(m1-m2)/m1,2)
            if isnan(pct_mean_chng):
                chng_date=np.nan
                idx_chng_date=np.nan

            return chng_date,pct_mean_chng,idx_chng_date
        except:
            return np.nan,np.nan,np.nan

    



    rows = ohlc_data.select("TICKER").distinct().collect()

    Tickers = [row["TICKER"] for row in rows]
    
    Openinterest_l=[]
    for t in Tickers: 
        ohlc_df = ohlc_data.filter(f"TICKER='{t}'").to_pandas()

        Openinterest_d={'Ticker':t}
        ChangePointL, Pct_Mean_ChngL, idx_ChangePointL = ChangePoint(ohlc_df['ADJCLOSE'].iloc[-LThreshold:],ohlc_df['DATE'].iloc[-LThreshold:])
        ChangePointH, Pct_Mean_ChngH, idx_ChangePointH = ChangePoint(ohlc_df['ADJCLOSE'].iloc[-HThreshold:],ohlc_df['DATE'].iloc[-HThreshold:]) 
        Openinterest_d.update({"CHANGE_POINT_L":ChangePointL, "PCT_MEAN_CHNG_L": Pct_Mean_ChngL})
        Openinterest_d.update({"CHANGE_POINT_H":ChangePointH, "PCT_MEAN_CHNG_H": Pct_Mean_ChngH})
        Openinterest_d.update({"PARAMS":f"HThreshold: {HThreshold}, LThreshold: {LThreshold}"})
        Openinterest_d.update({"LOADDATE":datetime.now()})
        Openinterest_l.append(Openinterest_d)

    

    #final data frame
    final_df=pd.DataFrame(Openinterest_l)
    final_df=final_df[((final_df["PCT_MEAN_CHNG_H"]>0) & (final_df["PCT_MEAN_CHNG_L"]<0))]


    final_sdf=session.create_dataframe(final_df)

    return final_sdf