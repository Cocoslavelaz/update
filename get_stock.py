import yfinance as yf 
import pandas as pd
import twstock

def get_close(code,start="2018-01-01",end="2024-01-01"):
    df = yf.download(code,start=start,end=end)
    return df

