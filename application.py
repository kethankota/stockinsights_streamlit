from datetime import datetime
import requests
import pandas as pd
#import numpy as np
from io import BytesIO
#import schedule
import pytz
import streamlit as st


original_tz = pytz.timezone('Asia/Kolkata')
currDate = datetime.strftime(datetime.now(original_tz).date(), "%d%m%Y")
url = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_{currDate}.csv'
filename = './data/df_sec_bhavdata_dump.csv'
def extract_NSEData(url,currDate,filename):
    try:
        response = requests.get(url.format(currDate=currDate),timeout=5)
        if response.status_code == 200:
            df_sec_bhavdata = pd.read_csv(BytesIO(response.content),skipinitialspace=True)
            df_sec_bhavdata_dump = pd.read_csv(filename)
            df_sec_bhavdata_dump = pd.concat([df_sec_bhavdata_dump,df_sec_bhavdata])
            delDate = df_sec_bhavdata_dump.DATE1.min() 
            df_sec_bhavdata_dump = df_sec_bhavdata_dump[~df_sec_bhavdata_dump['DATE1'].isin([delDate])]
            #df_sec_bhavdata_dump.to_csv(filename,index=False)
            return ("Successful",currDate)
    except:
        return ("Failure",currDate)
    
st.title("Currently application is in development\nView sample data for now")

st.write(datetime.now().astimezone().tzinfo)

st.dataframe(pd.read_csv(filename))
