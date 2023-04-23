from datetime import datetime
import requests
import pandas as pd
import numpy as np
from io import BytesIO
import pytz
import time
import streamlit as st

st.session_state.last_nse_run_status = 'Successful'
st.session_state.last_nse_run_date = '21042023'


def extract_NSEData():
    original_tz = pytz.timezone('Asia/Kolkata')
    currDate = datetime.strftime(datetime.now(original_tz).date(), "%d%m%Y")
    url = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_{currDate}.csv'
    filename = './data/df_sec_bhavdata_dump.csv'
    try:
        response = requests.get(url.format(currDate=currDate), timeout=5)
        if response.status_code == 200:
            df_sec_bhavdata = pd.read_csv(
                BytesIO(response.content), skipinitialspace=True)
            df_sec_bhavdata_dump = pd.read_csv(filename)
            df_sec_bhavdata_dump = pd.concat(
                [df_sec_bhavdata_dump, df_sec_bhavdata])
            delDate = df_sec_bhavdata_dump.DATE1.min()
            df_sec_bhavdata_dump = df_sec_bhavdata_dump[~df_sec_bhavdata_dump['DATE1'].isin([
                                                                                            delDate])]
            # df_sec_bhavdata_dump.to_csv(filename,index=False)
            return ("Successful", currDate)
    except:
        return ("Failure", currDate)


@st.cache_resource
def run_job(change_run_hour=0):
    while True:
        if datetime.now().time().hour == 17:
            tp_status = extract_NSEData()
            st.session_state.last_nse_run_status = tp_status[0]
            st.session_state.last_nse_run_date = tp_status[1]
            time.sleep(60*60)

with st.sidebar:
    st.header("This Area was reserved for manual script run controls")

#st.sidebar.header("This Area is still in development")

st.markdown("<h1 style='text-align: center; color: #EDF5E1;'>STOCK MARKET - INSIGHTS</h1>",
            unsafe_allow_html=True)
st.divider()
left_col, mid_col, right_col = st.columns([1, 1, 1])

with left_col:
   st.subheader("Exchange")
   st.subheader("Last Run")
   st.subheader("Run date")

with mid_col:
   st.subheader("NSE")
   if st.session_state.last_nse_run_status == 'Successful':
       st.subheader(":green["+st.session_state.last_nse_run_status+"]")
   else:
       st.subheader(":red["+st.session_state.last_nse_run_status+"]")
   st.subheader(
       st.session_state.last_nse_run_date[:2]+"/"+st.session_state.last_nse_run_date[2:4])

with right_col:
   st.subheader("BSE")
   if st.session_state.last_nse_run_status == 'Successful':
       st.subheader(":green["+st.session_state.last_nse_run_status+"]")
   else:
       st.subheader(":red["+st.session_state.last_nse_run_status+"]")
   st.subheader(
       st.session_state.last_nse_run_date[:2]+"/"+st.session_state.last_nse_run_date[2:4])

st.divider()
st.subheader("Download the reports from the below tabs")

nse_tab, bse_tab = st.tabs(
    [":white[NSE Insights Data]", ":white[BSE Insights Data]"])

with nse_tab:
    report_dates = ['21/04', '20/04', '19/04', '18/04', '17/04']
    with st.form("nse_report_download"):
        sel_report_dates = st.radio(
        "Select date to download report 👇",
        report_dates,
        key="nse_visibility",
        horizontal=True)

        # Every form must have a submit button.
        left_col_nse_tab, mid_col_nse_tab, right_col_nse_tab = st.columns([2, 1, 2])
        with mid_col_nse_tab:
            submitted = st.form_submit_button("Download")
            if submitted:
                st.write("Selected date : ", sel_report_dates)


with bse_tab:
    report_dates = ['21/04', '20/04', '19/04', '18/04', '17/04']
    with st.form("bse_report_download"):
        sel_report_dates = st.radio(
        "Select date to download report 👇",
        report_dates,
        key="bse_visibility",
        horizontal=True)

        # Every form must have a submit button.
        left_col_bse_tab, mid_col_bse_tab, right_col_bse_tab = st.columns([2, 1, 2])
        with mid_col_bse_tab:
            submitted = st.form_submit_button("Download")
            if submitted:
                st.write("Selected date : ", sel_report_dates)
