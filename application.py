from datetime import datetime
import pandas as pd
import time
import pytz
import streamlit as st
from src.main import *
from streamlit_autorefresh import st_autorefresh
import configparser

original_tz = pytz.timezone('Asia/Kolkata')
currDate = datetime.now(original_tz).date()
currDate = datetime.strftime(currDate, "%d%m%Y")


@st.cache_resource(experimental_allow_widgets=True)
def get_run_counter_session(val):
    refresh_counter = st_autorefresh(interval=60000, limit=None, key=None)
    return refresh_counter


counter = get_run_counter_session(0)
parser = configparser.ConfigParser()
parser.read('./src/state_config.toml')
my_config = configparser.ConfigParser()
my_config.read('./src/script_config.toml')


if ('counter_value' not in st.session_state) & \
    ('last_nse_run_status' not in st.session_state) & \
        ('last_nse_run_time' not in st.session_state):
    st.session_state.last_nse_run_status = parser['MANAGER']['STATUS']
    st.session_state.last_nse_run_time = parser['MANAGER']['RUNTIME']
    st.session_state.counter_value = int(parser['MANAGER']['COUNTER'])


@st.cache_data()
def update_counter(session_counter_value):
    global currDate
    if currDate != parser['MANAGER']['RUNDATE']:
        parser['MANAGER']['RUNDATE'] = datetime.strftime(currDate, "%d%m%Y")
        parser['MANAGER']['COUNTER'] = str(0)
        st.cache_data.clear()
    elif datetime.now(original_tz).time().strftime("%H:%M") == '20:00':
        run_nse_dataextraction()
        run_result = run_nse_insight_generation()
        st.session_state.last_nse_run_status = run_result[0]
        st.session_state.last_nse_run_time = str(
            datetime.now(original_tz).time().strftime("%H:%M"))
        parser['MANAGER']['STATUS'] = st.session_state.last_nse_run_status
        parser['MANAGER']['RUNDATE'] = currDate
        parser['MANAGER']['RUNTIME'] = st.session_state.last_nse_run_time
    else:
        parser['MANAGER']['COUNTER'] = str(session_counter_value)
    with open('./src/state_config.toml', 'w') as writeConfigFile:
        parser.write(writeConfigFile)


update_counter(st.session_state.counter_value + counter)

with st.sidebar:
    st.header("This Area was reserved for manual script run controls")

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
    st.subheader(st.session_state.last_nse_run_time)

with right_col:
    st.subheader("BSE")
    if st.session_state.last_nse_run_status == 'Successful':
        st.subheader(":green["+st.session_state.last_nse_run_status+"]")
    else:
        st.subheader(":red["+st.session_state.last_nse_run_status+"]")
    st.subheader(st.session_state.last_nse_run_time)

st.divider()
st.subheader("Download the reports from the below tabs")

nse_tab, bse_tab = st.tabs(
    [":white[NSE Insights Data]", ":white[BSE Insights Data]"])

with nse_tab:
    left_col_nse_tab, right_col_nse_tab = st.columns([3, 1])
    with left_col_nse_tab:
        st.write("")
        st.write("Click on download button to download report as on {date}".format(
            date = parser['MANAGER']['RUNDATE'][:2]+"/"+parser['MANAGER']['RUNDATE'][2:4]))
    with right_col_nse_tab:
        st.write("")
        with open(my_config['NSE']['OUTPUTDIR']+"nse_insights.csv", 'rb') as f:
            st.download_button('Download', f, file_name='NSE_Insights.csv')

with bse_tab:
    st.write("BSE still in development")
