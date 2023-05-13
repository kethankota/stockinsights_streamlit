import os
import ast
from datetime import datetime
import requests
import pandas as pd
import numpy as np
from io import BytesIO


def extract_NSEData(currDate, url, filename, is_dump=False):
    try:
        response = requests.get(url.format(currDate=currDate), timeout=5)
        if response.status_code == 200:
            df_sec_bhavdata = pd.read_csv(
                BytesIO(response.content), skipinitialspace=True)
            if os.path.isfile(filename):
                df_sec_bhavdata_dump = pd.read_csv(filename)
                df_sec_bhavdata_dump = pd.concat(
                    [df_sec_bhavdata_dump, df_sec_bhavdata])
            else:
                df_sec_bhavdata_dump = df_sec_bhavdata
            df_sec_bhavdata_dump['DATE1'] = pd.to_datetime(
                df_sec_bhavdata_dump['DATE1'])
            if not is_dump:
                delDate = df_sec_bhavdata_dump.DATE1.min()
                df_sec_bhavdata_dump = df_sec_bhavdata_dump[
                    ~df_sec_bhavdata_dump['DATE1'].isin([delDate])]
            df_sec_bhavdata_dump.drop_duplicates(inplace=True)
            df_sec_bhavdata_dump.to_csv(filename, index=False)
            return ("Successful",)
    except:
        return ("Failure",)


def extract_NSEInsights(my_config, currDate):
    try:
        df_sec_bhavdata_dump = pd.read_csv(my_config['NSE']['DATADUMPDIR'])
        mask_series = df_sec_bhavdata_dump['SERIES'].isin(
            ast.literal_eval(my_config['NSE']['NSEFILTER_SERIES']))
        df_sec_bhavdata_dump.loc[mask_series, 'DELIV_QTY'] = df_sec_bhavdata_dump.loc[mask_series, 'TTL_TRD_QNTY']
        df_sec_bhavdata_dump['DELIVERY_QTY_PER_TRADE'] = df_sec_bhavdata_dump.apply(
            lambda row: int(row['DELIV_QTY'])/int(row['NO_OF_TRADES']), axis=1)
        df_sec_bhavdata_dump.sort_values(['SYMBOL', 'DATE1'], inplace=True)
        df_sec_bhavdata_dump['AVG_1YEAR_DELIVERY_QTY_PER_TRADE'] = df_sec_bhavdata_dump.groupby(
            'SYMBOL')['DELIVERY_QTY_PER_TRADE'].rolling(window=180).mean().reset_index(0, drop=True)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_1TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] = df_sec_bhavdata_dump.apply(
            lambda row: True if row['DELIVERY_QTY_PER_TRADE'] > row['AVG_1YEAR_DELIVERY_QTY_PER_TRADE'] else False, axis=1)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_2TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] = df_sec_bhavdata_dump.apply(
            lambda row: True if row['DELIVERY_QTY_PER_TRADE'] > 2*row['AVG_1YEAR_DELIVERY_QTY_PER_TRADE'] else False, axis=1)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_3TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] = df_sec_bhavdata_dump.apply(
            lambda row: True if row['DELIVERY_QTY_PER_TRADE'] > 3*row['AVG_1YEAR_DELIVERY_QTY_PER_TRADE'] else False, axis=1)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_4TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] = df_sec_bhavdata_dump.apply(
            lambda row: True if row['DELIVERY_QTY_PER_TRADE'] > 4*row['AVG_1YEAR_DELIVERY_QTY_PER_TRADE'] else False, axis=1)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_5TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] = df_sec_bhavdata_dump.apply(
            lambda row: True if row['DELIVERY_QTY_PER_TRADE'] > 5*row['AVG_1YEAR_DELIVERY_QTY_PER_TRADE'] else False, axis=1)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_2TIME_AVG_1YEAR_DEL_QTY_PER_TRADE_COUNT'] = (
            df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_2TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] == True).rolling(window=int(my_config['NSE']['NSE_ROLLING_WINDOW'])).sum().replace(np.nan, 0)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_3TIME_AVG_1YEAR_DEL_QTY_PER_TRADE_COUNT'] = (
            df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_3TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] == True).rolling(window=int(my_config['NSE']['NSE_ROLLING_WINDOW'])).sum().replace(np.nan, 0)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_4TIME_AVG_1YEAR_DEL_QTY_PER_TRADE_COUNT'] = (
            df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_4TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] == True).rolling(window=int(my_config['NSE']['NSE_ROLLING_WINDOW'])).sum().replace(np.nan, 0)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_5TIME_AVG_1YEAR_DEL_QTY_PER_TRADE_COUNT'] = (
            df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_5TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] == True).rolling(window=int(my_config['NSE']['NSE_ROLLING_WINDOW'])).sum().replace(np.nan, 0)
        df_sec_bhavdata_dump['DAY_DEL_QTY_PER_TRADE_CROSS_5TIME_AVG_1YEAR_DEL_QTY_PER_TRADE_HARDCODED_FILTER'] = df_sec_bhavdata_dump.apply(
            lambda row: True if ((row['DAY_DEL_QTY_PER_TRADE_CROSS_5TIME_AVG_1YEAR_DEL_QTY_PER_TRADE'] == True) &
                                 (int(row['NO_OF_TRADES']) > 50) &
                                 (int(row['TTL_TRD_QNTY']) > 10000)) else False, axis=1)
        df_sec_bhavdata_dump.to_csv(my_config['NSE']['OUTPUTDIR']+"nse_insights.csv", index=False)
        return ("Successful",)
    except:
        return ("Failure",)
