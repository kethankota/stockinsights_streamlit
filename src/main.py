import os
from datetime import datetime
from datetime import timedelta
import pandas as pd
import pytz
import configparser
from src.nse_script import *

my_config = configparser.ConfigParser()
my_config.read('./src/script_config.toml')
original_tz = pytz.timezone('Asia/Kolkata')


def run_nse_dataextraction():
    global my_config
    global original_tz
    #currDate = datetime.now(original_tz).date()
    currDate = datetime.now(original_tz).date()-timedelta(days=1)
    nse_url = my_config['NSE']['URL']
    nse_dump_filename = my_config['NSE']['DATADUMPDIR']
    if os.path.isfile(nse_dump_filename):
        currDate = datetime.strftime(currDate, "%d%m%Y")
        obj_nseDataExtraction = extract_NSEData(
            currDate, nse_url, nse_dump_filename, False)
        return obj_nseDataExtraction
    else:
        dateRange = pd.date_range(
            currDate+timedelta(days=int(my_config['NSE']['DATERANGE'])), currDate)
        for currDate in dateRange:
            currDate = datetime.strftime(currDate.date(), "%d%m%Y")
            obj_nseDataExtraction = extract_NSEData(
                currDate, nse_url, nse_dump_filename, True)
        return obj_nseDataExtraction


def run_nse_insight_generation():
    currDate = datetime.now(original_tz).date()
    #currDate = datetime.now(original_tz).date()-timedelta(days=1)
    return extract_NSEInsights(my_config, currDate)

#run_nse_dataextraction()
#run_nse_insight_generation()
