import os 
import pandas as pd
import datetime
import urllib
import requests
import time

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Intraday_fetch_scripts.get_daily_vol_leaders import get_working_days
from Data_Access.Find_file_path import find_file
from api_keys import tda_key

historical_intraday_dir = "Intraday_formated"
historical_intraday_dir = find_file(historical_intraday_dir)

ticker_100_path = "Top 100 volume leaders.csv"
ticker_100_path = find_file(ticker_100_path)

raw_folder_to_read = "Intraday_raw"
raw_folder_to_read = find_file(raw_folder_to_read)

intraday_path_formatted = historical_intraday_dir + "Intra_" 

possibilites  = ["Volume" , "Highs_Lows"]



def find_raw_files_to_sum(attribute):

    file_to_write_to = intraday_path_formatted + attribute + ".csv"
    sum_csv = pd.DataFrame()
    try:
        sum_csv = pd.read_csv(file_to_write_to)
    except:
        sum_csv['date'] = 0
        sum_csv['ticker'] = 0
        print(sum_csv.head())
        print(file_to_write_to + " does not exist yet: new file")

    files_to_sum = []
    dir_content = os.listdir(raw_folder_to_read)
    for csv_file in dir_content:
        
        # ticker and date to perform calculation on
        csv_file_comp = csv_file.split("_")
        ticker = csv_file_comp[0]
        date = csv_file_comp[1]

        # check if this date and ticker are already in the intra sum file (for future uses)
        duplicate = sum_csv[(sum_csv['ticker'] == ticker) & (sum_csv['date'] == date)]
        if len(duplicate) == 0:
            files_to_sum.append(csv_file)
            continue

    return files_to_sum


'''
Reading data from collected intraday csv files from intraday_raw folder

the keys to writing these functions are:
1) I am capable of doing it the same thing again next time with other data

this function creates a new csv OR adds new rows of the same columns to the file
if I want to merge to csv I can use another function for that

now the problem is that if i modify the script that already have, i will just add
new tickers and dates. if I decide to 
1) add new columns to existing rows
2) make changes in the existing columns of existing rows
that won't be reflected.
the issue arises because i rely on what is in the file already and not what the new scipts are
So,
for I should rely on the file for existing dates and tickers, BUT should rely on the new script,
for columns

of course, this is only an issue if decide to rely on what already exists in the file and not 
just redo the whole thing

'''
def create_new_intra_sum_csv(attribute):

    csv_files = find_raw_files_to_sum(attribute)

    file_to_write_to = intraday_path_formatted + attribute + ".csv"
    sum_csv = pd.DataFrame()
    try:
        sum_csv = pd.read_csv(file_to_write_to)
    except:
        print(file_to_write_to + " does not exist yet: new file")

    for csv_index, new_raw_csv in enumerate(csv_files):
        
        # ticker and date to perform calculation on
        csv_file_comp = new_raw_csv.split("_")
        ticker = csv_file_comp[0]
        date = csv_file_comp[1]
        millis_date = int(datetime.datetime.strptime(date, "%Y-%m-%d").timestamp()) * 1000
        single_row = {"date" : date, "ticker" : ticker}

        ticker_df = pd.read_csv(raw_folder_to_read + new_raw_csv)

        # needed dict
        attribute_dict = {}
        if attribute == possibilites[0]:
            attribute_dict = calc_vol_sums(ticker_df, millis_date)
        elif attribute == possibilites[1]:
            attribute_dict = calc_intra_percent(ticker_df, millis_date)

        single_row.update(attribute_dict)
        # to preserve ticker 
        row_df = pd.DataFrame(single_row, columns=list(single_row.keys()), index=[0])
        sum_csv = sum_csv.append(row_df, ignore_index = True)
        print(sum_csv.head())

        if csv_index % 10 == 0:
            print(str(csv_index + 1) + " out of " + str(len(csv_files)))

    sum_csv.to_csv(file_to_write_to, index=False)
    return



'''
given single days all intraday data and day's starting timestamp
this function performs summation of volume from and before 9:30 am

returns dict of summation values
 '''
def calc_vol_sums(ticker_df, millis_date):

    # milis since 9:30
    # overwriting the values as I iterate over the keys
    vol_dict_v2 = {"1 min" : 60 * 1000,
                "5 min" : 300 * 1000, 
                "15 min" : 900 * 1000, 
                "30 min" : 1800 * 1000,
                "60 min" : 3600 * 1000,
                "EOD" : 23400 * 1000,
                "PM" : 0
                }

    nine_thirty = (32400 + 1800) * 1000 + millis_date
    nine_30_index = ticker_df.index[ticker_df["datetime"] >= nine_thirty].tolist()
    nine_30_index = nine_30_index[0]
    # partially calculates EOD and doesn't calculate PM
    for time in vol_dict_v2:
        new_time =  nine_thirty + vol_dict_v2[time]
        index_new_time = ticker_df.index[ticker_df["datetime"] >= new_time].tolist()
        if len(index_new_time) == 0:
            index_new_time = ticker_df.index[ticker_df["datetime"] < new_time].tolist()
            index_new_time = index_new_time[-1]
            print("selecting from prior times")
        else:
            index_new_time = index_new_time[0]
        vol_dict_v2[time] = ticker_df["volume"][nine_30_index:index_new_time].sum()

    # PM volume
    vol_dict_v2["PM"] = ticker_df["volume"][:nine_30_index].sum()
    # EOD volume, in the loop only calculated from 9:30, not all day
    vol_dict_v2["EOD"] = vol_dict_v2["EOD"] + vol_dict_v2["PM"]

    return vol_dict_v2

same_attribute_name = "Volume"
create_new_intra_sum_csv(same_attribute_name)
    
def calc_intra_percent(price_intra_pd, millis_date):

    highs_lows = {
        "Time of PMH" : 0,
        "PM High" : 0,
        "PMH to open" : 0,
        "Open to high" : 0,
        "Open to low" : 0,
        "Direction" : 0,
    }

    nine_thirty = (32400 + 1800) * 1000 + millis_date
    four_pm = 57600 * 1000 + millis_date
    four_pm_list = price_intra_pd.index[price_intra_pd["datetime"] <= four_pm].tolist()
    four_pm_index = four_pm_list[-1]

    nine_thirty_list = price_intra_pd.index[price_intra_pd["datetime"] >= nine_thirty].tolist()
    nine_thirty_index = nine_thirty_list[0]
    nine_thirty_price = price_intra_pd["open"][nine_thirty_index]

    # PH high
    pm_high = price_intra_pd["high"][:nine_thirty_index].max()
    highs_lows["PM High"] = pm_high

    # time of PMH
    pm_high_list = price_intra_pd.index[price_intra_pd["high"] == pm_high].tolist()
    # if list is empty add to current 
    if pm_high_list == [] :
        pm_high_list.append(nine_thirty_index)
    pm_high_index = pm_high_list[0]
    datetime = price_intra_pd["datetime"][pm_high_index]
    highs_lows["Time of PMH"] = datetime

    # PMH to open
    highs_lows["PMH to open"] = (nine_thirty_price - highs_lows["PM High"]) / highs_lows["PM High"] * 100
    highs_lows["PMH to open"] = round(highs_lows["PMH to open"],2)

    # Open to high
    open_to_high = price_intra_pd["high"][nine_thirty_index:four_pm_index].max()
    open_to_high = (open_to_high - nine_thirty_price) / nine_thirty_price * 100
    highs_lows["Open to high"] = round(open_to_high, 2)

    # Open to low
    open_to_low = price_intra_pd["low"][nine_thirty_index:four_pm_index].min()
    open_to_low = (open_to_low - nine_thirty_price) / nine_thirty_price * 100
    highs_lows["Open to low"] = round(open_to_low,2)

    # direction
    up = highs_lows["Open to high"] > -highs_lows["Open to low"] * 3
    down = 3 * highs_lows["Open to high"] < -highs_lows["Open to low"] 
    if up:
        highs_lows["Direction"] = 1
    elif down:
        highs_lows["Direction"] = -1
    else:
        highs_lows["Direction"] = 0

    return highs_lows


intraday_path_all = intraday_path_formatted + "All.csv"
def merge_exisiting_intraday():

    # collect dfs for merging
    dataframes_to_merge = []
    same_columns = []
    for index, i in enumerate(possibilites):
        path = intraday_path_formatted + i + ".csv"
        dataframes_to_merge.append(pd.read_csv(path))
        same_columns.append(list(dataframes_to_merge[index].columns))

    # check for missing rows
    col1 = list(dataframes_to_merge[0]['ticker'])
    col2 = list(dataframes_to_merge[1]['ticker'])
    for i in range(len(col1)):
        if col1[i] != col2[i]:
            print("missing row found" + str(i + 1))
            break
    
    # find duplicate columns in other dfs
    final_same = []
    for i in same_columns:
        final_same = list(set(i) & set(same_columns[0]))
     
    # drop repetitive columns
    for i in range(1, len(dataframes_to_merge)):
        dataframes_to_merge[i].drop(columns=final_same, inplace=True)

    big_df = pd.concat(dataframes_to_merge, axis=1).reindex(dataframes_to_merge[0].index)

    big_df.to_csv(intraday_path_all, index=False)
    
    return

        

create_new_intra_sum_csv("Volume")



            


