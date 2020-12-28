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

raw_folder_to_read = "Intraday_raw"
raw_folder_to_read = find_file(raw_folder_to_read)

intraday_path_formatted = historical_intraday_dir + "Intra_" 

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

    # perhaps if file already exists, I should rename it to "_old_version"
    file_to_write_to = intraday_path_formatted + attribute + ".csv"
    sum_csv = pd.DataFrame()
    # file exists so rename it 
    if os.path.exists(file_to_write_to):
        rename_existing_file = attribute + "_old_version.csv"
        rename_path = intraday_path_formatted + rename_existing_file
        os.rename(file_to_write_to, rename_path)

    dir_content = os.listdir(raw_folder_to_read)

    for csv_index, new_raw_csv in enumerate(dir_content):
        
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

        if csv_index % 10 == 0:
            print(str(csv_index + 1) + " out of " + str(len(dir_content)))

    sum_csv = sum_csv.set_index("date")
    sum_csv = sum_csv.sort_index()
    sum_csv.to_csv(file_to_write_to)
    return



'''
given single days all intraday data and day's starting timestamp
this function performs summation of volume from and before 9:30 am

returns dict of summation values
'''
def calc_vol_sums(ticker_df, millis_date):

    # milis since 9:30
    # overwriting the values as I iterate over the keys
    vol_dict_v2 = {
                "PM" : 0,
                "1 min" : 60 * 1000,
                "5 min" : 300 * 1000, 
                "15 min" : 900 * 1000, 
                "30 min" : 1800 * 1000,
                "60 min" : 3600 * 1000,
                "EOD" : 23400 * 1000
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

'''
given single days all intraday data and day's starting timestamp
this function performs summation of volume from and before 9:30 am

returns dict of summation values
'''    
def calc_intra_percent(intra_df, millis_date):

    highs_lows = {
        "Time of PMH" : 0,
        "PM High" : 0,
        "PMH to open" : 0,
        "Open to high" : 0,
        "Time of high" : 0,
        "Open to low" : 0,
        "Time of low" : 0,
        "Direction" : 0, 
    }

    nine_thirty = (32400 + 1800) * 1000 + millis_date
    four_pm = 57600 * 1000 + millis_date
    four_pm_list = intra_df.index[intra_df["datetime"] <= four_pm].tolist()
    four_pm_ind = four_pm_list[-1]

    nine_thirty_list = intra_df.index[intra_df["datetime"] >= nine_thirty].tolist()
    nin_30_ind = nine_thirty_list[0]
    nine_30_price = intra_df["open"][nin_30_ind]

    # PH high
    pm_high = intra_df["high"][:nin_30_ind].max()
    highs_lows["PM High"] = pm_high

    # time of PMH
    pm_high_list = intra_df.index[intra_df["high"] == pm_high].tolist()
    # if list is empty add to current 
    if pm_high_list == [] :
        pm_high_list.append(nin_30_ind)
    pm_high_index = pm_high_list[0]
    datetime = intra_df["datetime"][pm_high_index]
    highs_lows["Time of PMH"] = datetime

    # PMH to open
    highs_lows["PMH to open"] = (nine_30_price - highs_lows["PM High"]) / highs_lows["PM High"] * 100
    highs_lows["PMH to open"] = round(highs_lows["PMH to open"],2)

    # Open to high
    open_to_high = intra_df["high"][nin_30_ind:four_pm_ind].max()
    open_to_high_pct = (open_to_high - nine_30_price) / nine_30_price * 100
    highs_lows["Open to high"] = round(open_to_high_pct, 2)

    # Time of high, exclude pm
    high_list = intra_df.index[(intra_df["high"] == open_to_high) & (intra_df["datetime"] >= nine_thirty)].tolist()
    high_list_index = high_list[0]
    highs_lows["Time of high"] = intra_df["datetime"][high_list_index]

    # Open to low
    open_to_low = intra_df["low"][nin_30_ind:four_pm_ind].min()
    open_to_low_pct = (open_to_low - nine_30_price) / nine_30_price * 100
    highs_lows["Open to low"] = round(open_to_low_pct,2)

    # Time of low, exclude pm
    low_list = intra_df.index[(intra_df["low"] == open_to_low) & (intra_df["datetime"] >= nine_thirty)].tolist()
    low_list_index = low_list[0]
    highs_lows["Time of low"] = intra_df["datetime"][low_list_index]

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




# intraday_path_all = intraday_path_formatted + "All.csv"

# *comeback : this needs finishing up.
intraday_path_all = intraday_path_formatted + "All.csv"
def merge_exisiting_intraday():

    # file exists so rename it 
    if os.path.exists(intraday_path_all):
        rename_existing_file = intraday_path_formatted + "ALL_old_version.csv"
        rename_path = intraday_path_formatted + rename_existing_file
        os.rename(intraday_path_all, rename_path)

    # collect dfs for merging
    dataframes_to_merge = []
    same_columns = []
    for index, i in enumerate(possibilites):
        path = intraday_path_formatted + i + ".csv"
        df = pd.read_csv(path)
        df = df.set_index("date")
        df = df.sort_index()
        dataframes_to_merge.append(df)
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
    print(big_df.sample(5))

    big_df.to_csv(intraday_path_all)

    return


possibilites  = ["Volume" , "Highs_Lows"]
# create_new_intra_sum_csv(possibilites[0])
# create_new_intra_sum_csv(possibilites[1])
# list_to_merge = [intraday_path_formatted + pos + ".csv" for pos in possibilites]
# merge_exisiting_intraday() 




            


