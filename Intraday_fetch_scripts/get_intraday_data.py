import os 
import pandas as pd
import datetime
import urllib
import requests
import time
from progress.bar import Bar
import numpy as np

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Intraday_fetch_scripts.get_daily_vol_leaders import get_working_days
from Data_Access.Find_file_path import find_file
from api_keys import tda_key

# local_historical_directory = "Daily_period"
# historical_directory = find_file(local_historical_directory)

def delete_copies(historical_directory):
    csv_files = os.listdir(historical_directory)
    for csv in csv_files:
        current_day = csv[-6:]
        print(current_day)
        # delete file
        if current_day == '18.csv':
            print(csv)
            os.remove(historical_directory + csv)

# TOP_ELEMENTS = 100
#  for all tickers in the dataset
def compare_daily_attribute(attribute, historical_directory, TOP_ELEMENTS):

    csv_files = os.listdir(historical_directory)
    content = csv_files[0].split("_")
    start_date = content[1]
    end_date = content[2].split('.')
    end_date = end_date[0]
    biz_days = get_working_days(start_date, end_date)

    
    for date_index, date in enumerate(biz_days):
        sorted_list = []
        for file_index, file_name in enumerate(csv_files):
            name_split = file_name.split("_")
            ticker_name = name_split[0]
            stock_df = pd.read_csv(historical_directory+file_name)
            indices_small_floats = stock_df.index[stock_df["date"] == date].tolist()

            if len(indices_small_floats) != 0:
                att_results = stock_df[attribute][indices_small_floats[0]]
                # populate heap while it is not 10
                # i want the max heap, and theres no built in solution, so i invert the numbers to negative
                if len(sorted_list) < TOP_ELEMENTS:
                    sorted_list.append([att_results, ticker_name])
                    sorted_list = sorted(sorted_list, key = lambda x : x[0])
                # if new element has higher volume and heap is full, switch it
                elif sorted_list[0][0] < att_results:   
                    sorted_list[0] = [att_results, ticker_name]
                    sorted_list = sorted(sorted_list, key = lambda x : x[0])

            if file_index % 100 == 0:
                print(file_index)

        sorted_list = [str(i[1]) for i in sorted_list]
        # insert date as index at first place
        sorted_list.insert(0, date)
        one_line = ","
        one_line = one_line.join(sorted_list)

        FILE_NAME = "Top " + str(TOP_ELEMENTS) + " " + attribute  + " leaders.csv"
        with open("ALL DATA/Processed_datasets/" + FILE_NAME, "a+") as writer:
            writer.write(one_line + "\n")
        print(date_index)

    return


# part_hist_intraday_dir = "Intraday_formated"
# historical_intraday_dir = find_file(part_hist_intraday_dir)

# ticker_100_final = "Top 100 volume leaders.csv"
# ticker_100_path = find_file(ticker_100_final)

# raw_folder_to_store = "Intraday_raw"
# raw_folder_to_store = find_file(raw_folder_to_store)

'''
reading top 100 volume tickers from the file,
getting their intraday data and posting 
'''
def get_whole_intraday(ticker_100_path, raw_folder_to_store):

    tickers_leaders = pd.read_csv(ticker_100_path)
    for index, row in tickers_leaders.iterrows():
        # get date and name of the ticker
        date = row[0]
        tickers = list(row[1:])
        millis_date = int(datetime.datetime.strptime(date, "%Y-%m-%d").timestamp()) * 1000

        for ticker_index, ticker in enumerate(tickers):

            file_name = ticker + "_" + date + "_intraday.csv"
            file_path = raw_folder_to_store + file_name
            if os.path.exists(file_path):
                continue

            url = "https://api.tdameritrade.com/v1/marketdata/{}/pricehistory?".format(ticker)
            params = {
                "apikey": tda_key,
                "periodType": "day",
                "period" : 1,
                "frequencyType" : "minute",
                "frequency" : 1,
                "endDate" : millis_date,
                "startDate": millis_date,
            }
            time.sleep(0.35)
            content_url = url + urllib.parse.urlencode(params)
            content = requests.get(content_url)
            ticker_json = content.json()
            try:
                ticker_df = pd.DataFrame(ticker_json["candles"])
                ticker_df.to_csv(file_path)
            except:
                print(ticker_json)
                print("overflow")
                return      

            if ticker_index % 10 == 0:
                print(ticker_index)
        print(index)


# intraday_path_formatted = "Intraday_formated"
# intraday_path_formatted = find_file(intraday_path_formatted)
# intraday_path_all = intraday_path_formatted + "Intra_All.csv"

# possibilites  = ["Volume" , "Highs_Lows"]
def merge_exisiting_intraday(possibilites, intraday_path_formatted, intraday_path_all):

    # collect dfs for merging
    dataframes_to_merge = []
    same_columns = []
    for index, i in enumerate(possibilites):
        file_name = "Intra_" + i + ".csv"
        path = intraday_path_formatted + file_name
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

        
def add_cols_to_formatted_dataset(dir_to_search, file_to_add_to, columns_to_add):

    intra_df = pd.read_csv(file_to_add_to)
    new_cols = list(set(columns_to_add) - set(intra_df.columns))
    # add new columns to existing df
    for col in new_cols:
        intra_df[col] = np.nan

    # _2020-11-18_2020-12-17.csv
    # adding new raw columns
    bar = Bar('Processing', max=len(intra_df))
    for index, row in intra_df.iterrows():
        date = intra_df.at[index, "date"]
        ticker = intra_df.at[index, "ticker"]
        daily_file_path = dir_to_search + ticker + "_2020-11-18_2020-12-17.csv"
        daily_df = pd.read_csv(daily_file_path)
        for col in new_cols:
            daily_index_list = daily_df.index[daily_df["date"] == date].tolist()
            daily_index = daily_index_list[0]
            intra_df.at[index, col] =  daily_df.at[daily_index, col]
        bar.next()
    bar.finish()
    
    intra_df.to_csv(file_to_add_to, index=False)



            


