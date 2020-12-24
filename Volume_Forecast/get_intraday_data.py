import os 
import pandas as pd
import datetime
import urllib
import requests
import time

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Volume_Forecast.get_daily_vol_leaders import get_working_days
from Data_Access.Find_file_path import find_file
from api_keys import tda_key

local_historical_directory = "Daily_period"
historical_directory = find_file(local_historical_directory)

def delete_copies():

    csv_files = os.listdir(historical_directory)
    content = csv_files[0].split("_")
    for csv in csv_files:
        current_day = csv[-6:]
        print(current_day)
        # delete file
        if current_day == '18.csv':
            print(csv)
            os.remove(historical_directory + csv)

TOP_ELEMENTS = 100
#  for all tickers in the dataset
def compare_daily_attribute(attribute):

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


part_hist_intraday_dir = "Intraday_formated"
historical_intraday_dir = find_file(part_hist_intraday_dir)

ticker_100_final = "Top 100 volume leaders.csv"
ticker_100_path = find_file(ticker_100_final)

intraday_path_formatted = historical_intraday_dir + "Intra_" 

possibilites  = ["Volume" , "Highs_Lows"]

'''
Reading data from top 100 daily tickers of X days
And performing on them with their intraday data,
right now I can calculate volume and pricea data
'''
def get_intra_attributes(attribute):

    file_to_write_to = intraday_path_formatted + attribute + ".csv"
    tickers_leaders = pd.read_csv(ticker_100_path)

    # check last content of the file
    last_ticker_in_file = ""
    last_date_in_file = ""
    starting_index = 0
    try:
        with open(file_to_write_to) as f:
            for line in f:
                pass
            last_line = line
            last_line = last_line.split(",")
            last_date_in_file = last_line[0]
            last_ticker_in_file = last_line[1]
    except:
        print("File does not exist")

    first_time = True
    for index, row in tickers_leaders.iterrows():
        # get date and name of the ticker
        date = row[0]
        tickers = list(row[1:])
        millis_date = int(datetime.datetime.strptime(date, "%Y-%m-%d").timestamp()) * 1000

        # goes in sync with the last date AND stock in the file
        if last_date_in_file > date and first_time:
            continue
        elif last_date_in_file == date: 
            starting_index = tickers.index(last_ticker_in_file) + 1
            first_time = False
        else:
            starting_index = 0

        for ticker_index, ticker in enumerate(tickers[starting_index:]):

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
            time.sleep(0.25)
            content_url = url + urllib.parse.urlencode(params)
            content = requests.get(content_url)
            ticker_json = content.json()
            try:
                ticker_df = pd.DataFrame(ticker_json["candles"])
            except:
                print(ticker_json)
                print("overflow")
                return

            # needed dict
            attribute_dict = {}
            sorted_list = []
            if attribute == possibilites[0]:
                attribute_dict = calc_vol_sums(ticker_df, millis_date)
                sorted_list = [str(i[0]) for i in list(attribute_dict.values())]
            elif attribute == possibilites[1]:
                attribute_dict = calc_intra_percent(ticker_df, millis_date)
                sorted_list = [str(i) for i in list(attribute_dict.values())]

            # insert date as index at first place
            sorted_list.insert(0, date)
            sorted_list.insert(1, ticker)
            one_line = ","
            one_line = one_line.join(sorted_list)

            # create a file with headers
            if not os.path.exists(file_to_write_to):
                with open(file_to_write_to, "a") as writer:
                    first_content = ["date", "ticker"] + list(attribute_dict.keys())
                    first_line = ","
                    first_line = first_line.join(first_content)
                    writer.write(first_line + "\n")

            with open(file_to_write_to, "a") as writer:
                writer.write(one_line + "\n")

            if ticker_index % 10 == 0:
                print(ticker_index)

 
def calc_vol_sums(ticker_df, millis_date):

    # [amount of volume, milis since 9:30]
    vol_dict_v2 = {"1 min" : [0, 60 * 1000],
                "5 min" : [0, 300 * 1000], 
                "15 min" : [0, 900 * 1000], 
                "30 min" : [0, 1800 * 1000],
                "60 min" : [0, 3600 * 1000],
                "EOD" : [0, 23400 * 1000],
                "PM" : [0, 0]
                }

    nine_thirty = (32400 + 1800) * 1000 + millis_date
    nine_30_index = ticker_df.index[ticker_df["datetime"] >= nine_thirty].tolist()
    nine_30_index = nine_30_index[0]
    # partially calculates EOD and doesn't calculate 
    for time in vol_dict_v2:
        new_time =  nine_thirty + vol_dict_v2[time][1]
        index_new_time = ticker_df.index[ticker_df["datetime"] >= new_time].tolist()
        if len(index_new_time) == 0:
            index_new_time = ticker_df.index[ticker_df["datetime"] < new_time].tolist()
            index_new_time = index_new_time[-1]
            print("selecting from prior times")
        else:
            index_new_time = index_new_time[0]
        vol_dict_v2[time][0] = ticker_df["volume"][nine_30_index:index_new_time].sum()

    # PM volume
    vol_dict_v2["PM"][0] = ticker_df["volume"][:nine_30_index].sum()
    # EOD volume, in the loop only calculated from 9:30, not all day
    vol_dict_v2["EOD"][0] = vol_dict_v2["EOD"][0] + vol_dict_v2["PM"][0]

    return vol_dict_v2
    
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

        





            


