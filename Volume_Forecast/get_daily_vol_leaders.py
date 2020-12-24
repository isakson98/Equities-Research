import pandas as pd
from datetime import datetime
import heapq
import requests
import urllib
import os

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Data_Access.Find_file_path import find_file
from api_keys import fmp_key


# 1. find all working days in the past 30 days
def get_working_days(start, end=datetime.date(datetime.now())):

    fd = pd.bdate_range(start, end)
    print(type(fd))
    business_days = []
    # each "time" is "YYYY 00:00:00" so it needs to be split
    for time in fd:
        time = str(time)
        time_arr = time.split(" ")
        business_days.append(time_arr[0])

    return business_days

DIRECTORY_WITH_TICKERS = "All_ticker_symbols"
types = ["current", "delisted", "changes"]

# 2. run through all CURRENT and DELISTED and CHANGES stocks for that day
def get_leaders_for_period(metric, start, end=datetime.date(datetime.now())):

    final_dir_with_tickers = find_file(DIRECTORY_WITH_TICKERS)

    current_df = pd.read_csv(final_dir_with_tickers + types[0] + ".csv")
    # get all tickers that were not delisted at before this date
    delisted_df = pd.read_csv(final_dir_with_tickers + types[1] + ".csv")
    # get all tickers that were not delisted at before this date
    changes_df = pd.read_csv(final_dir_with_tickers + types[2] + ".csv")

    business_days = get_working_days(start, end)

    # from earliest to latest
    for index, date in enumerate(business_days):
        todays_ticker_series = get_tickers_for_the_day(current_df, delisted_df, changes_df, date)
        get_leaders_for_one_day(date, todays_ticker_series, metric)
        print("day " + str(index))

# first get all not yet delisted tickers = everything from first element until date given
# do changes
def get_tickers_for_the_day(current_df, delisted_df, changes_df, date):
    
    todays_ticker_series = current_df["Ticker"]

    '''
    basically everything delisted from the date in param until now must be added back
    '''
    delisted_list_indices = delisted_df.index[delisted_df["Date"] >= date].tolist()
    # nothing has been delisted yet
    if len(delisted_list_indices) != 0:
        # index at which the earlist stock has been delisted
        index_last_date = delisted_list_indices[-1] 
        # append to existing stocks all stocks that haven't yet been delisted
        not_yet_delisted_df = delisted_df["Stock Symbol"][:index_last_date + 1]
        todays_ticker_series = todays_ticker_series.append(not_yet_delisted_df, ignore_index=True) 

    '''
    basically all changes that happened to stocks in current
    must be changed to old names until date given -> reconstructing the past, mind blown
    '''
    # getting list of indices that have changes
    changes_list_indices = changes_df.index[changes_df["Date"] >= date].tolist()
    if len(changes_list_indices) != 0:
        number_changes = len(changes_list_indices)
        hash_map = {}
        # vectorize this sucker
        for index, changes_row in changes_df.head(number_changes).iterrows():
            new_ticker = changes_row["New Symbol"]
            hash_map[new_ticker] = changes_row["Old Symbol"]

        for index, today_ticker in todays_ticker_series.items():
            if today_ticker in hash_map:
                todays_ticker_series[index] = hash_map[today_ticker]
                todays_ticker_series[index] = hash_map[today_ticker]

    return todays_ticker_series


MAX_FLOAT = 100
historical_path = "Daily_period"
def get_csv_based_on_float(start, end=datetime.date(datetime.now())):

    final_dir_with_tickers = find_file(DIRECTORY_WITH_TICKERS)
    final_historical = find_file(historical_path)

    current_df = pd.read_csv(final_dir_with_tickers + types[0] + ".csv")
    indices_small_floats = current_df.index[current_df["Float"] < MAX_FLOAT].tolist()

    ticker_missing = []
    for index in indices_small_floats:
        ticker_missing.append(current_df["Ticker"][index])

    list_of_empty = []
    for index, ticker in enumerate(ticker_missing):
        file_name = final_historical + "/" + ticker + "_" + start + "_" + str(end) + ".csv"
        if os.path.exists(file_name):
            print("skip " + ticker)
            continue

        url = "https://fmpcloud.io/api/v3/historical-price-full/" + ticker + "?"
        
        params = {
            "from": start,
            "to": end,
            "datatype": "csv",
            "apikey": fmp_key
        }

        content_url = url + urllib.parse.urlencode(params)
        downloaded_csv = pd.read_csv(content_url, encoding="iso-8859-1")
        if len(downloaded_csv) < 2:
            print(ticker)
            list_of_empty.append(ticker)
        downloaded_csv.to_csv(file_name)
        if index % 100 == 0:
            print(index)

    with open("Empty historical " + str(start) + "_" + str(end), "a+") as writer:
        writer.write(list_of_empty)


PATH_TO_WRITE_TO_LEADERS = "Processed_datasets"
TOP_ELEMENTS = 20
# for the delisted -> find index at which date matches date in delisted row
def get_leaders_for_one_day(date, symbols, metric):

    final_path_to_write = find_file(PATH_TO_WRITE_TO_LEADERS)

    url = "https://fmpcloud.io/api/v3/historical-price-full/"
    
    params = {
        "from" : date,
        "to" : date,
        "apikey" : fmp_key,
    }

    sorted_list = []

    track_empty_results = []
    for index, ticker in enumerate(symbols):
        # url = url + ticker + "?"
        content = requests.get(url + ticker + "?",params = params)
        data = content.json()
        # empty results
        if len(data) == 0:
            track_empty_results.append(ticker)
            continue
        try:
            attribute_compared = data["historical"][0][metric]
        except:
            print(data)

        # populate heap while it is not 10
        # i want the max heap, and theres no built in solution, so i invert the numbers to negative
        if len(sorted_list) < TOP_ELEMENTS:
            sorted_list.append([attribute_compared, ticker])
            sorted_list = sorted(sorted_list, key = lambda x : x[0])
        # if new element has higher volume and heap is full, switch it
        elif sorted_list[0][0] < attribute_compared:
            sorted_list[0] = [attribute_compared, ticker]
            sorted_list = sorted(sorted_list, key = lambda x : x[0])

        print("Ticker in a day: " + str(index) + " " + str(attribute_compared))

    sorted_list = [str(i[1]) for i in sorted_list]
    # insert date as index at first place
    sorted_list.insert(0, date)
    one_line = ","
    one_line = one_line.join(sorted_list)

    FILE_NAME = "Top " + TOP_ELEMENTS +  " volume leaders.csv"
    with open(final_path_to_write + FILE_NAME, "a") as writer:
        writer.write(one_line + "\n")

