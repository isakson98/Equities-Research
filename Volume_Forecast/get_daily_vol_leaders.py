import pandas as pd
from datetime import datetime
import heapq
import requests
import os

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
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

DIRECTORY_WITH_TICKERS = "ALL DATA/All_ticker_symbols/"
types = ["current", "delisted", "changes"]

# 2. run through all CURRENT and DELISTED stocks for that day
def get_leaders_for_period(metric, start, end=datetime.date(datetime.now())):

    current_df = pd.read_csv(DIRECTORY_WITH_TICKERS + types[0] + ".csv")
    # get all tickers that were not delisted at before this date
    delisted_df = pd.read_csv(DIRECTORY_WITH_TICKERS + types[1] + ".csv")
    # get all tickers that were not delisted at before this date
    changes_df = pd.read_csv(DIRECTORY_WITH_TICKERS + types[2] + ".csv")

    business_days = get_working_days(start, end)

    # from earliest to latest
    for date in business_days:
        todays_ticker_series = get_tickers_for_the_day(current_df, delisted_df, changes_df, date)
        get_leaders_for_one_day(date, todays_ticker_series, metric)

# first get all not yet delisted tickers = everything from first element until date given
# do changes
def get_tickers_for_the_day(current_df, delisted_df, changes_df, date):
    
    todays_ticker_series = current_df["Ticker"]

    '''
    basically everything delisted from the date in param until now must be added back
    '''
    delisted_list_indices = delisted_df.index[delisted_df["Date"] >= date].tolist()
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

    number_changes = len(changes_list_indices)
    hash_map = {}
    # vectorize this sucker
    for index, changes_row in changes_df.head(number_changes).iterrows():
        new_ticker = changes_row["New Symbol"]
        hash_map[new_ticker] = changes_row["Old Symbol"]

    for index, today_ticker in todays_ticker_series.items():
        if today_ticker in hash_map:
            print("Changing " + today_ticker + " for " + hash_map[today_ticker])
            todays_ticker_series[index] = hash_map[today_ticker]
            todays_ticker_series[index] = hash_map[today_ticker]


    return todays_ticker_series


TOP_ELEMENTS = 10
# for the delisted -> find index at which date matches date in delisted row
def get_leaders_for_one_day(date, symbols, metric):

    url = "https://fmpcloud.io/api/v3/historical-price-full/"
    
    params = {
        "from" : date,
        "to" : date,
        "apikey" : fmp_key,
    }

    heap_structure = []

    for ticker in symbols:
        url = url + ticker + "?"
        content = requests.get(url,params = params)
        data = content.json()
        # populate heap while it is not 10
        # i want the max heap, and theres no built in solution, so i invert the numbers to negative
        if len(heap_structure) < TOP_ELEMENTS:
            heapq.heappush(heap_structure, -data["volume"])
        # if new element has higher volume and heap is full, switch it
        elif data["volume"] > (heapq.nsmallest(1,heap_structure)) and len(heap_structure) >= TOP_ELEMENTS:
            heapq.heappushpop(heap_structure, -data["volume"])


    # when writing to file, undo the negatives for volume




# 2. create a heap of 10 elements for every day

# 3. run through all CURRENT and DELISTED stocks for that day
#   keep track of delisted index so that you can start moving 

# 4. save heap into a file as one row, seprated by commas


get_leaders_for_period("volume", "2020-10-09")