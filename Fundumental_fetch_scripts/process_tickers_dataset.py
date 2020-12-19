import pandas as pd
import datetime
import requests
import os
from bs4 import BeautifulSoup

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Fundumental_fetch_scripts.harvest_ticker_names import DIRECTORY_TO_SAVE_IN, ACTION_TYPE
from api_keys import fmp_key, tda_key

# original format "February 20, 2020" changed to fmp url type "2019-03-12" => year-mm-dd
def change_date_format():
    for action_path in ACTION_TYPE:
        path = DIRECTORY_TO_SAVE_IN + action_path + ".csv"
        df = pd.read_csv(path)

        for index in range(len(df["Date"])):
            old_time_version = df["Date"][index]
            new_time_version = datetime.datetime.strptime(old_time_version, "%b %d, %Y").strftime("%Y-%m-%d")
            df["Date"][index] = new_time_version

        df.to_csv(path, index = False)


# before this funtion, current ticker dataset would not have updated ticker symbols
def match_changes_w_current():

    current_df =  pd.read_csv(DIRECTORY_TO_SAVE_IN + "current.csv")
    changes_df = pd.read_csv(DIRECTORY_TO_SAVE_IN + ACTION_TYPE[0] + ".csv")
    hash_map = {}

    # get old ticker into a hash map, where the values are new ticker symbol
    for index_label, changes_row in changes_df.iterrows():
        old_ticker = changes_row["Old Symbol"]
        hash_map[old_ticker] = [changes_row["New Symbol"], changes_row["New Company Name"]]

    for index_label, current_row in current_df.iterrows():
        current_ticker = current_row["Ticker"]
        if current_ticker in hash_map:
            print("Changing " + current_ticker + " for " + hash_map[current_row["Ticker"]][0])
            current_df.at[index_label, "Ticker"] = hash_map[current_row["Ticker"]][0]
            current_df.at[index_label, "Name"] = hash_map[current_row["Ticker"]][1]

    current_df[["Ticker", "Name"]].to_csv(DIRECTORY_TO_SAVE_IN + "current.csv", index=False)
    return

# bingo! i can send a single api request with several ticekrs
# some stocks also have been delisted from the changes
PER_REQUEST = 500
def get_float_for_current():
    current_df =  pd.read_csv(DIRECTORY_TO_SAVE_IN + "current.csv")
    # create column if necessary
    if "Float" not in current_df:
        current_df["Float"] = 0

    for ticker_index in range(0, len(current_df), PER_REQUEST):
        td_fund = "https://api.tdameritrade.com/v1/instruments"

        ticker_window = current_df["Ticker"][ticker_index : ticker_index+PER_REQUEST]
        ticker_window = list(ticker_window)

        params = {
            "apikey" : tda_key,
            "symbol" : ticker_window,
            "projection" : "fundamental",
        }

        content = requests.get(td_fund, params=params)
        data = content.json()

        # create ticker float dict from the response
        ticker_float_dict = {}
        for ticker in data:
            ticker_float_dict[ticker] = data[ticker]['fundamental']['marketCapFloat']

        # map ticker float to its ticker name based on the key, incrementally
        current_df["Float"][ticker_index : ticker_index+PER_REQUEST] = current_df["Ticker"][ticker_index : ticker_index+PER_REQUEST].map(ticker_float_dict)

    # manual verification
    print(current_df.head(30))
    print(current_df.tail(30))

    current_df.to_csv(DIRECTORY_TO_SAVE_IN + "current.csv", index=False)

def get_missing_float():
    current_df =  pd.read_csv(DIRECTORY_TO_SAVE_IN + "current.csv")
    list_missing = current_df.index[current_df["Float"] < 0.01].tolist()
    td_fund = "https://api.tdameritrade.com/v1/instruments"

    ticker_missing = []
    for index in list_missing:
        ticker_missing.append(current_df["Ticker"][index])


    params = {
        "apikey" : tda_key,
        "symbol" : ticker_missing,
        "projection" : "fundamental",
    }

    content = requests.get(td_fund, params=params)
    data = content.json()

    # create ticker float dict from the response
    ticker_float_dict = {}
    for ticker in data:
        ticker_float_dict[ticker] = data[ticker]['fundamental']['marketCapFloat']


    print(ticker_float_dict)


get_missing_float()