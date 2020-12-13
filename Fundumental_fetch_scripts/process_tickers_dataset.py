import pandas as pd
import datetime
import requests
import os

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Fundumental_fetch_scripts.harvest_ticker_names import DIRECTORY_TO_SAVE_IN, ACTION_TYPE

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
