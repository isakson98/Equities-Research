import pandas as pd 
import os
import requests
import time

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory

from Fundumental_fetch_scripts.get_fundumentals import categories_dict, sub_categories_dict
from Data_Access.Find_file_path import find_file
from api_keys import tda_key

api_key = tda_key

local_path_to_income_dir = "Income Statement/Current Stocks"
TOTAL_QUARTERS = 4

directory = "Processed_datasets"

def check_all_current_for_growth():

    full_path_to_income_dir = find_file(local_path_to_income_dir)
    directory_to_save_in = find_file(directory)

    file_winners = directory_to_save_in + "Growing revenue " + str(TOTAL_QUARTERS) +  " quarters.txt"
    files = os.listdir(full_path_to_income_dir)

    for index, income_file in enumerate(files):
        path_to_file = full_path_to_income_dir + income_file
        split_list = income_file.split("_")
        ticker_name = split_list[0]

        if is_ticker_growing_rev(path_to_file):
            with open(file_winners, "a+") as writer:
                writer.write(ticker_name + "\n")

        if index % 100 == 0:
            print(index)

check_all_current_for_growth()

def is_ticker_growing_rev(path_to_file):

    income_df = pd.read_csv(path_to_file)
    shape_tuple = income_df.shape
    row_size = shape_tuple[0]

    # make sure you do not check 4 quarters when only less is there
    if row_size < TOTAL_QUARTERS:
        return False

    prev = 0
    for i in range(TOTAL_QUARTERS-1, -1, -1):
        # if revenue greater than later quarter, not growth stock
        if income_df["revenue"][i] <= prev: return False
        prev = income_df["revenue"][i]

    return True

# calling td ameritrade api to check if there is recent stock action
# if no action, its an old stock that should be removed from the list
def check_stock_relevancy(file_name):

    url = "https://api.tdameritrade.com/v1/marketdata/"

    params = {
        "apikey" : tda_key,
        "periodType" : "day",
        "period" : 1,
        "frequencyType" : "minute",
        "frequency" : 30
    }

    ticker_in_file = []
    with open(file_name, "r") as reader:
        ticker_in_file = reader.read()
        ticker_in_file = ticker_in_file.splitlines()

    ticker_to_stay = []
    for index, ticker in enumerate(ticker_in_file):
        time.sleep(0.5)
        ticker = ticker.upper()
        url = url + ticker + "/pricehistory?"
        content = requests.get(url, params=params)
        data = content.json()
        try:
            if data["empty"] == False:
                ticker_to_stay.append(ticker)
            else:
                print(ticker)
        except Exception as e:
            print(data)
            print(e)

        if index % 10 == 0:
            print(index)

    # with open(file_name, "w") as writer:
    #     for ticker in ticker_to_stay:
    #         writer.write(ticker.join("\n"))

    

