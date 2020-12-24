# importing from same folder, but it is a subfolder
import requests
import pandas as pd
import urllib
import os

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory

from Fundumental_fetch_scripts.harvest_ticker_names import Get_ticker_dir_map
from Data_Access.Find_file_path import find_file
from api_keys import fmp_key

api_key = fmp_key


# every current dir will have all directories of the next list
starting_dir = ["ALL DATA/Fundumentals"]
categories = ["/Balance Sheet Statement",
              "/Cash Flow Statement",
              "/Income Statement",
]
sub_categories = ["/Current Stocks"]
fundies_categories = [starting_dir, categories, sub_categories]

initial_dir = (os.getcwd() + starting_dir[0])

#  hash map of key words to paths
#  keeps path and url tied together under one name
#  draw back ===> have to update the dictionary as more categories are added
#  downloading full csv, not shortened (contains link + quarter number)
#  ideally = have another dictionary per key declared separately,
#  which will kinds of like a struct for the fundie
#  -shorten is giving full thing for some reason?
categories_dict = {
    "Balance sheet" : {"Path" : initial_dir + categories[0], "URL" : "balance-sheet-statement-shorten/"} ,
    "Cash Flow" : {"Path" : initial_dir + categories[1], "URL" : "cash-flow-statement-shorten/"},
    "Income" : {"Path" : initial_dir + categories[2], "URL" : "income-statement-shorten/"},
}

period_types = {
    "Quarter" : "quarter",
    "Annual" : "annual"
}

# something that must be attached to the path 
#  draw back ===> have to update the dictionary as more sub_categories are added
sub_categories_dict = {
    "Current" : sub_categories[0],
}

fmp_general_url = "https://fmpcloud.io/api/v3/"
empty_data_counter = {}

def create_dirs_for_fundies(whole_directory, layer = 0, added_path = os.getcwd()):

    if layer == len(whole_directory):
        return

    for element in whole_directory[layer]:
        added_path += element
        if os.path.isdir(added_path) == False:
            # get current working directtory
            print("Double check you haven't renamed anything")
            input()
            os.mkdir(added_path)

        create_dirs_for_fundies(whole_directory, layer + 1, added_path)
        added_path = added_path.replace(element, "")

# download different types of statements into already created paths
def download_all_statements_companies_that_are(stock_status):

    hash_path = Get_ticker_dir_map()
    path_for_current = hash_path["Current"]

    if os.path.exists(path_for_current) == False:
        print("File does not exist or has been renamed")
        return

    ticker_data = pd.read_csv(path_for_current)
    symbol_column = ticker_data['Ticker']
    
    # fetch all ticker data
    try:
        for ticker_index, ticker in enumerate(symbol_column):
            if ticker == "WMT":
                print("stop")
            for fundie_type in categories_dict:
                # keep track of deleted stocks. write them in a file. then delete them from the main file
                download_csv(ticker, fundie_type, stock_status, "quarter")
            if (ticker_index % 100 == 0):
                print(ticker_index)
    except KeyboardInterrupt:
        print("Intentional exit")
    except Exception as e:
        print(e)


    
# args = ticker symbol, type of financial document, subcategory for the path
# fundie_type must conform to one of categories_dict keys
# stock_status must conform to one of sub_categories_dict keys
# period must conform to either "annual" or "quarter"
def download_csv(ticker, fundie_type, stock_status, period):

    # prepare directory
    final_csv_path = categories_dict[fundie_type]["Path"] + sub_categories_dict[stock_status] + "/" 
    # prepare file name
    file_name = ticker + "_" + fundie_type + "_" + period + ".csv"
    file_name = file_name.replace(" ", "_")
    file_name = file_name.lower()
    final_csv_path = final_csv_path + file_name

    if os.path.exists(final_csv_path):
        # local_dir = fundie_type + '/' + stock_status
        # print("{}.csv already exists in {} directory".format(ticker, local_dir))
        return 

    # if element in this hashmap, theres no data for at least one fundie, so i don't request data
    # could be that for some financials data missing and for some not?
    if empty_data_counter.get(ticker, 0) == 1:
        return 

    url = fmp_general_url+ categories_dict[fundie_type]["URL"] + ticker + "?"
    params = {
        "datatype" : "csv",
        "period" : period,
        "apikey" : api_key,
    }
    content_url = url + urllib.parse.urlencode(params)

    downloaded_csv = pd.read_csv(content_url, encoding="iso-8859-1")
    # if empty write record ones that do not have financials on the server
    # do not write if empty
    if downloaded_csv.empty == True:
        empty_data_counter[ticker] = 1
        return 

    downloaded_csv.to_csv(final_csv_path, index=True)

    return 

def clean_up_stocks_list(stock_status):

    hash_path = Get_ticker_dir_map()
    path_for_current = hash_path[stock_status]
    if os.path.exists(path_for_current) == False:
        print("List of tickers file does not exist or has been renamed")
        return
    ticker_data = pd.read_csv(path_for_current)

    # removing ticker one by one from the main list
    tickers_to_delete = list(empty_data_counter.keys())
    for ticker in tickers_to_delete:
        # finds the index of the ticker and returns a list of them
        location_list = ticker_data.index[ticker_data['Ticker'] == ticker].tolist()
        ticker_data.drop(index = location_list[0], inplace=True)

    # append names of failed stocks in the text file
    # append only those that are not in the file 
    deleted_stocks_file = "Failed " + stock_status + " Stocks" + ".txt"
    with open(deleted_stocks_file, "a+") as f:
        f.seek(0)
        failed_content = f.read()
        failed_content = failed_content.splitlines()

        for ticker in tickers_to_delete:
            if ticker not in failed_content:
                f.write(ticker + '\n')

    ticker_data.to_csv(path_for_current, index=False)


# create_dirs_for_fundies(fundies_categories)
# type_to_work_on = "Current"
# download_all_statements_companies_that_are(type_to_work_on)
# clean_up_stocks_list(type_to_work_on)


