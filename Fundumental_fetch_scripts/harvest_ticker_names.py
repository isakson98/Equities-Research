from bs4 import BeautifulSoup
import requests
import pandas as pd
import os

ALL_STOCKS_URL = "https://stockanalysis.com/stocks"
SELECTED_STOCKS_PARTIAL_URL = "https://stockanalysis.com/actions/"
ACTION_TYPE = ["changes", "spinoffs", "splits", "delisted"]

DIRECTORY_TO_SAVE_IN = "ALL DATA/All_ticker_symbols/"

# grabbing list of stocks from stockanalysis.com
def Harvest_ticker_names():
    if os.path.isdir(DIRECTORY_TO_SAVE_IN) == False:
        # get current working directtory
        path = os.getcwd()
        path += DIRECTORY_TO_SAVE_IN
        os.mkdir(path)
        return

    Harvest_all_stocks()
    for stock_type in ACTION_TYPE:
        Harvest_selected_stocks(stock_type)

"""
Fetches one list of > 6k stocks and saves it into a file as is
seperate function because the format is different on this page
"""
def Harvest_all_stocks():
    # check if file already exists
    name_of_file = DIRECTORY_TO_SAVE_IN + "current.csv"
    if os.path.exists(name_of_file):
        return

    # get the page html content and filter it through bs4
    page_html = requests.get(ALL_STOCKS_URL).text
    parse = BeautifulSoup(page_html, 'lxml')

    # find the table with with list of stocks
    table = parse.find(class_="no-spacing")

    # retrieve ticker and name as text from every table entry tag "li"
    ticker_and_name_list = []
    for ticker_name_tag in table.find_all("li"):
        ticker_and_name = ticker_name_tag.a.get_text()
        name_split = ticker_and_name.split(" - ")
        dict_row = {"Ticker" : name_split[0], "Name" : name_split[1]}
        ticker_and_name_list.append(dict_row)

    # names based on individual dict's keys
    column_names = list(ticker_and_name_list[0].keys())

    # initializing a dataframe with a list of dictionaries as rows
    ticker_and_name_df = pd.DataFrame(ticker_and_name_list, columns=column_names)
    ticker_and_name_df.to_csv(name_of_file)


"""
Fetches data from 4 different web pages on the same website
Saves the data as is into 4 differently named csv files
"""
def Harvest_selected_stocks(stock_type):
    name_of_file = DIRECTORY_TO_SAVE_IN + stock_type + ".csv"
    if os.path.exists(name_of_file):
            return

    page_html = requests.get(SELECTED_STOCKS_PARTIAL_URL + stock_type).text
    parse = BeautifulSoup(page_html, 'lxml')

    # find the table with with list of stocks
    table = parse.tbody

    # determine columns for the file : first tr in the table is the names
    column_names = []
    column_name_tags = table.find("tr")
    for col_name in column_name_tags:
            col_name_string  = col_name.get_text()
            column_names.append(col_name_string)

    # iterate over every row
    selected_list = []
    for table_row in table:
        stock_dict = {}
        # parse each row in this loop
        for col_index, col_string in enumerate(table_row):
            col_name_string  = col_string.get_text()
            # attribute each element of the row to columns name
            name_of_column = column_names[col_index] 
            stock_dict[name_of_column] = col_name_string
        selected_list.append(stock_dict)

    # initializing a dataframe with a list of dictionaries as rows
    ticker_and_name_df = pd.DataFrame(selected_list[1:], columns=column_names)
    ticker_and_name_df.to_csv(name_of_file, index=False)

def Get_ticker_dir_map():
    # get list of all current files with csv extentions

    # add directory path to their string
    current_path = os.getcwd() + "/" + DIRECTORY_TO_SAVE_IN

    files = os.listdir(current_path)
    csv_files = [single_csv for single_csv in files if single_csv.endswith(".csv")]

    hash_map = {}
    for csv_file in csv_files:
        formatted_name = csv_file.replace(".csv", "")
        formatted_name = formatted_name.capitalize()
        hash_map[formatted_name] = current_path + csv_file

    return hash_map


# Get_ticker_dir_map()
# Harvest_ticker_names()

