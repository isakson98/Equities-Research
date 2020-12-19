import os 
import pandas as pd
import datetime
import urllib
import requests
import time

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Volume_Forecast.get_daily_vol_leaders import get_working_days
from api_keys import tda_key

historical_directory = "ALL DATA/Historical/Daily_period/"

def delete_copies():
    csv_files = os.listdir(historical_directory)
    content = csv_files[0].split("_")
    start_date = content[1]
    end_date = content[2].split('.')
    end_date = end_date[0]

    for csv in csv_files:
        current_day = csv[-6:]
        print(current_day)
        # delete file
        if current_day == '18.csv':
            print(csv)
            os.remove(historical_directory+csv)

TOP_ELEMENTS = 100
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

# first 1 min volume
# first 5 min volume
# first 15 min volume
# first 30 min volume
# first 60 min volume
# EOD volume
# PM volume

historical_intraday_dir = "ALL DATA/Historical/Intraday_formated/"
ticker_100_path = "ALL DATA/Processed_datasets/Top 100 volume leaders.csv"
path_formatted = "ALL DATA/Historical/Intraday_formated/" 
path_formatted = path_formatted + "Intra_calc.csv" 
def get_specific_volume():

    tickers_leaders = pd.read_csv(ticker_100_path)

    # check last content of the file
    last_ticker_in_file = ""
    last_date_in_file = ""
    starting_index = 0
    try:
        with open(path_formatted) as f:
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
            starting_index =0

        for ticker_index, ticker in enumerate(tickers[starting_index:]):
            GLOBAL_ticker = ticker

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
            time.sleep(0.3)
            content_url = url + urllib.parse.urlencode(params)
            content = requests.get(content_url)
            ticker_json = content.json()
            try:
                ticker_df = pd.DataFrame(ticker_json["candles"])
            except:
                print(ticker_json)
                print("overflow")
                return
   
            vol_dict = perform_vol_sums(ticker_df, millis_date)

            sorted_list = [str(i[0]) for i in list(vol_dict.values())]
            # insert date as index at first place
            sorted_list.insert(0, date)
            sorted_list.insert(1, ticker)
            one_line = ","
            one_line = one_line.join(sorted_list)

            # create a file with headers
            if not os.path.exists(path_formatted):
                with open(path_formatted, "a") as writer:
                    first_content = ["date", "ticker"] + list(vol_dict.keys())
                    first_line = ","
                    first_line = first_line.join(first_content)
                    writer.write(first_line + "\n")

            with open(path_formatted, "a") as writer:
                writer.write(one_line + "\n")

            if ticker_index % 10 == 0:
                print(ticker_index)
        

GLOBAL_ticker = ""

 
def perform_vol_sums(ticker_df, millis_date):

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
            print(GLOBAL_ticker)
            print("selecting from prior times")
        else:
            index_new_time = index_new_time[0]
        vol_dict_v2[time][0] = ticker_df["volume"][nine_30_index:index_new_time].sum()

    # PM volume
    vol_dict_v2["PM"][0] = ticker_df["volume"][:nine_30_index].sum()
    # EOD volume
    vol_dict_v2["EOD"][0] = vol_dict_v2["EOD"][0] + vol_dict_v2["PM"][0]


    return vol_dict_v2



            

get_specific_volume()




