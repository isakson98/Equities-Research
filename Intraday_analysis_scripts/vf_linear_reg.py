import pandas as pd 
import numpy as np
import pickle
import os
from sklearn import preprocessing, model_selection
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
from progress.bar import Bar

import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Data_Access.Find_file_path import find_file

'''
example
list_of_cols = ["PM", "1 min","5 min","15 min","30 min","60 min","EOD"]

'''
def vf_every_time_period(file_to_read, list_of_cols, folder_to_write):

    intraday_df = pd.read_csv(file_to_read)
    volume_only = intraday_df.drop(['date', 'ticker'], axis=1)
    volume_only = volume_only[list_of_cols]
    volume_only = volume_only.dropna()

    # calculates volume forecast for each row from 5min to eod
    for i in range(volume_only.shape[1] - 1):
        print(i)
        X = np.array(volume_only.iloc[:, :-1])
        y = np.array(volume_only.iloc[:, -1:])
        # X = preprocessing.scale(X)

        y_name = volume_only.columns
        y_name = y_name[-1]
        file_name = 'vf_till_' + y_name + '.pickle'

        if os.path.exists(folder_to_write + file_name):
            volume_only = volume_only.iloc[:, :-1]
            continue
        
        classifier = LinearRegression()
        prev_best = 0
        # run through several times to get the best result of training
        for i in range(20):
            X_train, X_test, y_train, Y_test = model_selection.train_test_split(X, y, test_size=0.1)

            classifier = LinearRegression()
            classifier.fit(X_train, y_train) # run the model, most tedious step
            accuracy = classifier.score(X_test, Y_test) # test, out of sample data
            print(y_name + " " + str(accuracy))
            if accuracy > prev_best:
                prev_best = accuracy
                with open(folder_to_write + file_name, 'wb') as f:
                    pickle.dump(classifier, f)
            
        volume_only = volume_only.iloc[:, :-1]
    return


def vf_gradual_till_EOD(file_to_read, list_of_cols, folder_to_write):

    intraday_df = pd.read_csv(file_to_read)
    volume_only = intraday_df.drop(['date', 'ticker'], axis=1)
    volume_only = volume_only[list_of_cols]
    volume_only = volume_only.dropna()

    y = np.array(volume_only.iloc[:, -1:])
    # calculates volume forecast for each row from 5min to eod
    for i in range(volume_only.shape[1] - 2):
        print(i)
        X = np.array(volume_only.iloc[:, :-1])
        X = preprocessing.scale(X)

        x_name = volume_only.columns
        x_name = x_name[-2]
        file_name = 'EOD_vf_w_float_until_' + x_name + '.pickle'

        if os.path.exists(folder_to_write + file_name):
            volume_only = volume_only.iloc[:, :-1]
            continue

        X_train, X_test, y_train, Y_test = model_selection.train_test_split(X, y, test_size=0.1)

        classifier = LinearRegression()
        classifier.fit(X_train, y_train) # run the model, most tedious step
        accuracy = classifier.score(X_test, Y_test) # test, out of sample data
        print(x_name + " " + str(accuracy))

        with open(folder_to_write + file_name, 'wb') as f:
            pickle.dump(classifier, f)

        volume_only = volume_only.iloc[:, :-1]
    return

# float_loc = "current.csv"
# float_loc = find_file(float_loc)

def add_float_to_volume_df(intra_vol, float_loc, new_vol_intra):

    if os.path.exists(new_vol_intra):
        print(new_vol_intra + " exists. do you want to continue?")
        return

    float_df = pd.read_csv(float_loc)
    float_df = float_df[["Ticker", "Float"]]
    float_df.rename(columns={"Ticker":"ticker"}, inplace=True)
    float_df.set_index('ticker')
    float_df.sort_index()

    vol_df = pd.read_csv(intra_vol)
    vol_df.set_index('ticker')
    vol_df.sort_index()

    vol_df = vol_df.merge(float_df, on='ticker', how='left')
    vol_df.set_index("date")
    vol_df.sort_index()
    empty_vol = vol_df[vol_df["Float"] ==0]

    vol_df.to_csv(new_vol_intra)
    return

'''
criteria = "Direction"

'''
def compare_vf_with_real(file_to_examine, criteria):

    all_intra_df = pd.read_csv(file_to_examine)

    mean_eod = all_intra_df["EOD"].median()
    print("Median  EOD")
    print(round(mean_eod))

    list_to_track = ["PM", "1 min","5 min","15 min", "30 min", "60 min","EOD"]
    bulls_volume_dict = dict.fromkeys(list_to_track)
    bears_volume_dict = dict.fromkeys(list_to_track)

    for period in bulls_volume_dict:
        bulls_df = all_intra_df[(all_intra_df["Direction"] == 1) & (all_intra_df["EOD"] > mean_eod)] 
        bulls_volume_dict[period] = (round(bulls_df[period].mean()))

        bears_df = all_intra_df[(all_intra_df["Direction"] == -1) & (all_intra_df["EOD"] > mean_eod)]
        bears_volume_dict[period] = (round(bears_df[period].mean()))
    
    plt.style.use('fivethirtyeight')

    bull_vals = list(bulls_volume_dict.values())
    bear_vals = list(bears_volume_dict.values())

    plt.plot(list(bulls_volume_dict.keys()), bull_vals, label="Runners")
    plt.plot(list(bears_volume_dict.keys()), bear_vals, label="Faders")

    plt.xlabel("Time period from open")
    plt.ylabel("Median Volume")
    plt.legend()
    # plt.savefig("ALL DATA/Graphs/Median cases) intraday volume for runners&faders.png")

    plt.title("Intraday volume for lowfloats")
    plt.show()
    return

'''
find the difference between forecast and real volume for all time period as pct of forecast
compare pct for each time period with direction of the stock
*keep in mind direction is 3-1 ratio. whatever happens past it is uknown

find the difference between vf and real volume
cross reference it with direction
calculates volume forecast for each row from 5min to eod
the context is, given all the volume before the given, column
what should its volume be vs what is actually is?

'''
def get_vf_vs_real_every_period(volume_file, price_file, dest_file, weight_dir):

    # if file exists so rename it 
    # need to split apart the path
    if os.path.exists(dest_file):
        dest_file_list = dest_file.split("/")
        dest_file_list[-1] = "Old_version_" + dest_file_list[-1]
        symbol = "/"
        new_file_name = symbol.join(dest_file_list)
        os.rename(dest_file, new_file_name)

    vol_df = pd.read_csv(volume_file)
    vol_df = vol_df.drop(columns=["ticker", "date"])

    price_df = pd.read_csv(price_file)
    ticker_direction_df = price_df[["date", "ticker", "Direction"]]

    old_cols = vol_df.columns
    old_cols = old_cols[1:] # without pm
    new_cols = [old_col + " real/vf ratio" for old_col in old_cols]

    differences_df = pd.DataFrame(columns = new_cols)
    
    bar = Bar('Processing', max=len(vol_df))
    for row_index, row in vol_df.iterrows():
        ratio_row_dict = dict.fromkeys(new_cols)
        for col_index in range(len(row)-1):
            proper_dim_arr = [[col] for col in row[:col_index + 1]]
            X = np.array(proper_dim_arr).transpose()

            model_path = weight_dir + "vf_till_{}.pickle".format(old_cols[col_index])
            load_model = pickle.load(open(model_path, 'rb'))
            forecast = load_model.predict(X)
            # real volume of the candle
            real_vol = row[col_index + 1]
            # find ratio basing on forecast
            vf_vs_real_ratio = real_vol / forecast

            dict_index = new_cols[col_index]
            # vf_vs_real_ratio is 1 * 1 array, so i get the first index
            ratio_row_dict[dict_index] = round(vf_vs_real_ratio[0][0], 2)

            # print("Coefficients: " + str(load_model.coef_))
            # print("Forecast: " + str(forecast))
            # print("Real Volume: " + str(real_vol))
            # print("Ratio: " + str(vf_vs_real_ratio[0][0]))

        differences_df = differences_df.append(ratio_row_dict, ignore_index=True)
        bar.next()

    bar.finish()
        
    # merge direction with difference_df
    list_to_concat = [price_df, differences_df, vol_df]
    result = pd.concat(list_to_concat, axis=1)
    result.to_csv(dest_file)
    return
    

# i have vf from all metrics to EOD
# def vf_vs_real_till_EOD(): 


def vf_real_ratio_describe(file_to_open, categories):

    # initialize 
    ratio_keys = [ i + " real/vf ratio" for i in categories]
    bull_dict = dict.fromkeys(ratio_keys)
    bear_dict = dict.fromkeys(ratio_keys)
    big_df = pd.read_csv(file_to_open)
    median_vol = big_df["EOD"].median() 

    bull_df = big_df[(big_df["vwap"] > big_df["open"] ) & (big_df["EOD"] >= median_vol)]
    # bull_df = bull_df.set_index("EOD")
    # bull_df = bull_df.sort_index(ascending=False)
    # bull_df = bull_df.head(200)

    bear_df = big_df[(big_df["vwap"] < big_df["open"] ) & (big_df["EOD"] >= median_vol)]
    # bear_df = bear_df.set_index("EOD")
    # bear_df = bear_df.sort_index(ascending=False)
    # bear_df = bear_df.head(200)


    # difference between ratios
    for column_name in ratio_keys:
        # print(big_df[column_name].describe())
        print("VWAP at close > stock price at open: ")
        print(bull_df[column_name].describe())
        print()
        print("VWAP at close < stock price at open: ")
        print(bear_df[column_name].describe())

        # print("Bulls vf vs real ratio VS bears vf vs real ratio")
        # print("As + or - percentage of bears summary: ")
        # print((bull_df[column_name].describe() - bear_df[column_name].describe()) / bear_df[column_name].describe() * 100)

        print()
        print()

    # print(bull_df.tail())
    # print(bear_df.tail())

    # x values -> ratios from 1min to eod
    # y values -> median OR mean for ADR and ADF per each time interval

    return

def find_vf_real_corr(file_to_open, categories):
     



    return