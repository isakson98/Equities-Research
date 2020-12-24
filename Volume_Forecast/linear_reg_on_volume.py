import pandas as pd 
import numpy as np
import pickle
import os
from sklearn import preprocessing, model_selection
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt


import sys
sys.path.append(os.getcwd()) # current directory must be root project directory
from Data_Access.Find_file_path import find_file


part_hist_intraday_dir = "Intraday_formated"
intraday_vol_path = find_file(part_hist_intraday_dir)

intra_all = "Intra_All.csv"
intra_vol = "Intra_Volume.csv" 
new_vol_intra = "Intra_Vol_Float.csv"

weights_path = "VF_with_float"
weights_path = find_file(weights_path)

def vf_every_time_period(file_to_read):

    intraday_df = pd.read_csv(intraday_vol_path + file_to_read)
    volume_only = intraday_df.drop(['date', 'ticker'], axis=1)
    volume_only = volume_only[["Float","PM", "1 min","5 min","15 min","30 min","60 min","EOD"]]
    volume_only = volume_only.dropna()

    # calculates volume forecast for each row from 5min to eod
    for i in range(volume_only.shape[1] - 1):
        X = np.array(volume_only.iloc[:, :-1])
        y = np.array(volume_only.iloc[:, -1:])
        X = preprocessing.scale(X)

        y_name = volume_only.columns
        y_name = y_name[-1]
        file_name = 'vf_w_float_till_' + y_name + '.pickle'

        if os.path.exists("ALL DATA/ML_weights/VF_with_float/" + file_name):
            volume_only = volume_only.iloc[:, :-1]
            continue

        X_train, X_test, y_train, Y_test = model_selection.train_test_split(X, y, test_size=0.1)

        classifier = LinearRegression()
        classifier.fit(X_train, y_train) # run the model, most tedious step
        accuracy = classifier.score(X_test, Y_test) # test, out of sample data
        print(y_name + " " + str(accuracy))

        with open(weights_path + file_name, 'wb') as f:
            pickle.dump(classifier, f)

        volume_only = volume_only.iloc[:, :-1]


def vf_gradual_till_EOD(file_to_read):

    intraday_df = pd.read_csv(intraday_vol_path + file_to_read)
    volume_only = intraday_df.drop(['date', 'ticker'], axis=1)
    volume_only = volume_only[["Float","PM", "1 min","5 min","15 min","30 min","60 min", "EOD"]]
    volume_only = volume_only.dropna()

    y = np.array(volume_only.iloc[:, -1:])
    # calculates volume forecast for each row from 5min to eod
    for i in range(volume_only.shape[1] - 2):
        X = np.array(volume_only.iloc[:, :-1])
        X = preprocessing.scale(X)

        x_name = volume_only.columns
        x_name = x_name[-2]
        file_name = 'EOD_vf_w_float_until_' + x_name + '.pickle'

        if os.path.exists(file_name):
            volume_only = volume_only.iloc[:, :-1]
            continue

        X_train, X_test, y_train, Y_test = model_selection.train_test_split(X, y, test_size=0.1)

        classifier = LinearRegression()
        classifier.fit(X_train, y_train) # run the model, most tedious step
        accuracy = classifier.score(X_test, Y_test) # test, out of sample data
        print(x_name + " " + str(accuracy))

        with open(weights_path + file_name, 'wb') as f:
            pickle.dump(classifier, f)

        volume_only = volume_only.iloc[:, :-1]

float_loc = "current.csv"
float_loc = find_file(float_loc)

def add_float_to_volume_df():

    if os.path.exists(intraday_vol_path + new_vol_intra):
        print(new_vol_intra + " exists. do you want to continue?")
        return

    float_df = pd.read_csv(float_loc)
    float_df = float_df[["Ticker", "Float"]]
    float_df.rename(columns={"Ticker":"ticker"}, inplace=True)
    vol_df = pd.read_csv(intraday_vol_path + intra_vol)

    vol_df = vol_df.merge(float_df, on='ticker', how='left')
    vol_df.to_csv(intraday_vol_path + new_vol_intra)
    


criteria = "Direction"
def compare_vf_with_real():

    all_intra_df = pd.read_csv(intraday_vol_path + intra_all)

    mean_eod = all_intra_df["EOD"].median()
    print("Median  EOD")
    print(round(mean_eod))

    bulls_volume_dict = {
        "PM": 0, 
        "1 min" : 0,
        "5 min" : 0,
        "15 min" : 0, 
        "30 min" : 0, 
        "60 min" : 0,
        "EOD" : 0,
    }
    
    bears_volume_dict  = {
        "PM": 0, 
        "1 min" : 0,
        "5 min" : 0,
        "15 min" : 0, 
        "30 min" : 0, 
        "60 min" : 0,
        "EOD" : 0,
    }

    mix_volume_dict  = {
        "PM": 0, 
        "1 min" : 0,
        "5 min" : 0,
        "15 min" : 0, 
        "30 min" : 0, 
        "60 min" : 0,
        "EOD" : 0,
    }

    for period in bulls_volume_dict:
        bulls_df = all_intra_df[all_intra_df["Direction"] == 1] 
        # print(bulls_df)
        bulls_df = bulls_df[bulls_df["EOD"] > mean_eod]
        bulls_volume_dict[period] = (round(bulls_df[period].mean()))

        bears_df = all_intra_df[all_intra_df["Direction"] == -1]
        bears_df = bears_df[bears_df["EOD"] > mean_eod]
        # print(bears_df)
        bears_volume_dict[period] = (round(bears_df[period].mean()))
    
    plt.style.use('fivethirtyeight')

    bull_vals = list(bulls_volume_dict.values())
    bear_vals = list(bears_volume_dict.values())

    plt.plot(list(bulls_volume_dict.keys()), bull_vals, label="Runners")
    plt.plot(list(bears_volume_dict.keys()), bear_vals, label="Faders")

    plt.xlabel("Time period from open")
    plt.ylabel("Median Volume")
    plt.legend()
    plt.savefig("ALL DATA/Graphs/Median cases intraday volume for runners&faders.png")

    plt.title("Intraday volume for lowfloats")
    plt.show()


    
