import os
import pandas as pd 
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import collections


import sys 
sys.path.append(os.getcwd()) # current directory must be root project directory
from Data_Access.Find_file_path import find_file



'''
intra_file = "Intra_All.csv"
stock_criteria = list ["Direction", 1]
time_criteria = ["High of day", "Low of day"] (time in Intra_All.csv)

'''
def find_peak_time(intra_file, stock_criteria, time_criteria, chart_title):

    if not os.path.exists(intra_file):
        print(intra_file)
        print("File does not exist.")
        return

    all_intra_df = pd.read_csv(intra_file)
    median_eod = all_intra_df["EOD"].median()
    print(median_eod)
    crit_name = stock_criteria[0]
    crit_val = stock_criteria[1]
    run_w_vol_df = all_intra_df[(all_intra_df[crit_name] == crit_val) & (all_intra_df["EOD"] >= median_eod) ]

    run_w_vol_df[time_criteria] = [get_time_of_day(tmstp) for tmstp in run_w_vol_df[time_criteria]]

    x_y_dict = {}
    # graph from 9:30 to 4pm as x axis
    for time in run_w_vol_df[time_criteria]:
        x_y_dict[str(time)] = x_y_dict.get(str(time), 1) + 1

    sorted_x_y_dict = collections.OrderedDict(sorted(x_y_dict.items()))

    y_vals = list(sorted_x_y_dict.values()) 
    x_vals = list(sorted_x_y_dict.keys())


    fig, ax = plt.subplots()
    ax.bar(x_vals, y_vals)

    fig.autofmt_xdate()
    ax.fmt_xdata = mdates.DateFormatter("%H:%M")
    plt.xlabel("Time between open and close")
    plt.ylabel("Number of occurences")

    plt.title(chart_title)
    plt.show()
    graph_path = find_file("Graphs")
    fig.savefig(graph_path + chart_title + ".png")

    pass

def get_time_of_day(tmstp):

    dt_object = datetime.datetime.fromtimestamp(tmstp/1000)
    hour = dt_object.hour
    minute = dt_object.minute

    if minute > 45:
        hour = hour + 1
        minute = 0
    elif minute < 15:
        minute = 0
    else: 
        minute = 30

    time_of_day = datetime.time(hour, minute)

    return time_of_day

