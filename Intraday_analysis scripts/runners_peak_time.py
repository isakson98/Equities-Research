import os
import pandas as pd 
import datetime

import sys 
sys.path.append(os.getcwd()) # current directory must be root project directory
from Data_Access.Find_file_path import find_file


intra_file = "Intra_All.csv"
intra_file = find_file(intra_file)

def find_peak_time():

    all_intra_df = pd.read_csv(intra_file)
    runners_df = all_intra_df[all_intra_df["Direction"] == 1]
    
    pass

