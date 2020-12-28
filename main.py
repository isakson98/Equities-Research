from Data_Access.Find_file_path import find_file
from Intraday_analysis_scripts import vf_linear_reg, runners_peak_time
from Intraday_fetch_scripts import get_intraday_data


# write here programs to write
# csv files
intra_all_csv = find_file("Intra_All.csv")
intra_vol_csv = find_file("Intra_Volume.csv")
intra_hl_csv = find_file("Intra_Highs_Lows.csv")
new_vol_intra_csv = find_file("Intra_Vol_Float.csv")
vol_ratio_csv = find_file("Intra_vol_ratio_direc.csv")

# directories
intraday_vol_path_csv = find_file("Intraday_formated")
vf_w_float_path = find_file("VF_with_float")
vf_pure_path = find_file("VF_with_volume_only")
daily_period_path = find_file("Daily_period")


# stock_criteria = ["Direction", 1]
# time_criteria = "Time of high"
# chart_title = time_criteria + " of day for low float runners"
# runners_peak_time.find_peak_time(intra_all_csv, stock_criteria, time_criteria, chart_title)



# vf_linear_reg.get_vf_vs_real_every_period(intra_vol_csv, intra_hl_csv, vol_ratio_csv, vf_pure_path)
categories = ["1 min", "5 min", "15 min", "30 min", "60 min", "EOD"]
vf_linear_reg.vf_real_ratio_describe(vol_ratio_csv, categories)


# get_intraday_data.add_cols_to_formatted_dataset(daily_period_path, vol_ratio_csv, ['open', 'vwap', 'close'])








