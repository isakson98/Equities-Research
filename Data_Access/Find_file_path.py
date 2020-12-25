
import os
import re

parent_dir = "ALL DATA"

'''
i want to generalize my file access using the function
the end result is that i want to give ONLY a folder name or a file name
call this function, which will return me the full path

Problem: when using folder with same name -> you are in trouble
Solution: use folder above that
'''
def find_file(name):

    # in case there are some directories
    name_comp = name.split("/", 1)
    first_folder = name_comp[0]

    path = recurse_find(first_folder, parent_dir)

    # determine name type
    name_components = name.split(".")
    # its a directory, so i will add backslash at the end
    if path != "" and len(name_components) == 1:
        # has subdirectories
        if len(name_comp) != 1:
            path = path + '/' + name_comp[1]
        path = path + '/'
    
    return path


def recurse_find(name,  current_dir):
    content_list = os.listdir(current_dir)

    # check if desired file / dir in current dir
    if name in content_list:
        return current_dir + "/" + name 

    path = ""
    for content in content_list:
        if regex_no_match(name, content):
            break
        if os.path.isdir(current_dir + "/" + content):
            path = recurse_find(name, current_dir + "/" + content)
        # file found
        if path != "":
            break
    
    return path

# AACG_2020-11-18_2020-12-17.csv
pattern1 = "[a-zA-Z]+_[0-9]+-[0-9]+-[0-9]+_[0-9]+-[0-9]+-[0-9]+.csv"
# ABEV_2020-11-18_intraday.csv
pattern2= "[a-zA-Z]+_[0-9]+-[0-9]+-[0-9]+_intraday.csv"
# a_cash_flow_annual.csv
pattern3 = "[a-zA-Z]_cash_flow_annual.csv"
# a_balance_sheet_quarter.csv
pattern4 = "[a-zA-Z]_balance_sheet_quarter.csv"
# a_income_annual.csv
pattern5 = "[a-zA-Z]_income_annual.csv"
pattern_list = [pattern1, pattern2, pattern3, pattern4, pattern5]

'''
the purpose of this funciton is to speed up the look up of the file
i have several folders with the 1000's of files that have 
the same structure, and there is no point in searching through those
if the regex expression for it doesn't match
'''
def regex_no_match(desired_file, current_file):

    i_break = False
    for pattern in pattern_list:
        result_cur = re.findall(pattern, current_file)
        result_des = re.findall(pattern, desired_file)
        if len(result_cur) != 0:
            if len(result_des) == 0:
                i_break = True
                break

    return i_break


# check_reg_expr("a_income_annual.csv", "AACG_2020-11-18_2020-12-17.csv")
# check_reg_expr("crap.csv", "a_balance_sheet_quarter.csv")