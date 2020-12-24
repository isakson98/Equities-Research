
import os

parent_dir = "ALL DATA"

'''
i want to generalize my file access using the function
the end result is that i want to give ONLY a folder name or a file name
call this function, which will return me the full path

1 Problem: when using folder with same name -> you are in trouble
Solution: use folder above that
2 Problem: too slow, checking 1000's of csvs even when looking for folder
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
        if os.path.isdir(current_dir + "/" + content):
            path = recurse_find(name, current_dir + "/" + content)
        # file found
        if path != "":
            break
    
    return path



'''
keeping track of all files adjusted


fundumental fetch scripts
-harvest_ticekr_names
-process ticker dataset


fundumental manipulation scripst
-filter for growth or whatever


volume forecast
-get daily vol leaders





'''