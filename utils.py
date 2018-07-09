#!/usr/bin/python3
# Utility methods file

import os
import pandas as pd
import config
import constants
import pickle


data_files_path = os.path.join(config.DATA_FILES_PATH, 'interview')
extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)

ucc_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH, "ucc_data_dictionary.csv"))
ucc_pipe['UCC_DESCRIPTION'].fillna(ucc_pipe['UCC'].astype(str), inplace=True)
ucc_dict = pd.Series(ucc_pipe['UCC_DESCRIPTION'].values, index=ucc_pipe['UCC']).to_dict()

fmli_category_pipe = pd.read_csv(os.path.join(config.DATA_FILES_PATH, "fmli_data_dictionary.csv"))
fmli_dict = pd.Series(fmli_category_pipe['CAT_DESCRIPTION'].values, index=fmli_category_pipe['CAT_CODE']).to_dict()


def make_folder(_files_path):
    # Make extracted folder if it doesn't exist
    if not os.path.exists(_files_path):
        os.mkdir(_files_path)


def avg_spend_files_for_bucket(year_bucket):
    if year_bucket == '3':
        return constants.AVG_SPEND_FILES_3_YEAR
    elif year_bucket == '5':
        return constants.AVG_SPEND_FILES_5_YEAR


def category_dict_for_file(file_type):
    if file_type == 'mtbi':
        return ucc_dict
    elif file_type == 'fmli':
        return fmli_dict


def spline_dict_for_file(file_type, part_file_name):
    file_name = "{}_{}_{}.pkl".format(file_type, 'spline', part_file_name)
    file_path = os.path.join(config.SPLINES_FOLDER_PATH, file_name)

    with open(file_path, 'rb') as file:
        spline_dict = pickle.load(file)

    return spline_dict


def concat_data_for_type(_type, _year_folders):
    year_pipes = []
    for folder_name in _year_folders:
        year_folder_path = os.path.join(extract_files_path, folder_name)

        # Skip hidden files
        if '.' in year_folder_path:
            continue

        quarter_pipes = []
        # Walk the year folder path to see if the files are nested within another folder
        for dir_path, dir_names, file_names in os.walk(year_folder_path):
            if file_names:
                for file_name in file_names:
                    if file_name.endswith('.csv') and _type in file_name:
                        file_path = os.path.join(dir_path, file_name)
                        quarter_pipe = pd.read_csv(file_path)
                        quarter_pipes.append(quarter_pipe)
                        # break

        # print(quarter_pipes)
        year_pipe = pd.concat(quarter_pipes, axis=0, sort=False)
        year_pipes.append(year_pipe)

    final_pipe = pd.concat(year_pipes, axis=0, sort=False)
    return final_pipe
