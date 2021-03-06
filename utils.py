#!/usr/bin/python3
# Utility methods file

import os
import pandas as pd
import config
import constants
import pickle
import collections


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


def avg_spend_files_for_bucket(bucket_size):
    if bucket_size == '3':
        return constants.AVG_SPEND_FILES_3_YEAR
    elif bucket_size == '5':
        return constants.AVG_SPEND_FILES_5_YEAR


def category_dict_for_file(file_type, bucket_size=None):
    if bucket_size is None:
        if file_type == 'mtbi':
            return ucc_dict
        elif file_type == 'fmli':
            return fmli_dict
    else:
        if file_type == 'mtbi':
            category_pipe = pd.read_csv(os.path.join(config.GOODNESS_OF_DATA_FOLDER_PATH, "{}_god_{}yrs.csv".format(file_type, bucket_size)))
            filtered_category_pipe = category_pipe[category_pipe['GOODNESS_OF_DATA']]
            filtered_category_dict = pd.Series(filtered_category_pipe['UCC_DESCRIPTION'].values, index=filtered_category_pipe['UCC']).to_dict()

        elif file_type == 'fmli':
            category_pipe = pd.read_csv(os.path.join(config.GOODNESS_OF_DATA_FOLDER_PATH, "{}_god_{}yrs.csv".format(file_type, bucket_size)))
            filtered_category_pipe = category_pipe[category_pipe['GOODNESS_OF_DATA']]
            filtered_category_dict = pd.Series(filtered_category_pipe['CAT_DESCRIPTION'].values, index=filtered_category_pipe['CAT_CODE']).to_dict()

        return filtered_category_dict


def spline_dict_for_file(file_type, part_file_name):
    file_name = "{}_{}_{}.pkl".format(file_type, 'spline', part_file_name)
    file_path = os.path.join(config.SPLINES_FOLDER_PATH, file_name)

    with open(file_path, 'rb') as file:
        spline_dict = pickle.load(file)

    return spline_dict


def sort_dictionary(dict, sort_value):
    if sort_value == 'asc':
        return collections.OrderedDict(sorted(dict.items(), key=lambda t: t[1]))
    elif sort_value == 'desc':
        return collections.OrderedDict(sorted(dict.items(), key=lambda t: t[1], reverse=True))


def concat_data_for_type(_type, _year_folders, extract_files_path):
    year_pipes = []
    for folder_name in _year_folders:
        year_folder_path = os.path.join(extract_files_path, folder_name)
        short_year = folder_name[-2:]

        # Skip hidden files
        if '.' in year_folder_path:
            continue

        quarter_pipes = []
        # Walk the year folder path to see if the files are nested within another folder
        for dir_path, dir_names, file_names in os.walk(year_folder_path):
            if file_names:
                for file_name in file_names:
                    if file_name.endswith('.csv') and _type in file_name and short_year in file_name and '._' not in file_name:
                        print(file_name)
                        file_path = os.path.join(dir_path, file_name)
                        quarter_pipe = pd.read_csv(file_path)
                        quarter_pipes.append(quarter_pipe)
                        # break

        # print(quarter_pipes)
        print("*****")
        year_pipe = pd.concat(quarter_pipes, axis=0, sort=False)
        year_pipes.append(year_pipe)

    final_pipe = pd.concat(year_pipes, axis=0, sort=False)
    return final_pipe


def check_goodness_of_data(data_pipe):
    data_pipe = data_pipe[(data_pipe['AGE_REF'] >= config.GOOD_DATA_AGE_THRESHOLDS['min']) & (data_pipe['AGE_REF'] <= config.GOOD_DATA_AGE_THRESHOLDS['max'])]
    age_list = data_pipe['AGE_REF'].tolist()
    god_age_list = list(range(config.GOOD_DATA_AGE_THRESHOLDS['min'], config.GOOD_DATA_AGE_THRESHOLDS['max'] + 1))

    return True if age_list == god_age_list else False