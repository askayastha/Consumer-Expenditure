#!/usr/bin/python3
# Job script to process Diary files

import pandas as pd
import os
import config
import utils
from datetime import datetime

data_files_path = os.path.join(config.DATA_FILES_PATH, 'diary')
extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)


def main():
    years = list(config.DIARY_FILES.keys())
    start_year = years[0]
    end_year = years[-1]

    start_time = datetime.now()

    # Generate years bucket list according to the configuration
    for year in range(start_year, end_year, config.YEAR_BUCKET):
        years_bucket = []

        for i in range(config.YEAR_BUCKET):
            years_bucket.append(year + i)
        # print(years_bucket)
        process_diary_data_files(years_bucket)

    end_time = datetime.now()
    overall_time = end_time - start_time
    print("***** Processing completed for diary data in {} min(s) {} secs. *****".format(
        overall_time.seconds // 60, overall_time.seconds % 60))


def process_diary_data_files(_years):
    start_year = _years[0]
    end_year = _years[-1]
    print("\n***** PROCESSING DIARY DATA FROM {} TO {} *****".format(start_year, end_year))
    year_folders = []
    for year in _years:
        year_folders.append(config.DIARY_FILES[year])

    fmld_pipe = concat_data_for_type("fmld", year_folders)
    expd_pipe = concat_data_for_type("expd", year_folders)

    # age_pipe = fmld_pipe['AGE_REF'].groupby(fmld_pipe['NEWID']).max()
    age_pipe = fmld_pipe.groupby('NEWID', as_index=False)['AGE_REF'].max()

    # age_count_pipe = fmld_pipe['AGE_REF'].groupby(fmld_pipe['AGE_REF']).count()
    age_count_pipe = age_pipe.groupby(['AGE_REF'])['AGE_REF'].count()
    age_count_pipe = age_count_pipe.to_frame()
    age_count_pipe.rename(columns={'AGE_REF': 'AGE_COUNT'}, inplace=True)
    age_count_pipe.reset_index(drop=False, inplace=True)

    monthly_age_spend_pipe = pd.merge(expd_pipe[['NEWID', 'UCC', 'COST']], age_pipe, on='NEWID')
    monthly_age_spend_pipe.rename(columns={'COST': 'TOT_SPEND'}, inplace=True)
    monthly_age_spend_pipe.drop_duplicates(inplace=True)

    age_ucc_spend_pipe = monthly_age_spend_pipe.groupby(['AGE_REF', 'UCC'], as_index=False)['TOT_SPEND'].sum().round(2)

    avg_spend_by_age_ucc = pd.merge(age_ucc_spend_pipe, age_count_pipe, on='AGE_REF')
    avg_spend_by_age_ucc['AVG_SPEND'] = (avg_spend_by_age_ucc['TOT_SPEND'] / avg_spend_by_age_ucc['AGE_COUNT']).round(2)
    print(avg_spend_by_age_ucc)

    # Export processed data
    utils.make_folder(config.EXPORT_FILES_PATH)
    export_file = os.path.join(config.EXPORT_FILES_PATH, "avg_spend_diary_{}_to_{}.csv".format(start_year, end_year))
    avg_spend_by_age_ucc.to_csv(export_file, index=False)
    print("Exporting data to {}".format(export_file))


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
        year_pipe = pd.concat(quarter_pipes, axis=0)
        year_pipes.append(year_pipe)

    final_pipe = pd.concat(year_pipes, axis=0)
    return final_pipe


if __name__ == "__main__":
    main()
