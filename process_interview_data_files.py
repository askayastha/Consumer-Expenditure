#!/usr/bin/python3
# Job script to process Interview files

import pandas as pd
import os
import config
import utils
from datetime import datetime

data_files_path = os.path.join(config.DATA_FILES_PATH, 'interview')
extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)


def main():
    years = list(config.INTERVIEW_FILES.keys())
    start_year = int(years[0])
    end_year = int(years[len(years) - 1])

    start_time = datetime.now()

    # Generate years bucket list according to the configuration
    for year in range(start_year, end_year, config.YEAR_BUCKET):
        years_bucket = []

        for i in range(config.YEAR_BUCKET):
            years_bucket.append(str(year + i))
        # print(years_bucket)
        process_interview_data_files(years_bucket)

    end_time = datetime.now()
    overall_time = end_time - start_time
    print("***** Processing completed for interview data in {} min(s) {} secs. *****".format(
        int(overall_time.seconds / 60), overall_time.seconds % 60))


def process_interview_data_files(_years):
    start_year = _years[0]
    end_year = _years[len(_years) - 1]
    print("\n***** PROCESSING INTERVIEW DATA FROM {} TO {} *****".format(start_year, end_year))
    year_folders = []
    for year in _years:
        year_folders.append(config.INTERVIEW_FILES[year])

    fmli_pipe = concat_data_for_type('fmli', year_folders)
    mtbi_pipe = concat_data_for_type('mtbi', year_folders)

    # age_pipe = fmli_pipe['AGE_REF'].groupby(fmli_pipe['NEWID']).max()
    age_pipe = fmli_pipe.groupby('NEWID')['AGE_REF'].max()
    age_pipe = age_pipe.to_frame()
    age_pipe.reset_index(drop=False, inplace=True)

    # Sum finlwt by age in fmli
    final_wt_pipe = fmli_pipe.groupby(['AGE_REF'])['FINLWT21'].sum()
    final_wt_pipe = final_wt_pipe.to_frame()
    final_wt_pipe.reset_index(drop=False, inplace=True)

    # age_and_final_wt_pipe = pd.merge(age_pipe, final_wt_pipe, on='AGE_REF')

    # Sum cost group by ucc, newid in mbti
    monthly_age_spend_pipe = mtbi_pipe.groupby(['NEWID', 'UCC'])['COST'].sum()
    monthly_age_spend_pipe = monthly_age_spend_pipe.to_frame()
    monthly_age_spend_pipe.reset_index(drop=False, inplace=True)

    # Merge age and fnllwt by newid from fmli to mbti
    age_spend_pipe = pd.merge(monthly_age_spend_pipe, age_pipe, on='NEWID')
    age_spend_final_wt_pipe = pd.merge(age_spend_pipe, final_wt_pipe, on='AGE_REF')

    # Sum fnlwt*cost by age and ucc
    age_spend_final_wt_pipe['TOT_SPEND'] = age_spend_final_wt_pipe['FINLWT21'] * age_spend_final_wt_pipe['COST']

    age_ucc_spend_pipe = age_spend_final_wt_pipe.groupby(['AGE_REF', 'UCC'])['TOT_SPEND'].sum()
    age_ucc_spend_pipe = age_ucc_spend_pipe.to_frame()
    age_ucc_spend_pipe.reset_index(drop=False, inplace=True)

    age_ucc_spend_pipe = pd.merge(age_ucc_spend_pipe, final_wt_pipe, on='AGE_REF')

    # Divide by sum fnlwt by age calculated above
    age_ucc_spend_pipe['AVG_SPEND'] = ((age_ucc_spend_pipe['TOT_SPEND'] / age_ucc_spend_pipe['FINLWT21']) * 20).round(2)
    print(age_ucc_spend_pipe)

    # Export processed data
    utils.make_folder(config.EXPORT_FILES_PATH)
    export_file = os.path.join(config.EXPORT_FILES_PATH, "avg_spend_interview_{}_to_{}.csv".format(start_year, end_year))
    age_ucc_spend_pipe.to_csv(export_file, index=False)
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


if __name__ == "__main__": main()
