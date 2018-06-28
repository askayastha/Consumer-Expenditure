#!/usr/bin/python3
# Job script to process Interview files

import pandas as pd
import os
import config
import utils
from datetime import datetime

data_files_path = os.path.join(config.DATA_FILES_PATH, 'interview')
extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)

data_dict_pipe = pd.read_excel(os.path.join(config.DATA_FILES_PATH, 'ce_source_integrate.xlsx'), skiprows=[0, 1, 2])
data_dict_pipe.rename(columns={'Description': 'UCC_DESC'}, inplace=True)
data_dict_pipe = data_dict_pipe.filter(items=['UCC', 'UCC_DESC'])
data_dict_pipe['UCC'] = pd.to_numeric(data_dict_pipe['UCC'], errors='coerce', downcast='integer')
data_dict_pipe.dropna(inplace=True)
data_dict_pipe.drop_duplicates(inplace=True)
# data_dict_file = os.path.join(config.EXPORT_FILES_PATH, "data_dict_file.csv")
# data_dict_pipe.to_csv(data_dict_file)

YEAR_BUCKET_MULTIPLIER = config.YEAR_BUCKET_MULTIPLIERS[config.YEAR_BUCKET]


def main():
    years = list(config.INTERVIEW_FILES.keys())
    start_year = years[0]
    end_year = years[-1]

    start_time = datetime.now()

    # Generate years bucket list according to the configuration
    for year in range(start_year, end_year, config.YEAR_BUCKET):
        years_bucket = []

        for i in range(config.YEAR_BUCKET):
            years_bucket.append(year + i)
        # print(years_bucket)
        process_mtbi_data_files(years_bucket)
        process_fmli_data_files(years_bucket)

    end_time = datetime.now()
    overall_time = end_time - start_time
    print("***** Processing completed for interview data in {} min(s) {} secs. *****".format(
        overall_time.seconds // 60, overall_time.seconds % 60))


def process_mtbi_data_files(_years):
    start_year = _years[0]
    end_year = _years[-1]
    print("\n***** PROCESSING MTBI DATA FROM {} TO {} *****".format(start_year, end_year))
    year_folders = []
    for year in _years:
        year_folders.append(config.INTERVIEW_FILES[year])

    fmli_pipe = concat_data_for_type('fmli', year_folders)
    mtbi_pipe = concat_data_for_type('mtbi', year_folders)

    # age_pipe = fmli_pipe['AGE_REF'].groupby(fmli_pipe['NEWID']).max()
    age_pipe = fmli_pipe.groupby('NEWID', as_index=False)['AGE_REF'].max()

    # Retain NEWID and FINLWT21 column
    finl_wt_pipe = fmli_pipe[['NEWID', 'FINLWT21']]

    # Sum(FINLWT21) grouped by AGE_REF in 'fmli' files
    sum_finl_wt_pipe = fmli_pipe.groupby(['AGE_REF'], as_index=False)['FINLWT21'].sum()
    sum_finl_wt_pipe.rename(columns={'FINLWT21': 'SUM_FINLWT21'}, inplace=True)

    # Sum(COST) grouped by NEWID, UCC in 'mtbi' files
    monthly_age_spend_pipe = mtbi_pipe.groupby(['NEWID', 'UCC'], as_index=False)['COST'].sum()

    # Join AGE_REF by NEWID from 'fmli' to 'mbti' files
    monthly_age_spend_pipe = pd.merge(monthly_age_spend_pipe, age_pipe, on='NEWID', how='left')
    monthly_age_spend_pipe.drop_duplicates(inplace=True)

    # Join FINLWT21 by NEWID from 'fmli' to 'mbti' files
    monthly_age_spend_finl_wt_pipe = pd.merge(monthly_age_spend_pipe, finl_wt_pipe, on='NEWID', how='left')
    monthly_age_spend_finl_wt_pipe.drop_duplicates(inplace=True)

    # Sum(WT_COST) grouped by AGE_REF, UCC
    monthly_age_spend_finl_wt_pipe['WT_COST'] = monthly_age_spend_finl_wt_pipe['FINLWT21'] * monthly_age_spend_finl_wt_pipe['COST']
    monthly_age_ucc_spend_pipe = monthly_age_spend_finl_wt_pipe.groupby(['AGE_REF', 'UCC'], as_index=False)['WT_COST'].sum()

    # Join the denominator Sum(FINLWT21) grouped by AGE_REF from 'fmli' to spend pipe
    monthly_age_ucc_spend_pipe = pd.merge(monthly_age_ucc_spend_pipe, sum_finl_wt_pipe, on='AGE_REF', how='left')
    monthly_age_ucc_spend_pipe.drop_duplicates(inplace=True)

    # Calcuate the average spend by dividing Sum(FINLWT21 * COST) by the denominator Sum(FINLWT21) grouped by AGE_REF
    monthly_age_ucc_spend_pipe['AVG_SPEND'] = ((monthly_age_ucc_spend_pipe['WT_COST'] / monthly_age_ucc_spend_pipe['SUM_FINLWT21']) * YEAR_BUCKET_MULTIPLIER).round(2)

    monthly_age_ucc_spend_pipe.drop(columns='SUM_FINLWT21', inplace=True)

    # Filter rows with AGE_REF >= 20 and AGE_REF <= 80
    monthly_age_ucc_spend_pipe = monthly_age_ucc_spend_pipe[
        (monthly_age_ucc_spend_pipe['AGE_REF'] >= config.AGE_THRESHOLDS['min']) & (monthly_age_ucc_spend_pipe['AGE_REF'] <= config.AGE_THRESHOLDS['max'])]
    print(monthly_age_ucc_spend_pipe)

    # Export processed data
    utils.make_folder(config.EXPORT_FILES_PATH)
    export_file = os.path.join(config.EXPORT_FILES_PATH, "mtbi_avg_spend_intrvw_{}_to_{}.csv".format(start_year, end_year))
    monthly_age_ucc_spend_pipe.to_csv(export_file, index=False)
    print("Exporting data to {}".format(export_file))

    # Reshape and export processed data
    reshaped_data = monthly_age_ucc_spend_pipe.pivot('UCC', 'AGE_REF', 'AVG_SPEND')
    reshaped_data = pd.merge(reshaped_data, data_dict_pipe, on='UCC', how='left')
    reshaped_data.drop_duplicates(inplace=True)
    reshaped_data.insert(1, 'UCC_DESCRIPTION', reshaped_data['UCC_DESC'])
    reshaped_data.drop(columns='UCC_DESC', inplace=True)
    reshaped_file = os.path.join(config.EXPORT_FILES_PATH, "mtbi_reshaped_avg_spend_intrvw_{}_to_{}.csv".format(start_year, end_year))
    reshaped_data.to_csv(reshaped_file, index=False)


def process_fmli_data_files(_years):
    start_year = _years[0]
    end_year = _years[-1]
    print("\n***** PROCESSING FMLI DATA FROM {} TO {} *****".format(start_year, end_year))
    year_folders = []
    for year in _years:
        year_folders.append(config.INTERVIEW_FILES[year])

    fmli_pipe = concat_data_for_type('fmli', year_folders)

    # Generate expense variables list
    expn_vars = [x for x in list(fmli_pipe) if x.endswith('PQ') or x.endswith('CQ')]
    expn_vars_dict = {}

    # Organize expense variables in dictionary for data processing
    for expn_var in expn_vars:
        expn_vars_dict[expn_var[0:-2]] = []

    for expn_var in expn_vars:
        expn_vars_dict.get(expn_var[0:-2]).append(expn_var)

    # Filter out non expense variables using the fact that individual expense would be reported in pairs (PQ, CQ)
    expn_vars_dict = {key: val for key, val in expn_vars_dict.items() if len(val) == 2}

    # Sum(FINLWT21) grouped by AGE_REF in 'fmli' files
    finl_wt_pipe = fmli_pipe.groupby(['AGE_REF'], as_index=False)['FINLWT21'].sum()
    finl_wt_pipe.rename(columns={'FINLWT21': 'SUM_FINLWT21'}, inplace=True)
    # print(finl_wt_pipe)

    # Get the weighted spend
    for key, val in expn_vars_dict.items():
        fmli_pipe['WT_' + key] = fmli_pipe['FINLWT21'] * (fmli_pipe[val[0]] + fmli_pipe[val[1]])

    # Sum(expn_vars) grouped by AGE_REF
    wt_expn_vars = [x for x in list(fmli_pipe) if x.startswith('WT_')]
    spend_pipe = fmli_pipe.groupby(['AGE_REF'], as_index=False)[wt_expn_vars].sum().round(2)
    spend_pipe = pd.merge(spend_pipe, finl_wt_pipe, on='AGE_REF', how='left')
    spend_pipe.drop_duplicates(inplace=True)

    # Calculate the average spend by dividing weighted spend by the denominator Sum(FINLWT21) grouped by AGE_REF
    for key, val in expn_vars_dict.items():
        spend_pipe[key] = ((spend_pipe['WT_' + key] / spend_pipe['SUM_FINLWT21']) * YEAR_BUCKET_MULTIPLIER).round(2)
        spend_pipe.drop(columns='WT_' + key, inplace=True)

    spend_pipe.drop(columns='SUM_FINLWT21', inplace=True)

    # Filter rows with AGE_REF >= 20 and AGE_REF <= 80
    spend_pipe = spend_pipe[(spend_pipe['AGE_REF'] >= config.AGE_THRESHOLDS['min']) & (spend_pipe['AGE_REF'] <= config.AGE_THRESHOLDS['max'])]
    print(spend_pipe)

    # Export processed data
    utils.make_folder(config.EXPORT_FILES_PATH)
    export_file = os.path.join(config.EXPORT_FILES_PATH, "fmli_avg_spend_intrvw_{}_to_{}.csv".format(start_year, end_year))
    spend_pipe.to_csv(export_file, index=False)
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
        year_pipe = pd.concat(quarter_pipes, axis=0, sort=False)
        year_pipes.append(year_pipe)

    final_pipe = pd.concat(year_pipes, axis=0, sort=False)
    return final_pipe


if __name__ == "__main__":
    main()