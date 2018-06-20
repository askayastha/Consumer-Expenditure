import pandas as pd
import os
import config
import utils

data_files_path = os.path.join(config.DATA_FILES_PATH, "interview")
extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)

data_dict_pipe = pd.read_excel(os.path.join(config.DATA_FILES_PATH, 'ce_source_integrate.xlsx'),
                                   skiprows=[0, 1, 2])
data_dict_pipe.rename(columns={'Description': 'UCC_DESC'}, inplace=True)
data_dict_pipe = data_dict_pipe.filter(items=['UCC', 'UCC_DESC'])
data_dict_pipe['UCC'] = pd.to_numeric(data_dict_pipe['UCC'], errors='coerce', downcast='integer')
data_dict_pipe.dropna(inplace=True)


def main():
    process_interview_data_files()
    process_fmli_data_files()


def process_interview_data_files():
    fmli_pipe = concat_data_for_type("fmli")
    mtbi_pipe = concat_data_for_type("mtbi")

    # age_pipe = fmli_pipe['AGE_REF'].groupby(fmli_pipe['NEWID']).max()
    age_pipe = fmli_pipe.groupby('NEWID', as_index=False)['AGE_REF'].max()

    # Retain NEWID and FINLWT21 column
    finl_wt_pipe = fmli_pipe[['NEWID', 'FINLWT21']]
    print(finl_wt_pipe)

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
    monthly_age_spend_finl_wt_pipe['WT_COST'] = monthly_age_spend_finl_wt_pipe['FINLWT21'] * \
                                                monthly_age_spend_finl_wt_pipe['COST']
    monthly_age_ucc_spend_pipe = monthly_age_spend_finl_wt_pipe.groupby(['AGE_REF', 'UCC'], as_index=False)[
        'WT_COST'].sum()

    # Join the denominator Sum(FINLWT21) grouped by AGE_REF from 'fmli' to spend pipe
    monthly_age_ucc_spend_pipe = pd.merge(monthly_age_ucc_spend_pipe, sum_finl_wt_pipe, on='AGE_REF', how='left')
    monthly_age_ucc_spend_pipe.drop_duplicates(inplace=True)

    # Divide Sum(FINLWT21 * COST) by the denominator Sum(FINLWT21) grouped by AGE_REF
    monthly_age_ucc_spend_pipe['AVG_SPEND'] = (
                (monthly_age_ucc_spend_pipe['WT_COST'] / monthly_age_ucc_spend_pipe['SUM_FINLWT21']) * 20).round(2)
    monthly_age_ucc_spend_pipe = monthly_age_ucc_spend_pipe[
        (monthly_age_ucc_spend_pipe['AGE_REF'] >= 20) & (monthly_age_ucc_spend_pipe['AGE_REF'] <= 80)]
    print(monthly_age_ucc_spend_pipe)

    # Export processed data
    utils.make_folder(config.EXPORT_FILES_PATH)
    export_file = os.path.join(config.EXPORT_FILES_PATH, "test_export_file.csv")
    monthly_age_ucc_spend_pipe.to_csv(export_file, index=False)
    print("Exporting data to {}".format(export_file))

    # Reshape and export processed data
    reshaped_data = monthly_age_ucc_spend_pipe.pivot('UCC', 'AGE_REF', 'AVG_SPEND')
    reshaped_data = pd.merge(reshaped_data, data_dict_pipe, on='UCC', how='left')
    reshaped_data.drop_duplicates(inplace=True)
    reshaped_data.insert(1, 'UCC_DESCRIPTION', reshaped_data['UCC_DESC'])
    reshaped_data.drop(columns='UCC_DESC', inplace=True)
    print(reshaped_data)
    reshaped_file = os.path.join(config.EXPORT_FILES_PATH, "test_reshaped_file.csv")
    reshaped_data.to_csv(reshaped_file, index=False)


def process_fmli_data_files():
    fmli_pipe = concat_data_for_type('fmli')
    expense_vars = [x for x in list(fmli_pipe) if x.endswith('PQ') or x.endswith('CQ')]
    expense_vars_dict = {}

    for expense_var in expense_vars:
        expense_vars_dict[expense_var[0:-2]] = []

    for expense_var in expense_vars:
        expense_vars_dict.get(expense_var[0:-2]).append(expense_var)
    print(expense_vars_dict)

    # Sum(FINLWT21) grouped by AGE_REF in 'fmli' files
    finl_wt_pipe = fmli_pipe.groupby(['AGE_REF'], as_index=False)['FINLWT21'].sum()
    finl_wt_pipe.rename(columns={'FINLWT21': 'SUM_FINLWT21'}, inplace=True)
    # print(finl_wt_pipe)

    # Get the weighted spend
    for key, val in expense_vars_dict.items():
        if len(val) < 2:
            fmli_pipe['WT_' + key] = fmli_pipe['FINLWT21'] * fmli_pipe[val[0]]
        else:
            fmli_pipe['WT_' + key] = fmli_pipe['FINLWT21'] * (fmli_pipe[val[0]] + fmli_pipe[val[1]])

    # Sum(expense_vars) grouped by AGE_REF
    wt_expense_vars = [x for x in list(fmli_pipe) if x.startswith('WT_')]
    spend_pipe = fmli_pipe.groupby(['AGE_REF'], as_index=False)[wt_expense_vars].sum().round(2)
    spend_pipe = pd.merge(spend_pipe, finl_wt_pipe, on='AGE_REF', how='left')
    spend_pipe.drop_duplicates(inplace=True)

    for key, val in expense_vars_dict.items():
        spend_pipe[key] = ((spend_pipe['WT_' + key] / spend_pipe['SUM_FINLWT21']) * 20).round(2)
        spend_pipe.drop(columns='WT_' + key, inplace=True)

    spend_pipe.drop(columns='SUM_FINLWT21', inplace=True)
    spend_pipe = spend_pipe[(spend_pipe['AGE_REF'] >= 20) & (spend_pipe['AGE_REF'] <= 80)]
    print(spend_pipe)

    # Export processed data
    utils.make_folder(config.EXPORT_FILES_PATH)
    export_file = os.path.join(config.EXPORT_FILES_PATH, "test_fmli_export_file.csv")
    spend_pipe.to_csv(export_file, index=False)
    print("Exporting data to {}".format(export_file))


def concat_data_for_type(_type):
    for folder_name in os.listdir(extract_files_path):
        year_path = os.path.join(extract_files_path, folder_name)
        print(year_path)

        # Skip hidden files
        if '.' in year_path:
            continue

        pipes = []
        for file_name in os.listdir(year_path):
            if file_name.endswith(".csv") and _type in file_name:
                file_path = os.path.join(year_path, file_name)
                pipe = pd.read_csv(file_path)
                pipes.append(pipe)
                # break

        final_pipe = pd.concat(pipes, axis=0, sort=False)

        return final_pipe


if __name__ == "__main__":
    main()
