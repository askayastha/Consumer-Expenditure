import pandas as pd
import os
import config
import utils

data_files_path = os.path.join(config.DATA_FILES_PATH, "interview")
extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)


def main():
    fmli_pipe = concat_data_for_type("fmli")
    mtbi_pipe = concat_data_for_type("mtbi")

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
    print(age_ucc_spend_pipe)

    age_ucc_spend_pipe = pd.merge(age_ucc_spend_pipe, final_wt_pipe, on='AGE_REF')

    # Divide by sum fnlwt by age calculated above
    age_ucc_spend_pipe['AVG_SPEND'] = ((age_ucc_spend_pipe['TOT_SPEND'] / age_ucc_spend_pipe['FINLWT21']) * 20).round(2)
    print(age_ucc_spend_pipe)

    # Export processed data
    utils.make_folder(config.EXPORT_FILES_PATH)
    export_file = os.path.join(config.EXPORT_FILES_PATH, "test_export_file.csv")
    age_ucc_spend_pipe.to_csv(export_file, index=False)
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

        final_pipe = pd.concat(pipes, axis=0)

        return final_pipe


if __name__ == "__main__": main()