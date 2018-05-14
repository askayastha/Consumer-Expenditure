import pandas as pd
import os
import config

data_files_path = os.path.join(config.DATA_FILES_PATH, "interview")
extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)


def main():
    fmli_pipe = concat_data_for_type("fmli")
    mtbi_pipe = concat_data_for_type("mtbi")

    # age_pipe = fmli_pipe['AGE_REF'].groupby(fmli_pipe['NEWID']).max()
    age_pipe = fmli_pipe.groupby('NEWID')['AGE_REF'].max()
    age_pipe = age_pipe.to_frame()
    age_pipe.reset_index(drop=False, inplace=True)

    # spend_pipe = fmli_pipe[['TOTEXPPQ', 'TOTEXPCQ']].groupby(fmli_pipe['NEWID']).sum().round()
    spend_pipe = fmli_pipe.groupby(['NEWID'])['TOTEXPPQ', 'TOTEXPCQ'].sum().round()
    spend_pipe['TOT_SPEND'] = spend_pipe['TOTEXPPQ'] + spend_pipe['TOTEXPCQ']
    spend_pipe.reset_index(drop=False, inplace=True)

    # age_count_pipe = fmli_pipe['AGE_REF'].groupby(fmli_pipe['AGE_REF']).count()
    age_count_pipe = age_pipe.groupby(['AGE_REF'])['AGE_REF'].count()
    age_count_pipe = age_count_pipe.to_frame()
    age_count_pipe = age_count_pipe.rename(columns={'AGE_REF': 'AGE_COUNT'})
    age_count_pipe.reset_index(drop=False, inplace=True)

    age_spend_pipe = pd.merge(age_pipe, spend_pipe, on='NEWID')
    monthly_age_spend_pipe = pd.merge(mtbi_pipe[['NEWID', 'UCC']], age_spend_pipe, on='NEWID')

    # age_ucc_spend_pipe = monthly_age_spend_pipe['TOT_SPEND'].groupby([monthly_age_spend_pipe['AGE_REF'], monthly_age_spend_pipe['UCC']]).sum()
    age_ucc_spend_pipe = monthly_age_spend_pipe.groupby(['AGE_REF', 'UCC'])['TOT_SPEND'].sum()
    age_ucc_spend_pipe = age_ucc_spend_pipe.to_frame()
    age_ucc_spend_pipe.reset_index(drop=False, inplace=True)

    avg_spend_by_age_ucc = pd.merge(age_ucc_spend_pipe, age_count_pipe, on='AGE_REF')
    avg_spend_by_age_ucc['AVG_SPEND'] = (avg_spend_by_age_ucc['TOT_SPEND'] / avg_spend_by_age_ucc['AGE_COUNT']).round()

    print(avg_spend_by_age_ucc)
    export_file_path = os.path.join(config.EXPORT_FILES_PATH, "test_export_file.csv")
    avg_spend_by_age_ucc.to_csv(export_file_path, index=False)

    print("Exported data to {}".format(export_file_path))


def concat_data_for_type(_type):
    for folder_name in os.listdir(extract_files_path):
        year_path = os.path.join(extract_files_path, folder_name)

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