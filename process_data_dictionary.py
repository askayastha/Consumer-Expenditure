import pandas as pd
import config
import constants
from datetime import datetime
import os

data_files_path = os.path.join(config.DATA_FILES_PATH, 'interview')
extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)

data_dict_pipe = pd.read_excel(os.path.join(config.DATA_FILES_PATH, 'ce_source_integrate.xlsx'), skiprows=[0, 1, 2])
data_dict_pipe.rename(columns={'Description': 'UCC_DESC'}, inplace=True)
data_dict_pipe = data_dict_pipe.filter(items=['UCC', 'UCC_DESC'])
data_dict_pipe['UCC'] = pd.to_numeric(data_dict_pipe['UCC'], errors='coerce', downcast='integer')
data_dict_pipe.dropna(inplace=True)
data_dict_pipe.drop_duplicates(inplace=True)

ucc_pipes = []


def main():
    years = list(constants.INTERVIEW_FILES.keys())
    start_year = years[0]
    end_year = years[-1]

    start_time = datetime.now()

    # Generate years bucket list according to the configuration
    for year in range(start_year, end_year, 1):
        process_data_files(year)

    final_uccs = pd.concat(ucc_pipes, axis=0).unique()
    final_ucc_pipe = pd.DataFrame(final_uccs, columns=['UCC'])
    final_ucc_pipe = pd.merge(final_ucc_pipe, data_dict_pipe, on='UCC', how='left')
    final_ucc_pipe.drop_duplicates(inplace=True)
    final_ucc_pipe.insert(1, 'UCC_DESCRIPTION', final_ucc_pipe['UCC_DESC'])
    final_ucc_pipe.drop(columns='UCC_DESC', inplace=True)

    # Replace double quotes with single quote
    final_ucc_pipe['UCC_DESCRIPTION'] = final_ucc_pipe['UCC_DESCRIPTION'].str.replace("''", "'")

    # Export UCC data dictionary
    export_file = os.path.join(config.DATA_FILES_PATH,
                               "ucc_data_dictionary.csv")
    final_ucc_pipe.to_csv(export_file, index=False)
    print("Exporting data to {}".format(export_file))

    end_time = datetime.now()
    overall_time = end_time - start_time
    print("***** Processing completed for interview data in {} min(s) {} secs. *****".format(
        overall_time.seconds // 60, overall_time.seconds % 60))


def process_data_files(_year):
    print("\n***** PROCESSING DATA FOR {} *****".format(_year))
    mtbi_pipe = concat_data_for_type('mtbi', [constants.INTERVIEW_FILES[_year]])
    ucc_pipes.append(mtbi_pipe['UCC'])


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
