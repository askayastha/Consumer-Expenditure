#!/usr/bin/python3
# Job script to extract Consumer Expenditure Survey data

from zipfile import *
import os
import constants
import config


def main():
    extract_data_files(constants.INTERVIEW_FILES)
    extract_data_files(constants.DIARY_FILES)


def extract_data_files(_type):
    # Setup for data file types
    if _type == constants.INTERVIEW_FILES:
        data_files_path = os.path.join(config.DATA_FILES_PATH, 'interview')
        extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)
        make_folder(extract_files_path)
        extract_file_types = constants.EXTRACT_INTERVIEW_FILE_TYPES
    elif _type == constants.DIARY_FILES:
        data_files_path = os.path.join(config.DATA_FILES_PATH, 'diary')
        extract_files_path = os.path.join(data_files_path, config.EXTRACT_FOLDER_NAME)
        make_folder(extract_files_path)
        extract_file_types = constants.EXTRACT_DIARY_FILE_TYPES

    # Extract all the required data files
    for file_name in os.listdir(data_files_path):
        if file_name.endswith('.zip'):
            file_path = os.path.join(data_files_path, file_name)
            zip_archive = ZipFile(file_path)
            file_name_no_ext = file_name.rstrip('.zip')

            for file in zip_archive.namelist():
                for file_type in extract_file_types:
                    if file.startswith(file_name_no_ext) and file_type in file:
                        print("Extracting {}".format(file))
                        zip_archive.extract(file, path=extract_files_path)

            # break


def make_folder(_files_path):
    # Make extracted folder if it doesn't exist
    if not os.path.exists(_files_path):
        os.mkdir(_files_path)


if __name__ == "__main__": main()
