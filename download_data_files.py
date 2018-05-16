import wget
import os
import config


def main():
    # Make data files folder if it doesn't exist
    if not os.path.exists(config.DATA_FILES_PATH):
        os.mkdir(config.DATA_FILES_PATH)

    os.chdir(config.DATA_FILES_PATH)

    download_data_files(config.INTERVIEW_FILES)
    download_data_files(config.DIARY_FILES)


def change_folder(_name):
    # Make data files folder if it doesn't exist
    new_path = os.path.join(config.DATA_FILES_PATH, _name)
    if not os.path.exists(new_path):
        os.mkdir(new_path)

    os.chdir(new_path)


def download_data_files(_type):
    # Setup for data file types
    if _type == config.INTERVIEW_FILES:
        change_folder('interview')
        download_files_list = config.INTERVIEW_FILES.values()
    elif _type == config.DIARY_FILES:
        change_folder('diary')
        download_files_list = config.DIARY_FILES.values()

    # Download all the data files
    for file in download_files_list:
        file_name = file + '.zip'
        download_url = config.DATA_FILES_BASE_URL + file_name

        if not file_exists(download_url):
            print("\nDownloading {}".format(download_url))
            wget.download(download_url)


def file_exists(_download_url):
    file_found = False
    for file_name in os.listdir():
        if _download_url.endswith(file_name):
            file_found = True

    return file_found


if __name__ == "__main__": main()
