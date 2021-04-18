#!/usr/bin/python3
# Configuration file

import os
from datetime import datetime
import constants

DATA_FILES_BASE_URL = "https://www.bls.gov/cex/pumd/data/comma/"
EXTRACT_FOLDER_NAME = "extracted_data_files"
DOWNLOAD_FOLDER_NAME = "pumd_data_files"
YEAR_BUCKET_SIZE = 3

# Make changes to download path here...
# DOWNLOAD_PATH = os.path.expanduser("~")
DOWNLOAD_PATH = "/Volumes/ADATA"
DATA_FILES_PATH = os.path.join(DOWNLOAD_PATH, DOWNLOAD_FOLDER_NAME)
SPLINES_FOLDER_PATH = os.path.join(DATA_FILES_PATH, 'splines')
GOODNESS_OF_FIT_FOLDER_PATH = os.path.join(DATA_FILES_PATH, 'goodness_of_fit')
GOODNESS_OF_DATA_FOLDER_PATH = os.path.join(DATA_FILES_PATH, 'goodness_of_data')
AGGREGATE_DATA_FOLDER_PATH = os.path.join(DATA_FILES_PATH, 'aggregate_data')
today = datetime.now().strftime('%b%d').lower()
EXPORT_FILES_PATH = os.path.join(DATA_FILES_PATH, "processed_data_{}yrs_bucket_{}".format(YEAR_BUCKET_SIZE, today))
PROCESSED_DATA_FOLDER = "processed_data_{}yrs_bucket_aug30"

YEAR_BUCKET_SIZE_MULTIPLIERS = {
    3: 12,
    5: 20
}

AGE_THRESHOLDS = {
    "min": 20,
    "max": 80
}

GOOD_DATA_AGE_THRESHOLDS = {
    "min": 30,
    "max": 60
}
