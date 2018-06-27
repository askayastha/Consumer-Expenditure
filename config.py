#!/usr/bin/python3
# Configuration file

import os
from datetime import datetime
import constants

DATA_FILES_BASE_URL = "https://www.bls.gov/cex/pumd/data/comma/"
EXTRACT_FOLDER_NAME = "extracted_data_files"
DOWNLOAD_FOLDER_NAME = "pumd_data_files"
YEAR_BUCKET = 5

# Make changes to download path here...
# DOWNLOAD_PATH = os.path.join(os.path.expanduser("~"), "Downloads")
DOWNLOAD_PATH = "/Volumes/Transcend"
DATA_FILES_PATH = os.path.join(DOWNLOAD_PATH, DOWNLOAD_FOLDER_NAME)
today = datetime.now().strftime('%b%d').lower()
EXPORT_FILES_PATH = os.path.join(DATA_FILES_PATH, "processed_data_{}yrs_bucket_{}".format(YEAR_BUCKET, today))

YEAR_BUCKET_MULTIPLIERS = {
    3: 12,
    5: 20
}

AGE_THRESHOLDS = {
    "min": 20,
    "max": 80
}

INTERVIEW_FILES = {
    1996: "intrvw96",
    1997: "intrvw97",
    1998: "intrvw98",
    1999: "intrvw99",
    2000: "intrvw00",
    2001: "intrvw01",
    2002: "intrvw02",
    2003: "intrvw03",
    2004: "intrvw04",
    2005: "intrvw05",
    2006: "intrvw06",
    2007: "intrvw07",
    2008: "intrvw08",
    2009: "intrvw09",
    2010: "intrvw10",
    2011: "intrvw11",
    2012: "intrvw12",
    2013: "intrvw13",
    2014: "intrvw14",
    2015: "intrvw15",
    2016: "intrvw16"
}

DIARY_FILES = {
    1996: "diary96",
    1997: "diary97",
    1998: "diary98",
    1999: "diary99",
    2000: "diary00",
    2001: "diary01",
    2002: "diary02",
    2003: "diary03",
    2004: "diary04",
    2005: "diary05",
    2006: "diary06",
    2007: "diary07",
    2008: "diary08",
    2009: "diary09",
    2010: "diary10",
    2011: "diary11",
    2012: "diary12",
    2013: "diary13",
    2014: "diary14",
    2015: "diary15",
    2016: "diary16"
}

JOBS = {
    1: "python3 {}".format(constants.DOWNLOAD_SCRIPT),
    2: "python3 {}".format(constants.EXTRACT_SCRIPT),
    3: "python3 {}".format(constants.PROCESS_INTERVIEW_SCRIPT),
    4: "python3 {}".format(constants.PROCESS_DIARY_SCRIPT)
}

JOBS_DESC = {
    1: "Download CE data files",
    2: "Extract CE data files",
    3: "Process CE Interview data",
    4: "Process CE Diary data",
    5: "Quit"
}
