#!/usr/bin/python3
# Static variables file

# File Types
PUMD_DATA_FILE_TYPES = ['interview', 'diary']
EXTRACT_INTERVIEW_FILE_TYPES = ['fmli', 'mtbi']
EXTRACT_DIARY_FILE_TYPES = ['fmld', 'expd']
MEAN_SQUARED_ERROR_BY_MEAN = "mean_squared_error_by_mean"
MEAN_ABSOLUTE_ERROR_BY_MEAN = "mean_absolute_error_by_mean"

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


# Scripts
DOWNLOAD_SCRIPT = "download_data_files.py"
EXTRACT_SCRIPT = "extract_data_files.py"
PROCESS_INTERVIEW_SCRIPT = "process_interview_data_files.py"
PROCESS_DIARY_SCRIPT = "process_diary_data_files.py"
PROCESS_DATA_DICTIONARY = "process_data_dictionary.py"
PROCESS_SPLINES = "process_splines.py"


JOBS = {
    1: "python3 {}".format(DOWNLOAD_SCRIPT),
    2: "python3 {}".format(EXTRACT_SCRIPT),
    3: "python3 {}".format(PROCESS_INTERVIEW_SCRIPT),
    4: "python3 {}".format(PROCESS_DIARY_SCRIPT),
    5: "python3 {}".format(PROCESS_DATA_DICTIONARY),
    6: "python3 {}".format(PROCESS_SPLINES)
}


JOBS_DESC = {
    1: "Download CE data files",
    2: "Extract CE data files",
    3: "Process CE Interview data",
    4: "Process CE Diary data",
    5: "Process CE data dictionary",
    6: "Process CE splines",
    7: "Quit"
}


# MTBI Files Dictionary
AVG_SPEND_FILES_3_YEAR = {
    "1996-1998": "avg_spend_intrvw_1996_to_1998",
    "1999-2001": "avg_spend_intrvw_1999_to_2001",
    "2002-2004": "avg_spend_intrvw_2002_to_2004",
    "2005-2007": "avg_spend_intrvw_2005_to_2007",
    "2008-2010": "avg_spend_intrvw_2008_to_2010",
    "2011-2013": "avg_spend_intrvw_2011_to_2013",
    "2014-2016": "avg_spend_intrvw_2014_to_2016"
}


AVG_SPEND_FILES_5_YEAR = {
    "1996-2000": "avg_spend_intrvw_1996_to_2000",
    "2001-2005": "avg_spend_intrvw_2001_to_2005",
    "2006-2010": "avg_spend_intrvw_2006_to_2010",
    "2011-2015": "avg_spend_intrvw_2011_to_2015"
}
