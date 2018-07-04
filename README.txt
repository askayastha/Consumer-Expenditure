Consumer Expenditure Survey Data Wrangler
-----------------------------------------
In the project directory, there are four major Python job scripts to handle the Consumer Expenditure Survey data.
* download_data_files.py
* extract_data_files.py
* process_interview_data_files.py
* process_diary_data_files.py

There are also a few support Python files which have the following purpose.
* config.py: In this file, you can configure things such as file names and directory paths for download and extraction.
             You can also change the year bucket to change the data evaluation period. Default is 3.
* constants.py: The job scripts need this file for internal use.
* utils.py: This file has the supporting helper methods.
* main.py: This is the main menu script to select and execute jobs. You should use this script to run jobs.


System Requirements
-------------------
* Python 3.5+


Dependencies
------------
* Third Party Libraries: pandas, wget, scipy, dash


Installation
------------
In order for the scripts to work, you need to install pandas and wget.
$ pip3 install -U pandas
$ pip3 install -U wget
$ pip3 install -U scipy
$ pip3 install dash==0.21.1  # The core dash backend
$ pip3 install dash-renderer==0.13.0  # The dash front-end
$ pip3 install dash-html-components==0.11.0  # HTML components
$ pip3 install dash-core-components==0.23.0  # Supercharged components


Usage
-----
In order to run the jobs, change to project directory in terminal and execute:
$ python3 main.py


Author
------
Ashish Kayastha, ashish.kayastha@me.com