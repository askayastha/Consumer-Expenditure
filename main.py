#!/usr/bin/python3
# Main script to run multiple jobs

import sys
import re
import os
import constants


def main():
    print_jobs_list()
    jobs_list = input_jobs()
    run_jobs(jobs_list)


def input_jobs():
    try:
        jobs = input()
    except KeyboardInterrupt:
        print()
        sys.exit()

    # Compiles regular expression pattern for efficiency. The following pattern is for validating whether the job
    # numbers are specified in format such as 1-5,6,7,8-11
    pattern = re.compile("(^([1-9][0-9]?-[1-9][0-9]?)|^[1-9][0-9]?)((,[1-9][0-9]?-[1-9][0-9]?)|(,[1-9][0-9]?))*$")
    if not re.search(pattern, jobs):
        print("The specified job(s) {} are not in valid format. Valid format is something like 1 or 1,2,3 or 1-3.\n".format(jobs))
        sys.exit()
    jobs_to_run = jobs.split(',')
    specified_jobs_to_run = []

    for job in jobs_to_run:
        if job.find('-') != -1:
            i, j = job.split('-')
            i = int(i)
            j = int(j)

            while i <= j:
                specified_jobs_to_run.append(str(i))
                i += 1
        else:
            specified_jobs_to_run.append(job)

    return specified_jobs_to_run


def print_jobs_list():
    print("\nCONSUMER EXPENDITURE SURVEY")
    print("***************************")

    for key in constants.JOBS_DESC.keys():
        print("{}. {}".format(key, constants.JOBS_DESC[key]))

    print("\nPlease specify a job number to run. (eg. 1 or 1,2,3 or 1-3 etc.) ?")


def run_jobs(_jobs_list):
    for job_num in _jobs_list:
        job = constants.JOBS.get(int(job_num))

        if job is None:
            sys.exit() if job_num == '5' else print("The job(s) that you specified doesn't exist. Please enter valid job number(s)!!!\n")
        else:
            job_success = os.system(job)

            if job_success == 0:  # Checks for job success
                print("\n{} was successful.".format(constants.JOBS_DESC[int(job_num)]))
            else:
                print("\n{} was not successful.".format(constants.JOBS_DESC[int(job_num)]))
                sys.exit()


if __name__ == "__main__": main()
