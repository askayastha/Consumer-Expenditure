import numpy as np
import pandas as pd
import utils
import config
import os


def main():
    for bucket_size in ['3', '5']:
        ucc_god_pipes = [utils.ucc_pipe[['UCC', 'UCC_DESCRIPTION']]]
        fmli_god_pipes = [utils.fmli_category_pipe[['CAT_CODE', 'CAT_DESCRIPTION']]]

        for file_type in ['mtbi', 'fmli']:
            for bucket_name, part_file_name in utils.avg_spend_files_for_bucket(bucket_size).items():
                file_name = "{}_gof_{}.csv".format(file_type, part_file_name)
                file = os.path.join(config.GOODNESS_OF_FIT_FOLDER_PATH, file_name)

                gof_pipe = pd.read_csv(file)
                gof_pipe.rename(columns={'GOODNESS_OF_DATA': bucket_name}, inplace=True)

                if file_type == 'mtbi':
                    ucc_god_pipes.append(gof_pipe[bucket_name])

                elif file_type == 'fmli':
                    fmli_god_pipes.append(gof_pipe[bucket_name])

        ucc_god_pipe = pd.concat(ucc_god_pipes, axis=1, sort=False)
        fmli_god_pipe = pd.concat(fmli_god_pipes, axis=1, sort=False)

        calculate_overall_god(ucc_god_pipe, bucket_size)
        calculate_overall_god(fmli_god_pipe, bucket_size)

        export_god_file(ucc_god_pipe, bucket_size, 'mtbi')
        export_god_file(fmli_god_pipe, bucket_size, 'fmli')


def calculate_overall_god(god_pipe, bucket_size):
    bucket_names = list(utils.avg_spend_files_for_bucket(bucket_size).keys())
    god_value = False

    for bucket_name in bucket_names:
        god_value = god_value | god_pipe[bucket_name]

    god_pipe['GOODNESS_OF_DATA'] = god_value


def export_god_file(god_pipe, bucket_size, file_type):
    utils.make_folder(config.GOODNESS_OF_DATA_FOLDER_PATH)
    god_file_name = "{}_god_{}yrs.csv".format(file_type, bucket_size)
    god_file = os.path.join(config.GOODNESS_OF_DATA_FOLDER_PATH, god_file_name)
    god_pipe.to_csv(god_file, index=False)
    print("Exporting data to {}".format(god_file))


if __name__ == "__main__":
    main()
