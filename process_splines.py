import numpy as np
import pandas as pd
from scipy.interpolate import UnivariateSpline
from scipy.signal.windows import gaussian
from scipy.ndimage import filters
import config
import os
import pickle
import utils


def main():
    for bucket_value in ['3', '5']:
        folder_path = os.path.join(config.DATA_FILES_PATH, "processed_data_{}yrs_bucket_jun26".format(int(bucket_value)))

        for file_type in ['mtbi', 'fmli']:
            for part_file_name in utils.avg_spend_files_for_bucket(bucket_value).values():
                file_name = "{}_{}.csv".format(file_type, part_file_name)
                file_path = os.path.join(folder_path, file_name)

                avg_spend_pipe = pd.read_csv(file_path)
                spline_dict = {}
                print("\n***** PROCESSING SPLINES FROM FILE: {} *****".format(file_name))

                if file_type == 'mtbi':
                    for category_value in utils.ucc_dict.keys():
                        filtered_pipe = avg_spend_pipe[avg_spend_pipe['UCC'] == int(category_value)]
                        spline = calculate_spline(filtered_pipe, file_type, category_value)
                        spline_dict[category_value] = spline

                elif file_type == 'fmli':
                    for category_value in utils.fmli_dict.keys():
                        filtered_pipe = avg_spend_pipe[['AGE_REF', category_value]]
                        spline = calculate_spline(filtered_pipe, file_type, category_value)
                        spline_dict[category_value] = spline

                utils.make_folder(config.SPLINES_FOLDER_PATH)
                spline_file_name = "{}_{}_{}.pkl".format(file_type, 'spline', part_file_name)
                spline_file_path = os.path.join(config.SPLINES_FOLDER_PATH, spline_file_name)

                with open(spline_file_path, 'wb') as file:
                    pickle.dump(spline_dict, file)


def calculate_spline(data_pipe, file_type, category_value):
    x = data_pipe['AGE_REF']
    y = data_pipe['AVG_SPEND'] if file_type == 'mtbi' else data_pipe[category_value]

    _, var = moving_average(y)

    try:
        u_spline = UnivariateSpline(x, y, w=1/np.sqrt(var))
    except Exception:
        return None

    return u_spline


def moving_average(series, sigma=3):
    b = gaussian(39, sigma)
    average = filters.convolve1d(series, b/b.sum())
    var = filters.convolve1d(np.power(series-average, 2), b/b.sum())
    return average, var


if __name__ == "__main__":
    main()
