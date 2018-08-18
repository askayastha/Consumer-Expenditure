import numpy as np
import pandas as pd
from scipy.interpolate import UnivariateSpline
from scipy.signal.windows import gaussian
from scipy.ndimage import filters
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
import config
import constants
import os
import pickle
import utils


def main():
    for bucket_size in ['3', '5']:
        folder_path = os.path.join(config.DATA_FILES_PATH, config.PROCESSED_DATA_FOLDER.format(int(bucket_size)))

        for file_type in ['mtbi', 'fmli']:
            for part_file_name in utils.avg_spend_files_for_bucket(bucket_size).values():
                file_name = "{}_{}.csv".format(file_type, part_file_name)
                file_path = os.path.join(folder_path, file_name)

                avg_spend_pipe = pd.read_csv(file_path)
                spline_dict = {}
                print("\n***** PROCESSING SPLINES FROM FILE: {} *****".format(file_name))

                if file_type == 'mtbi':
                    gof_pipe = utils.ucc_pipe.copy()
                    gof_pipe['MSE_BY_MEAN'] = np.nan
                    gof_pipe['MAE_BY_MEAN'] = np.nan

                    for category_value in utils.ucc_dict.keys():
                        filtered_pipe = avg_spend_pipe[avg_spend_pipe['UCC'] == int(category_value)]

                        # Spline
                        spline = calculate_spline(filtered_pipe, file_type, category_value)
                        spline_dict[category_value] = spline

                        # Goodness of Fit
                        mse_by_mean_value = calculate_goodness_of_fit(filtered_pipe, spline, file_type, category_value, constants.MEAN_SQUARED_ERROR_BY_MEAN)
                        mae_by_mean_value = calculate_goodness_of_fit(filtered_pipe, spline, file_type, category_value, constants.MEAN_ABSOLUTE_ERROR_BY_MEAN)
                        gof_pipe.loc[gof_pipe['UCC'] == category_value, 'MSE_BY_MEAN'] = mse_by_mean_value
                        gof_pipe.loc[gof_pipe['UCC'] == category_value, 'MAE_BY_MEAN'] = mae_by_mean_value

                        # Goodness of Data
                        god_value = utils.check_goodness_of_data(filtered_pipe)
                        gof_pipe.loc[gof_pipe['UCC'] == category_value, 'GOODNESS_OF_DATA'] = god_value

                elif file_type == 'fmli':
                    gof_pipe = utils.fmli_category_pipe.copy()
                    gof_pipe['MSE_BY_MEAN'] = np.nan
                    gof_pipe['MAE_BY_MEAN'] = np.nan

                    for category_value in utils.fmli_dict.keys():
                        try:
                            filtered_pipe = avg_spend_pipe[['AGE_REF', category_value]]
                        except KeyError:
                            continue

                        # Spline
                        spline = calculate_spline(filtered_pipe, file_type, category_value)
                        spline_dict[category_value] = spline

                        # Goodness of Fit
                        mse_by_mean_value = calculate_goodness_of_fit(filtered_pipe, spline, file_type, category_value, constants.MEAN_SQUARED_ERROR_BY_MEAN)
                        mae_by_mean_value = calculate_goodness_of_fit(filtered_pipe, spline, file_type, category_value, constants.MEAN_ABSOLUTE_ERROR_BY_MEAN)
                        gof_pipe.loc[gof_pipe['CAT_CODE'] == category_value, 'MSE_BY_MEAN'] = mse_by_mean_value
                        gof_pipe.loc[gof_pipe['CAT_CODE'] == category_value, 'MAE_BY_MEAN'] = mae_by_mean_value

                        # Goodness of Data
                        god_value = utils.check_goodness_of_data(filtered_pipe)
                        gof_pipe.loc[gof_pipe['CAT_CODE'] == category_value, 'GOODNESS_OF_DATA'] = god_value

                # Export pickle files
                export_pickle_files(file_type, part_file_name, spline_dict)

                # Export goodness of fit files
                export_goodness_of_fit_files(file_type, gof_pipe, part_file_name)


def export_goodness_of_fit_files(file_type, gof_pipe, part_file_name):
    utils.make_folder(config.GOODNESS_OF_FIT_FOLDER_PATH)
    gof_file_name = "{}_gof_{}.csv".format(file_type, part_file_name)
    gof_file = os.path.join(config.GOODNESS_OF_FIT_FOLDER_PATH, gof_file_name)
    gof_pipe.to_csv(gof_file, index=False)
    print("Exporting data to {}".format(gof_file))


def export_pickle_files(file_type, part_file_name, spline_dict):
    utils.make_folder(config.SPLINES_FOLDER_PATH)
    spline_file_name = "{}_spline_{}.pkl".format(file_type, part_file_name)
    spline_file = os.path.join(config.SPLINES_FOLDER_PATH, spline_file_name)

    with open(spline_file, 'wb') as file:
        pickle.dump(spline_dict, file)

    print("Exporting data to {}".format(spline_file))


def calculate_goodness_of_fit(data_pipe, spline, file_type, category_value, error_type):
    if spline is None:
        return np.nan

    if file_type == 'mtbi':
        data_dict = pd.Series(data_pipe['AVG_SPEND'].values, index=data_pipe['AGE_REF']).to_dict()
    elif file_type == 'fmli':
        data_dict = pd.Series(data_pipe[category_value].values, index=data_pipe['AGE_REF']).to_dict()

    y_true = []
    for age in data_dict.keys():
        y_true.append(data_dict[age])

    # xs = data_pipe['AGE_REF'].tolist()
    xs = list(data_dict.keys())
    y_pred = spline(xs).tolist()
    # expected_vals = data_pipe['AVG_SPEND'].tolist() if file_type == 'mtbi' else data_pipe[category_value].tolist()

    try:
        if error_type == constants.MEAN_SQUARED_ERROR_BY_MEAN:
            gof_value = mean_squared_error(y_true, y_pred) / abs(np.mean(y_true))
        elif error_type == constants.MEAN_ABSOLUTE_ERROR_BY_MEAN:
            gof_value = mean_absolute_error(y_true, y_pred) / abs(np.mean(y_true))
    
    except ValueError:
        return np.nan

    return gof_value


def calculate_spline(data_pipe, file_type, category_value):
    x = data_pipe['AGE_REF']
    y = data_pipe['AVG_SPEND'] if file_type == 'mtbi' else data_pipe[category_value]

    _, var = moving_average(y)

    try:
        u_spline = UnivariateSpline(x, y, w=1 / np.sqrt(var))
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
