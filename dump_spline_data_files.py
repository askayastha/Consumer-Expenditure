import numpy as np
import pandas as pd
import utils
import config
import os
from datetime import datetime


age_list = [age for age in range(config.AGE_THRESHOLDS['min'], config.AGE_THRESHOLDS['max'] + 1)]


def main():
    start_time = datetime.now()

    for bucket_size in ['3', '5']:
        mtbi_agg_data_pipes = []
        fmli_agg_data_pipes = []

        for file_type in ['mtbi', 'fmli']:
            for bucket_name, part_file_name in utils.avg_spend_files_for_bucket(bucket_size).items():
                # for i, (cat_code, cat_desc) in enumerate(category_dict.items()):
                #     data_pipe.loc[i] = [cat_code, cat_desc, bucket_name]

                spline_dict = utils.spline_dict_for_file(file_type, part_file_name)
                data_pipe = good_data_categories(bucket_size, file_type)
                data_pipe['YEAR_BUCKET'] = bucket_name

                if file_type == 'mtbi':
                    category_list = data_pipe['UCC'].tolist()
                elif file_type == 'fmli':
                    category_list = data_pipe['CAT_CODE'].tolist()

                for age in age_list:
                    data_pipe[str(age)] = np.nan

                for cat_code in category_list:
                    try:
                        spline = spline_dict[cat_code]
                    except KeyError:
                        continue

                    for age in age_list:
                        if file_type == 'mtbi':
                            data_pipe.loc[data_pipe['UCC'] == cat_code, str(age)] = spline_value_for_age(age, spline)
                        elif file_type == 'fmli':
                            data_pipe.loc[data_pipe['CAT_CODE'] == cat_code, str(age)] = spline_value_for_age(age, spline)

                gof_file_name = "{}_gof_{}.csv".format(file_type, part_file_name)
                gof_file = os.path.join(config.GOODNESS_OF_FIT_FOLDER_PATH, gof_file_name)
                gof_pipe = pd.read_csv(gof_file)
                gof_pipe.dropna(inplace=True)

                if file_type == 'mtbi':
                    god_list = gof_pipe[gof_pipe['GOODNESS_OF_DATA']]['UCC'].tolist()
                    data_pipe = data_pipe[data_pipe['UCC'].isin(god_list)]

                    gof_pipe = gof_pipe[['UCC', 'MSE_BY_MEAN', 'MAE_BY_MEAN']]
                    gof_pipe.rename(columns={'MSE_BY_MEAN': 'MSE_BY_MEAN_COPY'}, inplace=True)
                    gof_pipe.rename(columns={'MAE_BY_MEAN': 'MAE_BY_MEAN_COPY'}, inplace=True)
                    data_pipe = pd.merge(data_pipe, gof_pipe, on='UCC', how='left')
                    data_pipe.drop_duplicates(inplace=True)
                    data_pipe.insert(3, 'MSE_BY_MEAN', data_pipe['MSE_BY_MEAN_COPY'])
                    data_pipe.insert(4, 'MAE_BY_MEAN', data_pipe['MAE_BY_MEAN_COPY'])
                    data_pipe.drop(columns='MSE_BY_MEAN_COPY', inplace=True)
                    data_pipe.drop(columns='MAE_BY_MEAN_COPY', inplace=True)
                    mtbi_agg_data_pipes.append(data_pipe)

                elif file_type == 'fmli':
                    god_list = gof_pipe[gof_pipe['GOODNESS_OF_DATA']]['CAT_CODE'].tolist()
                    data_pipe = data_pipe[data_pipe['CAT_CODE'].isin(god_list)]

                    gof_pipe = gof_pipe[['CAT_CODE', 'MSE_BY_MEAN', 'MAE_BY_MEAN']]
                    gof_pipe.rename(columns={'MSE_BY_MEAN': 'MSE_BY_MEAN_COPY'}, inplace=True)
                    gof_pipe.rename(columns={'MAE_BY_MEAN': 'MAE_BY_MEAN_COPY'}, inplace=True)
                    data_pipe = pd.merge(data_pipe, gof_pipe, on='CAT_CODE', how='left')
                    data_pipe.drop_duplicates(inplace=True)
                    data_pipe.insert(3, 'MSE_BY_MEAN', data_pipe['MSE_BY_MEAN_COPY'])
                    data_pipe.insert(4, 'MAE_BY_MEAN', data_pipe['MAE_BY_MEAN_COPY'])
                    data_pipe.drop(columns='MSE_BY_MEAN_COPY', inplace=True)
                    data_pipe.drop(columns='MAE_BY_MEAN_COPY', inplace=True)
                    fmli_agg_data_pipes.append(data_pipe)

        mtbi_agg_data_pipe = pd.concat(mtbi_agg_data_pipes, axis=0, sort=False)
        fmli_agg_data_pipe = pd.concat(fmli_agg_data_pipes, axis=0, sort=False)

        mtbi_agg_data_pipe.sort_values(by=['UCC_DESCRIPTION', 'YEAR_BUCKET'], inplace=True)
        fmli_agg_data_pipe.sort_values(by=['CAT_DESCRIPTION', 'YEAR_BUCKET'], inplace=True)

        # Export aggregate files
        export_aggregate_file(mtbi_agg_data_pipe, bucket_size, 'mtbi')
        export_aggregate_file(fmli_agg_data_pipe, bucket_size, 'fmli')

    end_time = datetime.now()
    overall_time = end_time - start_time
    print("***** Processing completed for spline data in {} min(s) {} secs. *****".format(
        overall_time.seconds // 60, overall_time.seconds % 60))


def spline_values_for_ages(age_list, spline):
    return spline(age_list).tolist() if spline is not None else np.nan


def spline_value_for_age(age, spline):
    return spline(age).tolist() if spline is not None else np.nan


def good_data_categories(bucket_size, file_type):
    god_file_name = "{}_god_{}yrs.csv".format(file_type, bucket_size)
    god_file = os.path.join(config.GOODNESS_OF_DATA_FOLDER_PATH, god_file_name)
    god_pipe = pd.read_csv(god_file)
    god_pipe = god_pipe[god_pipe['GOODNESS_OF_DATA']]

    if file_type == 'mtbi':
        return god_pipe[['UCC', 'UCC_DESCRIPTION']]

    elif file_type == 'fmli':
        return god_pipe[['CAT_CODE', 'CAT_DESCRIPTION']]


def export_aggregate_file(agg_data_pipe, bucket_size, file_type):
    utils.make_folder(config.AGGREGATE_DATA_FOLDER_PATH)
    agg_file_name = "{}_aggregate_splines_{}yrs.xlsx".format(file_type, bucket_size)
    agg_file = os.path.join(config.AGGREGATE_DATA_FOLDER_PATH, agg_file_name)
    writer = pd.ExcelWriter(agg_file)
    agg_data_pipe.to_excel(writer, index=False)
    writer.save()
    print("Exporting data to {}".format(agg_file))


if __name__ == "__main__":
    main()
