import pandas as pd
import utils
import config
import os


def main():
    for bucket_size in ['3', '5']:
        mtbi_agg_data_pipes = []
        fmli_agg_data_pipes = []

        for file_type in ['mtbi', 'fmli']:
            for bucket_name, part_file_name in utils.avg_spend_files_for_bucket(bucket_size).items():
                reshaped_file_name = "{}_reshaped_{}.csv".format(file_type, part_file_name)
                reshaped_file = os.path.join(config.DATA_FILES_PATH, config.PROCESSED_DATA_FOLDER.format(int(bucket_size)), reshaped_file_name)

                gof_file_name = "{}_gof_{}.csv".format(file_type, part_file_name)
                gof_file = os.path.join(config.GOODNESS_OF_FIT_FOLDER_PATH, gof_file_name)
                gof_pipe = pd.read_csv(gof_file)

                data_pipe = pd.read_csv(reshaped_file)
                data_pipe['YEAR_BUCKET_COPY'] = bucket_name
                data_pipe.insert(2, 'YEAR_BUCKET', data_pipe['YEAR_BUCKET_COPY'])
                data_pipe.drop(columns='YEAR_BUCKET_COPY', inplace=True)

                if file_type == 'mtbi':
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

        # Filter categories
        mtbi_agg_data_pipe = filter_categories_for_data(mtbi_agg_data_pipe, bucket_size, 'mtbi')
        fmli_agg_data_pipe = filter_categories_for_data(fmli_agg_data_pipe, bucket_size, 'fmli')

        # Export aggregate files
        export_aggregate_file(mtbi_agg_data_pipe, bucket_size, 'mtbi')
        export_aggregate_file(fmli_agg_data_pipe, bucket_size, 'fmli')


def filter_categories_for_data(agg_data_pipe, bucket_size, file_type):
    god_file_name = "{}_god_{}yrs.csv".format(file_type, bucket_size)
    god_file = os.path.join(config.GOODNESS_OF_DATA_FOLDER_PATH, god_file_name)
    god_pipe = pd.read_csv(god_file)

    if file_type == 'mtbi':
        god_list = god_pipe[god_pipe['GOODNESS_OF_DATA']]['UCC'].tolist()
        agg_data_pipe = agg_data_pipe[agg_data_pipe['UCC'].isin(god_list)]
    elif file_type == 'fmli':
        god_list = god_pipe[god_pipe['GOODNESS_OF_DATA']]['CAT_CODE'].tolist()
        agg_data_pipe = agg_data_pipe[agg_data_pipe['CAT_CODE'].isin(god_list)]

    return agg_data_pipe


def export_aggregate_file(agg_data_pipe, bucket_size, file_type):
    utils.make_folder(config.AGGREGATE_DATA_FOLDER_PATH)
    agg_file_name = "{}_aggregate_data_{}yrs.xlsx".format(file_type, bucket_size)
    agg_file = os.path.join(config.AGGREGATE_DATA_FOLDER_PATH, agg_file_name)
    writer = pd.ExcelWriter(agg_file)
    agg_data_pipe.to_excel(writer, index=False)
    writer.save()
    print("Exporting data to {}".format(agg_file))


if __name__ == "__main__":
    main()
