"""This script creates as a parquet file from Segment Logs 
"""
import os
import sys

import pandas as pd
import pyarrow.parquet as parquet


def get_files(path):
    ff = []
    for dir, _, files in os.walk(path):
        for f in files:
            ff.append(dir + "/" + f)
    return ff


Gorilla_Extracted_Files = '/srv/data3/abduvoris/Paper-2-Datasets/gorilla_only_extracted/Parquet/'

output_df_col_names = ['univariate_id', 'model_type_id', 'before_size', 'after_size', 'after_size_without_residuals',
                'residuals_size',]

# pub const PMC_MEAN_ID: u8 = 0; pub const SWING_ID: u8 = 1; pub const GORILLA_ID: u8 = 2;
model_name_dict = dict(zip([0,1,2], ['pmc', 'swing', 'gorilla']))

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} dataset_name logs_path error_bound save_path")
        sys.exit(0)

    dataset_name=sys.argv[1]
    logs_path = sys.argv[2] 
    eb = sys.argv[3]
    save_path=sys.argv[4]
    
    logs = pd.read_csv(
        logs_path, 
        header=None, 
        names=output_df_col_names
        )
    name = dataset_name.replace('_sample', '') if '_sample' in dataset_name else dataset_name
    column_names = get_files(Gorilla_Extracted_Files + f"/{name}/{name}-{eb}")
    column_names = [col_name.split('-')[-1].replace('.parquet','') for col_name in column_names]
    names_dict = dict(zip(logs['univariate_id'].unique().tolist(), column_names))
    try:
        logs['column_name'] = logs['univariate_id'].apply(lambda x: names_dict[x])
    except Exception as e:
        print("Could not write column names due to: " + str(e))
    logs['model_type'] = logs['model_type_id'].apply(lambda x: model_name_dict[x])
    # write dataset as parquet file
    logs.to_parquet(f'{save_path}/processed-{dataset_name}-{eb}.parquet', compression='snappy')