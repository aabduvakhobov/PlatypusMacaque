from iotdb.Session import Session
from iotdb.template.MeasurementNode import TSDataType
from iotdb.utils import NumpyTablet

import numpy as np
import pyarrow
from pyarrow import parquet

import glob
import sys
import time
import logging
import os


# read config file and create env vars
THREASHOLD=10_000_000 # the number of rows in the tablet that IoTDB can process
ROOT = "root."


def get_files(path):
    if os.path.isdir(path):
        parquet_files = glob.glob(path + os.sep + "*.parquet")
        parquet_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        parquet_files = [path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")
    return parquet_files


def create_iotdb_session():
    ip = "127.0.0.1"
    port_ = "6667"
    username_ = "root"
    password_ = "root"
    # Apache IoT DB Database and TimeSeries creation
    session = Session(ip, port_, username_, password_)
    session.open(False)
    return session


def create_values_and_timestamps(table, start_index, end_index):
    np_values_ = []
    for schema in table.schema:
        if schema.type not in [pyarrow.timestamp('ms'), pyarrow.timestamp('us'), pyarrow.timestamp('s')]:
            # append values into value list
            np_values_.append(np.array( table.column(schema.name).to_numpy()[start_index:end_index], TSDataType.FLOAT.np_dtype()) )
            # create and append bitmaps to bitmap list
            # we continue with the last file to extract timestamps as unix int64
        else:
            if schema.type == pyarrow.timestamp('us'):
                np_timestamps_ = np.array(table.column(schema.name)[start_index:end_index].to_numpy(), TSDataType.INT64.np_dtype())
            else:
                np_timestamps_ = np.array(table.column(schema.name)[start_index:end_index].cast(pyarrow.int64()).to_numpy(), TSDataType.INT64.np_dtype())
            # create timestamp values
    if len(np_timestamps_) == 0 or len(np_values_) == 0:
        raise ValueError(f"Your timestamp with len: {len(np_timestamps_)} or numpy list with len: {len(np_values_)} is empty!")
    return np_timestamps_, np_values_


def validate_device_name(name):
    return name.replace(".","").replace("-","_").replace(' ', '_')


def create_and_insert_numpy_tablets(file_path, dataset_name, session):
    tic = time.perf_counter()
    ingestion_time = 0
    # we ingest a single big Parquet file in this context
    table = parquet.read_table(file_path)
    table = table.select([col for col in table.column_names if col not in ['__index_level_0__']])
    site_name = validate_device_name(file_path.split('/')[-1].split('.')[0])
    # measurement are column names in the Parquet file apart from datetime
    measurements_ = [validate_device_name(col.name) for col in table.schema if col.type not in [pyarrow.timestamp('ms'),  pyarrow.timestamp('us'), pyarrow.timestamp('s')]]    # we return list of numpy Tablets
    # split dataset by 10 million rows and ingest in batches
    last_number = table.num_rows // THREASHOLD
    for i in range(last_number + 1):
        if i == last_number:
            start_index = i * THREASHOLD
            end_index = None
            print(f"Index: [{i * THREASHOLD}  : ]")
        else:
            start_index = i * THREASHOLD
            end_index = THREASHOLD + (i * THREASHOLD)
            print(f"Index: [{i * THREASHOLD}  : {THREASHOLD + (i*THREASHOLD)}]")
        # TODO: have a clause to return if start_index and end_index return empty table
        if table[start_index:end_index].num_rows == 0:
            continue
        
        # once we have start and end indeces, we create timestamps and numpy values
        np_timestamps_, np_values_ = create_values_and_timestamps(table, start_index, end_index) 
        # create data types for dataset columns  
        data_types_ = [TSDataType.FLOAT] * len(measurements_) 
        # create NumpyTablet
        file_name = ROOT + str(dataset_name) if dataset_name == 'powerlog' else ROOT + str(dataset_name) + "." + site_name 
        np_tablet_ = NumpyTablet.NumpyTablet(file_name, measurements_, data_types_, np_values_, np_timestamps_)
        print("Ingested: " + file_path)
        session.insert_tablet(np_tablet_)
    ingestion_time = time.perf_counter() - tic
    # return ingestion_time
    return ingestion_time


def main(file_path:str, dataset_name:str):
    session = create_iotdb_session()
    # give path to multivariate time series dataset
    ingestion_time = create_and_insert_numpy_tablets(file_path, dataset_name, session)
    
    session.execute_non_query_statement("FLUSH")
    session.close()
    return ingestion_time


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} dataset_name path/to/file.parquet")
        sys.exit(0)
    
    dataset_name = sys.argv[1]
    parquet_file_path = sys.argv[2]
    
    logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

    total_ingestion_time = 0
    for file in get_files(parquet_file_path):
        ingestion_time = main(file, dataset_name)
        total_ingestion_time += ingestion_time
    logging.info(f"Ingestion finished in:{total_ingestion_time:0.4f} seconds")