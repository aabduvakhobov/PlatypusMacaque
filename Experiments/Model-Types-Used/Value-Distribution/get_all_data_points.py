import sys

from pyarrow import parquet
import pyarrow
from pyarrow import flight
import time

SELECT_ONE_QUERY = "SELECT {} FROM {};"
Gorilla_Extracted_Files = '/srv/data3/abduvoris/Paper-2-Datasets/gorilla_only_extracted/Parquet/'


def get_safe_col_name(col_name):
    return col_name.lower().replace(" ", "_").replace(".", "")


def get_files(path):
    import os
    ff = []
    for dir, _, files in os.walk(path):
        for f in files:
            ff.append(dir + "/" + f)
    return ff


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} dataset_name original_dataset_file eb save_path")
    
    dataset_name=sys.argv[1]
    original_data_path=sys.argv[2]

    error_bound=sys.argv[3]
    save_path=sys.argv[4]
    
    
    # Read schema of the original dataset
    if dataset_name == 'wind':
        column_names = parquet.read_schema(original_data_path).names
    elif 'powerlog' in dataset_name: 
        name = 'powerlog'
    elif 'turbinelog' in dataset_name:
        name='turbinelog'
    else:
        name=dataset_name
    column_names = get_files(Gorilla_Extracted_Files + f"/{name}/{name}-{error_bound}")
    column_names = [col_name.split('-')[-1].replace('.parquet','') for col_name in column_names]
    # iterate over columns of it
    flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
    for column_name in column_names:
        if column_name.lower() in ['turbine', 'timestamp', 'datetime']: continue
        ticket = flight.Ticket(f"SELECT {get_safe_col_name(column_name)} FROM {dataset_name}")
        flight_stream_reader = flight_client.do_get(ticket)
        # We read batches to ensure all data is retrieved
        for flight_stream_chunk in flight_stream_reader:
            _ = flight_stream_chunk.data
        time.sleep(10)
        # now preprocess segments table
    time.sleep(3)
    flight_client.close()
    time.sleep(5)
    