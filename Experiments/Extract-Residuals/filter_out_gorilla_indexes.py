import sys
import numpy as np

from pyarrow import parquet
import pyarrow
from pyarrow import flight

PMC_SENTINEL_VALUE = np.inf
SWING_SENTINEL_VALUE = -np.inf
TURBINE_ID = 2310183 

def get_safe_col_name(col_name):
    return col_name.lower().replace(" ", "_")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} original_dataset_path dataset eb save_path")
    
    original_dataset_path=sys.argv[1]
    dataset=sys.argv[2]
    error=float(sys.argv[3])
    save_path=sys.argv[4]
    
    original_df = parquet.read_table(original_dataset_path)
    
    flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
    ticket = flight.Ticket(f"SELECT * FROM {dataset}")
    flight_stream_reader = flight_client.do_get(ticket)

    output_table = None
    for flight_stream_chunk in flight_stream_reader:
        record_batch = flight_stream_chunk.data
        if output_table is None:
            output_table = pyarrow.Table.from_batches([record_batch])
        else:
            output_table = pyarrow.concat_tables([output_table, pyarrow.Table.from_batches([record_batch])])
    if output_table is not None and output_table.shape[0] > 0:
        #TODO: rewrite this logic with only pyarrow
        # output_df = output_df.to_pandas().reset_index(drop=True)
        # output_df = output_df.reset_index(drop=True)
        for col in original_df.column_names:
            if col in ['ts', 'datetime', 'Time', 'date', 'TimeStamp', 'timestamp', 'Turbine']:
                continue
            safe_name = get_safe_col_name(col)
            tempdf = output_table.to_pandas().reset_index(drop=True)
            # We only keep one turbine ID for turbinelog
            if dataset == 'turbinelog':
                tempdf = tempdf.loc[tempdf[get_safe_col_name('Turbine')] == str(TURBINE_ID)]
            tempdf = tempdf.loc[~tempdf[safe_name].isin([PMC_SENTINEL_VALUE, SWING_SENTINEL_VALUE]), safe_name]
            # check if dataset is not empty
            if tempdf.shape[0] > 10000:
                print(col)
                parquet.write_table(
                    original_df.take(tempdf.index.tolist()).flatten().select([col]), 
                    save_path+f'/{error}-{col}.parquet', 
                    compression="snappy"
                    )
