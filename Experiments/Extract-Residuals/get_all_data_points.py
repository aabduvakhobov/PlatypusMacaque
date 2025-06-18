import sys

from pyarrow import parquet
import pyarrow
from pyarrow import flight

PMC_SENTINEL_VALUE = -888.0
SWING_SENTINEL_VALUE = -999.0

def get_safe_col_name(col_name):
    return col_name.lower().replace(" ", "_")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} dataset_name eb save_path")
        sys.exit(0)
    
    dataset_name=sys.argv[1]
    error_bound=float(sys.argv[2])
    save_path=sys.argv[3]
    
    flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
    ticket = flight.Ticket(f"SELECT * FROM {dataset_name}")
    flight_stream_reader = flight_client.do_get(ticket)

    output_table = None
    for flight_stream_chunk in flight_stream_reader:
        record_batch = flight_stream_chunk.data
        if output_table is None:
            output_table = pyarrow.Table.from_batches([record_batch])
        else:
            output_table = pyarrow.concat_tables([output_table, pyarrow.Table.from_batches([record_batch])])
    if output_table is not None and output_table.num_rows > 0:
        #TODO: rewrite this logic with only pyarrow
        # output_df = output_df.to_pandas().reset_index(drop=True)
        # output_df = output_df.reset_index(drop=True)
        ts_col = None
        for col in output_table.column_names:
            if col.lower() in ['ts', 'datetime', 'time', 'date', 'timestamp']:
                ts_col = col
                continue
            temp_table = output_table.select([ts_col, col])
            # check if dataset is not empty
            if temp_table.num_rows > 10000:
                print(col)
                parquet.write_table(
                    temp_table,
                    save_path+f'/{error_bound}-{col}.parquet', 
                    compression="snappy"
                    )