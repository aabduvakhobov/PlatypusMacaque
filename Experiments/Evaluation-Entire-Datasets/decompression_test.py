import sys

from pyarrow import parquet
from pyarrow import flight
import time
import logging

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
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} dataset_name error_bound")
    logging.basicConfig(
        filename=f"{sys.argv[1]}-{sys.argv[2]}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        )
    
    tick = time.perf_counter()
    dataset_name=sys.argv[1]
    # iterate over columns of it
    flight_client = flight.FlightClient("grpc://127.0.0.1:9999")
    ticket = flight.Ticket(f"SELECT * FROM {dataset_name}")
    flight_stream_reader = flight_client.do_get(ticket)
    # We read batches to ensure all data is retrieved
    for flight_stream_chunk in flight_stream_reader:
        _ = flight_stream_chunk.data
    flight_client.close()
    tock = time.perf_counter()
    print(f"Decompression time: {tock - tick:.4f} s")
    logging.info(f"Decompression time: {tock - tick:.4f} s")
    time.sleep(5)
    