import sys
import csv
import numpy as np

import pyarrow
from pyarrow import flight

PMC_SENTINEL_VALUE = "-999.0"
SWING_SENTINEL_VALUE = "999.0"
SELECT_ONE_QUERY = "SELECT * FROM {} LIMIT 1;"
CNT_GORILLA_QUERY = "SELECT COUNT(timestamp) FROM {} WHERE {} <> {} AND {} <> {}"
CNT_PMC_QUERY = "SELECT COUNT(timestamp) FROM {} WHERE {} == " + PMC_SENTINEL_VALUE
CNT_SWING_QUERY = "SELECT COUNT(timestamp) FROM {} WHERE {} == " + SWING_SENTINEL_VALUE

CNT_QUERIES = {"gorilla" : CNT_GORILLA_QUERY, "pmc" : CNT_PMC_QUERY, "swing": CNT_SWING_QUERY,}

def get_safe_col_name(col_name):
    return col_name.lower().replace(" ", "_")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} dataset error_bound coefficient")
        sys.exit(0)

    dataset=sys.argv[1]
    error=float(sys.argv[2])
    coefficient=sys.argv[3]
    
    flight_client = flight.FlightClient("grpc://127.0.0.1:9999") 
    ticket = flight.Ticket(SELECT_ONE_QUERY.format(dataset))
    flight_stream_reader = flight_client.do_get(ticket)
    column_names = None
    for flight_stream_chunk in flight_stream_reader:
        record_batch = flight_stream_chunk.data
        column_names = pyarrow.Table.from_batches([record_batch]).schema.names
    header = ['dataset','error_bound','coefficient','model_type', 'signal', 'value_cnt']
    rows = [header]
    for column_name in column_names:
        if column_name in ['timestamp', 'TimeStamp', 'time', 'date', 'datetime']: continue
        for model_type, model_type_query in CNT_QUERIES.items():
            if model_type == 'gorilla':
                model_type_query = model_type_query.format(dataset, column_name, SWING_SENTINEL_VALUE, column_name, PMC_SENTINEL_VALUE)
            else:
                model_type_query = model_type_query.format(dataset, column_name)
            ticket = flight.Ticket(model_type_query)
            flight_stream_reader = flight_client.do_get(ticket)
            try:
                value = flight_stream_reader.read_pandas().iloc[0,0]
            except Exception as e:
                print("Received exception while reading data: " + e)
                sys.exit(0)
            
            if value == None:
                print("Received null for response")
            rows.append([dataset, error, coefficient, model_type, column_name, value])
    
    with open(f'model_distribution_result_{dataset}-{str(error)}-{coefficient}.csv', 'w', newline = '') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(rows)