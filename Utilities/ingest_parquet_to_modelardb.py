"""This is a copy of: https://github.com/ModelarData/Utilities/blob/main/Apache-Parquet-Loader/main.py commit hash [version](https://github.com/ModelarData/Utilities/commit/021f53a5d9bcf6d3c1ad9ec0ec135b1748b00304)
Authors: Søren Kejser Jensen and Christian Schmidt Godiksen.
"""
import os
import sys
import glob

import pyarrow
from pyarrow import parquet
from pyarrow import flight


# Helper Functions.
def table_exists(flight_client, table_name):
    tables = map(lambda flight: flight.descriptor.path, flight_client.list_flights())
    return [bytes(table_name, "UTF-8")] in tables


def create_model_table(flight_client, table_name, schema, error_bound):
    # Construct the CREATE MODEL TABLE string with column names quoted to also
    # support special characters in column names such as spaces and punctuation.
    columns = []
    for field in schema:
        if field.type == pyarrow.timestamp("ms") or field.type == pyarrow.timestamp("us"):
            columns.append(f"`{field.name}` TIMESTAMP")
        elif field.type == pyarrow.float32():
            columns.append(f"`{field.name}` FIELD({error_bound}%)")
        # column device is added by us to be able to ingest data to TimescaleDB
        elif field.name == 'device':
            continue
        elif field.type == pyarrow.string():
            columns.append(f"`{field.name}` TAG")
        else:
            raise ValueError(f"Unsupported Data Type: {field.type}")

    sql = f"CREATE MODEL TABLE {table_name} ({', '.join(columns)})"

    # Execute the CREATE MODEL TABLE command.
    ticket = flight.Ticket(str.encode(sql))
    result = flight_client.do_get(ticket)
    return list(result)


def read_parquet_file_or_folder(path):
    # Read Apache Parquet file or folder.
    arrow_table = parquet.read_table(path)
    arrow_table = arrow_table.select([col for col in arrow_table.column_names if col != 'device'])

    # Ensure the schema only uses supported types.
    columns = []
    column_names = []
    for field in arrow_table.schema:
        column_names.append(field.name)

        if field.type == pyarrow.float16() or field.type == pyarrow.float64():
            # Ensure fields are float32 as others are not supported.
            columns.append((field.name, pyarrow.float32()))
        elif field.type in [
            pyarrow.timestamp("s"),
            pyarrow.timestamp("us"),
            pyarrow.timestamp("ns"),
        ]:
            # Ensure timestamps are timestamp[ms] as others are not supported.
            columns.append((field.name, pyarrow.timestamp("us")))
        else:
            columns.append((field.name, field.type))

    safe_schema = pyarrow.schema(columns)

    # Cast the columns to the supported types.
    arrow_table = arrow_table.rename_columns(column_names)
    return arrow_table.cast(safe_schema)


def do_put_arrow_table(flight_client, table_name, arrow_table):
    upload_descriptor = flight.FlightDescriptor.for_path(table_name)
    writer, _ = flight_client.do_put(upload_descriptor, arrow_table.schema)
    writer.write(arrow_table)
    writer.close()

    # Flush the data to disk.
    action = flight.Action("FlushMemory", b"")
    result = flight_client.do_action(action)
    return list(result)


# Main Function.
if __name__ == "__main__":
    if len(sys.argv) != 4 and len(sys.argv) != 5:
        print(f"usage: {sys.argv[0]} host table parquet_file_or_folder [relative_error_bound]")
        sys.exit(1)

    flight_client = flight.FlightClient(f"grpc://{sys.argv[1]}")
    table_name = sys.argv[2]
    error_bound = sys.argv[4] if len(sys.argv) == 5 else "0.0"

    if os.path.isdir(sys.argv[3]):
        parquet_files = glob.glob(sys.argv[3] + os.sep + "*.parquet")
        parquet_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(sys.argv[3]):
        parquet_files = [sys.argv[3]]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")

    arrow_table = read_parquet_file_or_folder(parquet_files[0])
    if not table_exists(flight_client, table_name):
        create_model_table(flight_client, table_name, arrow_table.schema, error_bound)

    for index, parquet_file in enumerate(parquet_files):
        print(f"- Processing {parquet_file} ({index + 1} of {len(parquet_files)})")
        arrow_table = read_parquet_file_or_folder(parquet_file)
        do_put_arrow_table(flight_client, table_name, arrow_table)