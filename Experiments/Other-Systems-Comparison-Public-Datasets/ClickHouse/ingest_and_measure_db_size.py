import sys
import os
import glob
import logging
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as parquet
import clickhouse_connect


CHECK_DB_SIZE_QUERY = """
SELECT formatReadableSize(sum(bytes))
FROM system.parts
WHERE active AND (database = '{}')"""

DROP_DB_QUERY =  "DROP DATABASE IF EXISTS {};"
# Mapping from PyArrow type to ClickHouse type
ARROW_TO_CH = {
    pa.int8(): "Int8",
    pa.int16(): "Int16",
    pa.int32(): "Int32",
    pa.int64(): "Int64",
    pa.uint8(): "UInt8",
    pa.uint16(): "UInt16",
    pa.uint32(): "UInt32",
    pa.uint64(): "UInt64",
    pa.float32(): "Float32",
    pa.float64(): "Float64",
    pa.string(): "String",
    pa.binary(): "String",
    pa.bool_(): "UInt8",   # ClickHouse has no native Boolean
}

def create_clickhouse_database(db_name: str, engine: str = None, if_not_exists: bool = True) -> str:
    """
    Generate a ClickHouse CREATE DATABASE DDL statement.

    Args:
        db_name (str): Name of the database.
        engine (str, optional): Database engine (e.g., "Atomic", "Ordinary"). Defaults to None.
        if_not_exists (bool): Whether to add IF NOT EXISTS. Defaults to True.

    Returns:
        str: CREATE DATABASE SQL statement.
    """
    parts = ["CREATE DATABASE"]
    if if_not_exists:
        parts.append("IF NOT EXISTS")
    parts.append(f"`{db_name}`")
    if engine:
        parts.append(f"ENGINE = {engine}")
    return " ".join(parts) + ";"

def arrow_type_to_clickhouse(dtype: pa.DataType) -> str:
    """Convert a PyArrow data type to ClickHouse type."""
    if pa.types.is_timestamp(dtype):
        # Default precision ns → DateTime64(9)
        unit_to_precision = {"s": 0, "ms": 3, "us": 6, "ns": 9}
        prec = unit_to_precision.get(dtype.unit, 0)
        return f"DateTime64({prec})"
    if dtype in ARROW_TO_CH:
        return ARROW_TO_CH[dtype]
    raise ValueError(f"Unsupported Arrow type: {dtype}")

def arrow_schema_to_clickhouse(schema: pa.Schema, table_name: str, primary_key: str) -> str:
    """Generate ClickHouse CREATE TABLE DDL from PyArrow schema."""
    cols = []
    for field in schema:
        if table_name == 'turbinelog' and field.type == pa.string():
            ch_type = 'Int32'
        else:
            ch_type = arrow_type_to_clickhouse(field.type)
        # Handle nullable fields
        if field.nullable:
            ch_type = f"{ch_type}"
        cols.append(f"  `{field.name}` {ch_type}")
    
    col_defs = ",\n".join(cols)
    statement = (        f"CREATE TABLE `{table_name}` (\n"
        f"{col_defs}\n"
        f") ENGINE = MergeTree\n"
        f"PRIMARY KEY ({primary_key});")
    statement = (f"CREATE TABLE `{table_name}` (\n"
        f"{col_defs}\n"
        f") ENGINE = MergeTree\n"
        f"ORDER BY ({primary_key});")
    return statement


def get_first_timestamp_col(schema_or_table) -> list[str]:
    """
    Retrieve the names of all timestamp columns from a PyArrow Schema or Table.

    Args:
        schema_or_table (pa.Schema | pa.Table): The schema or table to inspect.

    Returns:
        list[str]: List of column names that are timestamps.
    """
    schema = schema_or_table.schema if isinstance(schema_or_table, pa.Table) else schema_or_table
    
    cols = [
        field.name
        for field in schema
        if pa.types.is_timestamp(field.type)
    ]
    return cols[0] if cols else None

def get_files(path):
    if os.path.isdir(path):
        parquet_files = glob.glob(path + os.sep + "*.parquet")
        parquet_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        parquet_files = [path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")
    return parquet_files

import re

def parse_size(size_str: str) -> int:
    """
    Parse a human-readable size string like '558.04 MiB' into bytes.

    Supports KiB, MiB, GiB, TiB, PiB, EiB (binary prefixes, base=1024).
    """
    size_str = size_str.strip()
    
    match = re.match(r"^([\d.]+)\s*([KMGTPE]?iB)$", size_str, re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid size format: {size_str}")
    
    number, unit = match.groups()
    number = float(number)
    unit = unit.lower()

    multipliers = {
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
        "pib": 1024**5,
        "eib": 1024**6,
    }
    
    if unit == "ib":  # in case someone writes just "iB"
        return int(number)
    
    return int(number * multipliers[unit])


def change_turbinelog_schema(table: pa.Table, to_type: pa.DataType = pa.int32()) -> pa.Table:
    """
    Cast all string columns in a PyArrow table to integers, keep others unchanged.

    Args:
        table (pa.Table): Input PyArrow table
        to_type (pa.DataType): Target integer type (default Int64)

    Returns:
        pa.Table: New table with string columns cast to integers
    """
    new_columns = []
    for field in table.schema:
        col = table[field.name]
        if pa.types.is_string(field.type):
            # cast string → int
            new_col = pc.cast(col, to_type)
        else:
            new_col = col
        new_columns.append(new_col)

    return pa.table(new_columns, names=table.column_names)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} db_table_name /path/to/folder_or_file")
        sys.exit(1)
    parquet_file_path = sys.argv[2]
    files = get_files(parquet_file_path)
    
    logging.basicConfig(filename='ingestion.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    
    client = clickhouse_connect.get_client(host='localhost', username='default')
    db_table_name = sys.argv[1]
    if db_table_name == 'turbinelog':
        table = change_turbinelog_schema(parquet.read_table(files[0]))
    else: 
        table = parquet.read_table(files[0])
    # Create DB
    client.command(create_clickhouse_database(db_table_name, None)) 
    client.close()
    # Connect to newly created DB
    client = clickhouse_connect.get_client(host='localhost', username='default', database=db_table_name)
    timestamp_col = get_first_timestamp_col(table.schema)
    # Create table
    client.command(arrow_schema_to_clickhouse(table.schema,db_table_name, timestamp_col))
    total_ingestion_time = 0
    # Insert pyarrow tables
    for file in files:
        table = change_turbinelog_schema(parquet.read_table(file)) if db_table_name == 'turbinelog' else parquet.read_table(file)
        tic = time.perf_counter()
        client.insert_arrow(db_table_name, table, db_table_name)       
        total_ingestion_time += time.perf_counter() - tic
    logging.info(f"Ingestion finished in:{total_ingestion_time:0.4f} seconds")
    db_size_str = client.command(CHECK_DB_SIZE_QUERY.format(db_table_name))
    # Log table size
    logging.info(f"Total DB size:{db_size_str}")
    print(db_size_str)
    print(f"In bytes: {parse_size(db_size_str)}")
    # Drop table
    #client.command(DROP_DB_QUERY.format(db_table_name))
    client.close()