"""
ingest_parquet_to_timescale.py
This script automates the ingestion of Parquet files into a TimescaleDB (PostgreSQL with TimescaleDB extension) database table. It performs the following steps:
1. Starts a PostgreSQL server if not already running.
2. Reads the schema from the first Parquet file and creates a corresponding PostgreSQL table.
3. Enables the TimescaleDB extension and creates a hypertable based on the timestamp column.
4. Attaches the PostgreSQL database to DuckDB for efficient Parquet-to-Postgres data transfer.
5. Copies data from Parquet files into the PostgreSQL table using DuckDB.
6. Adds and enforces a compression policy on the TimescaleDB hypertable.
7. Verifies that the number of rows ingested matches the number of rows in the Parquet files.
8. Outputs the table's OID and closes all connections.
Usage:
    python ingest_parquet_to_timescale.py <table_name> <path/to/parquet_files_or_file> <path/to/postgres_database>
Arguments:
    table_name                Name of the table to create and ingest data into.
    path/to/parquet_files     Path to a directory containing Parquet files or a single Parquet file.
    path/to/postgres_database Path to the PostgreSQL database directory (for pg_ctl).
Environment Variables:
    POSTGRES_USER             PostgreSQL user for authentication.
Dependencies:
    - duckdb
    - psycopg2
    - pyarrow
    - glob
    - subprocess
    - time
    - datetime
Functions:
    - get_files(path): Returns a sorted list of Parquet files from a directory or a single file.
    - pyarrow_to_postgres_type(pa_type): Maps PyArrow types to PostgreSQL types.
    - create_schema_from_parquet_table(table, table_name): Generates a CREATE TABLE statement from a Parquet schema.
    - start_postgres_server(path, log_file): Starts the PostgreSQL server.
    - create_postgres_database(dbname): Creates a PostgreSQL database if it does not exist.
    - create_postgres_connection(dbname): Connects to a PostgreSQL database.
    - enable_timescale_db(cur): Enables the TimescaleDB extension.
    - stop_postgres_server(path): Stops the PostgreSQL server.
    - get_timestamp_col(table): Finds the timestamp column in a Parquet table schema.
    - create_hypertable(cur, db_name, timestamp_col): Converts a table into a TimescaleDB hypertable.
    - attach_postgres_to_duckdb(duck_conn, dbname, user): Attaches a PostgreSQL database to DuckDB.
    - copy_parquet_files(files, dbname, duck_con): Copies multiple Parquet files into PostgreSQL via DuckDB.
    - copy_parquet_file(file, dbname, duck_con): Copies a single Parquet file into PostgreSQL via DuckDB.
    - execute_fetch_one(cur, query): Executes a query and fetches a single result.
    - add_compression_policy(cur, table_name, pg_conn): Adds a TimescaleDB compression policy to a table.
    - compress_uncompressed_chunks(cur, table_name): Compresses uncompressed chunks in a TimescaleDB table.
    - main(db_table_name, data_path, path_to_postgres_database): Orchestrates the ingestion process.
Note:
    - The script assumes the PostgreSQL server is accessible locally and that the user has the necessary permissions.
    - The script is designed for batch ingestion and benchmarking scenarios.
"""
import os
import sys
import glob
import subprocess
import time
import datetime

import duckdb
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pyarrow
import pyarrow.parquet as parquet

# ENV Vars
STDOUT = subprocess.PIPE
STDERR = subprocess.PIPE
POSTGRES_USER="postgres"

def get_files(path):
    if os.path.isdir(path):
        parquet_files = glob.glob(path + os.sep + "*.parquet")
        parquet_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        parquet_files = [path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")
    return parquet_files

def pyarrow_to_postgres_type(pa_type):
    if pyarrow.types.is_int8(pa_type) or pyarrow.types.is_int16(pa_type):
        return "SMALLINT"
    elif pyarrow.types.is_int32(pa_type):
        return "INTEGER"
    elif pyarrow.types.is_int64(pa_type):
        return "BIGINT"
    elif pyarrow.types.is_uint8(pa_type) or pyarrow.types.is_uint16(pa_type):
        return "SMALLINT"
    elif pyarrow.types.is_uint32(pa_type):
        return "INTEGER"
    elif pyarrow.types.is_uint64(pa_type):
        return "BIGINT"
    elif pyarrow.types.is_float32(pa_type):
        return "REAL"
    elif pyarrow.types.is_float64(pa_type):
        return "DOUBLE PRECISION"
    elif pyarrow.types.is_boolean(pa_type):
        return "BOOLEAN"
    elif pyarrow.types.is_string(pa_type) or pyarrow.types.is_large_string(pa_type):
        return "TEXT"
    elif pyarrow.types.is_binary(pa_type) or pyarrow.types.is_large_binary(pa_type):
        return "BYTEA"
    elif pyarrow.types.is_timestamp(pa_type):
        return "TIMESTAMP"
    elif pyarrow.types.is_date32(pa_type) or pyarrow.types.is_date64(pa_type):
        return "DATE"
    elif pyarrow.types.is_decimal(pa_type):
        return f"NUMERIC({pa_type.precision}, {pa_type.scale})"
    else:
        raise ValueError(f"Unsupported pyarrow type: {pa_type}")


def create_schema_from_parquet_table(table: pyarrow.Table, table_name: str) -> str:
    fields = table.schema
    columns = []
    for field in fields:
        pg_type = pyarrow_to_postgres_type(field.type)
        null_str = "" if field.nullable else " NOT NULL"
        columns.append(f'"{field.name}" {pg_type}{null_str}')
    columns_str = ",\n  ".join(columns)
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  {columns_str}\n);'


def start_postgres_server(path:str, log_file:str):
    # Start PostgreSQL in the background
    subprocess.Popen(
        ["pg_ctl", "start", "-D", path, "-l", log_file], 
        stdout=STDOUT, 
        stderr=STDERR
    )
    print("Starting server in 10s")
    time.sleep(10)


def create_postgres_database(dbname):
    # Connect to the default database (e.g., "postgres")
    conn = psycopg2.connect(
        dbname="postgres",
        user=POSTGRES_USER,
        password="postgres",
        host="localhost",
        port=5432
    )
    # Set autocommit so CREATE DATABASE works
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    # Check if database exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    exists = cur.fetchone()
    if not exists:
        cur.execute(f'CREATE DATABASE "{dbname}";')
        print(f"Database '{dbname}' created.")
    else:
        print(f"Database '{dbname}' already exists.")
        
    conn = psycopg2.connect(f"dbname={dbname} user='{POSTGRES_USER}'")
    return conn


def create_postgres_connection(dbname):
    return psycopg2.connect(f"dbname={dbname} user='{POSTGRES_USER}'")

    
def enable_timescale_db(cur):
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    
    
def stop_postgres_server(path):
    subprocess.run(["pg_ctl", "stop", "-D", path], stdout=STDOUT, stderr=STDERR)


def get_timestamp_col(table):
    for schema in table.schema:
        if pyarrow.types.is_timestamp(schema.type):
            return schema.name
    print("Timestamp column does not exist in your first file")
    sys.exit(0)
    


def create_hypertable(cur, db_name, timestamp_col):
    cur.execute(f"SELECT create_hypertable('{db_name}', by_range('{timestamp_col}'))")


def attach_postgres_to_duckdb(duck_conn, dbname, user=POSTGRES_USER):
    # sample connection string: "postgresql://zg03zi@cs.aau.dk:postgres@127.0.0.1:5432/your_database"
    duck_conn.sql(f"ATTACH 'dbname={dbname} user={user} host=127.0.0.1' AS db (TYPE postgres);")
    

def copy_parquet_files(files, dbname, duck_con):
    # Copy your parquet to duckdb: `duckdb -s "COPY db.<TABLE_NAME> FROM '<FILENAME>.parquet' (FORMAT parquet);"`
    file_count = 0
    for file in files:
        duck_con.sql(f"COPY db.{dbname} FROM '{file}' (FORMAT parquet);")
        file_count += parquet.read_metadata(file).num_rows
        print(f"Copied: {file}")
    return file_count


def copy_parquet_file(file, dbname, duck_con):
    # Copy your parquet to duckdb: `duckdb -s "COPY db.<TABLE_NAME> FROM '<FILENAME>.parquet' (FORMAT parquet);"`
    file_count = 0
    duck_con.sql(f"COPY db.{dbname} FROM '{file}' (FORMAT parquet);")
    file_count += parquet.read_metadata(file).num_rows
    print(f"Copied: {file}")
    return file_count


def execute_fetch_one(cur, query):
    cur.execute(query)
    return cur.fetchone()[0]


def add_compression_policy(cur, table_name, pg_conn):
    if table_name == 'turbinelog':
        cur.execute(f"ALTER TABLE {table_name} SET (timescaledb.compress, timescaledb.compress_segmentby = 'turbine');")
    elif table_name not in ['powerlog', 'wind']:
        cur.execute(f"ALTER TABLE {table_name} SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');")
    else:
        cur.execute(f"ALTER TABLE {table_name} SET (timescaledb.compress);")
    cur.execute(f"SELECT add_compression_policy('{table_name}', INTERVAL '1 day');")
    # Force compression
    pg_conn.commit()
    

def compress_uncompressed_chunks(cur, table_name):
    cur.execute(f"SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks( '{table_name}', older_than => INTERVAL '1 day') i;")
    

def main(db_table_name, data_path, path_to_postgres_database):
    # start postgres table
    start_postgres_server(path_to_postgres_database, 'postgres_logs.log')
    # get all files
    files = get_files(data_path)
    first_table = parquet.read_table(files[0])
    timestamp_col = get_timestamp_col(first_table)
    
    # create postgres table
    pg_conn = create_postgres_database(db_table_name)
    # create table in a database
    cur = pg_conn.cursor()
    
    # create postgres table from the schema
    cur.execute(create_schema_from_parquet_table(first_table, db_table_name))
    
    # enable timescale db extension
    enable_timescale_db(cur)
    create_hypertable(cur, db_table_name, timestamp_col)
    
    pg_conn.commit()
    # create duckdb connection
    duck_conn = duckdb.connect()
    # attach postgres db to duckdb
    attach_postgres_to_duckdb(duck_conn, db_table_name)
    # create compression policy with postgres
    add_compression_policy(cur, db_table_name, pg_conn)
    print(f"Started compression at: {datetime.datetime.now()}")    
    # copy your parquet files to postgres with duckdb
    tic = time.perf_counter()
    num_parquet_rows = 0
    for file in files[:5]:
        num_parquet_rows += copy_parquet_file(file, db_table_name, duck_conn)
    time.sleep(300)
    compress_uncompressed_chunks(cur, db_table_name)
    # num_parquet_rows = copy_parquet_files(files, db_table_name, duck_conn)
    print(f"Copied to postgres in {time.perf_counter()-tic:.4f} seconds")
    duck_conn.close()
    # check that count rows match
    db_num_rows = execute_fetch_one(cur, f"SELECT COUNT(*) FROM {db_table_name};")
    if num_parquet_rows != db_num_rows:
        print("Parquet and Postgres table numbers: DO NOT MATCH")
        sys.exit(0)
    else:
        print("Parquet and Postgres table numbers: MATCH")
    # print oid
    oid = execute_fetch_one(cur, f"SELECT oid FROM pg_class where relname='{db_table_name}';")
    print(f"{db_table_name}'s OID: {oid}")
    # Closing the cursor and pg connection
    cur.close()
    pg_conn.close()
    # stop the postgres server
    #stop_postgres_server(path_to_postgres_database)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} table_name path/to/parquet_files /path/to/postgres_database")
        sys.exit(0)
    table_name = sys.argv[1]
    data_path = sys.argv[2]
    path_to_postgres_database = sys.argv[3]

    main(table_name, data_path, path_to_postgres_database)
