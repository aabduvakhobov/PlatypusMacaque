"""_summary_
This module provides utility functions for managing PostgreSQL databases and interacting with TimescaleDB.
Functions:
    drop_database(dbname, user, password, host="localhost", port=5432):
        Drops a specified PostgreSQL database after terminating all active connections to it.
    create_postgres_connection(dbname, user=USER):
        Creates and returns a connection to a specified PostgreSQL database.
    execute_fetch_one(dbname, query):
        Executes a query on a specified PostgreSQL database and fetches the first result.
Usage:
    This script can be executed directly from the command line with the following arguments:
        1. database_name: The name of the database to operate on.
        2. job_name: The operation to perform. Supported values are:
            - 'drop': Drops the specified database.
            - 'check_uncompressed_chunks': Checks and prints the count of uncompressed chunks in a TimescaleDB hypertable.
Example:
    To drop a database:
        python utils.py my_database drop
    To check uncompressed chunks:
        python utils.py my_database check_uncompressed_chunks
Dependencies:
    - psycopg2: A PostgreSQL adapter for Python.
Constants:
    USER: Default username for database connections.
    PASSWORD: Default password for database connections.

Returns:
    _type_: _description_
"""
import sys

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import subprocess
import time

# ENV Vars
STDOUT = subprocess.PIPE
STDERR = subprocess.PIPE
USER="zg03zi@cs.aau.dk"
PASSWORD="postgres"

def drop_database(dbname, user, password, host="localhost", port=5432):
    # Connect to a different database (not the one you're dropping)
    conn = psycopg2.connect(
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Terminate connections to the database (PostgreSQL restricts DROP otherwise)
    cur.execute(f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = %s AND pid <> pg_backend_pid();
    """, (dbname,))

    # Drop the database
    cur.execute(f'DROP DATABASE IF EXISTS "{dbname}";')
    print(f"Database '{dbname}' dropped.")

    cur.close()
    conn.close()
    
    
def create_postgres_connection(dbname, user=USER):
    return psycopg2.connect(f"dbname={dbname} user='{user}'")


def execute_fetch_one(dbname, query):
    conn = create_postgres_connection(dbname)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result


def start_postgres_server(path:str, log_file:str):
    # Start PostgreSQL in the background
    subprocess.Popen(
        ["pg_ctl", "start", "-D", path, "-l", log_file], 
        stdout=STDOUT, 
        stderr=STDERR
    )
    print("Starting server in 10s")
    time.sleep(10)


def stop_postgres_server(path):
    subprocess.run(["pg_ctl", "stop", "-D", path], stdout=STDOUT, stderr=STDERR)
    

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} database_name job_name[drop or check_uncompressed_chunks] /path/to/postgres_data")
        sys.exit(0)
    # start the postgres server 
    start_postgres_server(sys.argv[3], 'postgres_logs.log')
    if sys.argv[2] == 'drop':
        drop_database(sys.argv[1], USER, PASSWORD)
        print(f"{sys.argv[1]} is dropped!")
    elif sys.argv[2] == 'check_uncompressed_chunks':
        query_to_check_compressed_chunks = "SELECT COUNT(*) FROM timescaledb_information.chunks WHERE is_compressed <> 't' AND hypertable_name = '{}';"
        uncompressed_chunks = execute_fetch_one(sys.argv[1], query_to_check_compressed_chunks.format(sys.argv[1]))
        print(f"Uncompressed chunks: {uncompressed_chunks}")
    # stop the postgres server
    stop_postgres_server(sys.argv[3])