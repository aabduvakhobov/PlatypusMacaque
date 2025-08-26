#!/bin/bash

set -e

ModelarDB_PATH="../../ModelarDB-versions/ModelarDB"
# Where ModelarDB stores data
ModelarDB_Data=$HOME/ModelarDB-Home/ModelarDB-data-store/

# Confirm that path to ModelarDB Utilities is correct
ingestion_script="../../Utilities/ingest_parquet_to_modelardb.py"
output_dir=$(pwd)

table_name=$1
data_dir=$2
error_bound=$3
batch_size=$4

# ModelarDB's default port number
port="127.0.0.1:9999"
# time to sleep for vacuum
sleep_for_vacuum=5


if [ $# -ne 4 ]; 
then
    echo "Usage: script.sh dataset_name /path/to/parquet_files error_bound batch_size"
    exit 0
fi

if ! [ -d $data_dir ]
then
    echo "Usage: script.sh dataset_name /path/to/parquet_files"
    exit 0
fi

clean_modelar_data() {
    # remove databases
    if [ -d $ModelarDB_Data/tables ]
    then
        rm -r $ModelarDB_Data/tables $ModelarDB_Data/metadata
    fi
}

start_modelardb() {
    $1 $ModelarDB_Data &> logs &
}

stop_modelardb() {
    pkill modelardbd
}

main() {
    echo "batch_size is: $batch_size"
    # sed -E -i "61s/[0-9]+ \* 1024/$batch_size * 1024/" $ModelarDB_PATH/crates/modelardb_server/src/storage/mod.rs
    # Ensure release build is done
    # cargo build --release --manifest-path $1/Cargo.toml
    echo "ATTENTION started ModelarDBD"
    start_modelardb $1/target/release/modelardbd
    results_file=$batch_size-$error_bound-$table_name-compression_results.log
    # timing ingestion process
    start=$SECONDS
    sleep 5
    python3 $ingestion_script "127.0.0.1:9999" $table_name $data_dir $error_bound
    # sleep for a minute or two to finish vacuuming
    duration=$((SECONDS-start))
    # write results of the python program logs to the common file
    sleep $sleep_for_vacuum
    echo "Compressed in $duration seconds" > $results_file
    compression_size=$(du -h -d0 $ModelarDB_Data)
    echo "Compression size: $compression_size" >> $results_file
    # stop ModelarDB
    stop_modelardb
    sleep $sleep_for_vacuum
    # We remove old data after each ingestion
    clean_modelar_data
}

# Main function
clean_modelar_data
# start new round with modified modelardb
echo "Starting: $ModelarDB_PATH"
# compress_error_bounds $path
main $ModelarDB_PATH