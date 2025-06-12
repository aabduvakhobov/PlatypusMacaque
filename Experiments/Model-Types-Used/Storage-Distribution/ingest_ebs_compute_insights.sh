#!/bin/bash

set -e

table_name=$1
data_file_or_files_path=$2
### Fixed parameters

# Path to Utilities
analysis_script="../../Utilities/compute_storage_insights.py"
ingestion_script="../../Utilities/ingest_parquet_to_modelardb.py"

error_bounds="0 0.01 0.1 1 5 10"
port="127.0.0.1:9999"
# time to sleep for vacuum
sleep_for_vacuum=5

ModelarDB_Data=$HOME/modelardb_data

modelarDB_vanilla_path=../ModelarDB-versions/ModelarDB
modelardb_gorillaV_path=../ModelarDB-versions/ModelarDB-Gorilla
modelardb_legacy_timestamp_path=../ModelarDB-versions/ModelarDB-GorillaTS
modelardb_macaque_path=../ModelarDB-versions/ModelarDB-Macaque



if [ $# -ne 2 ]; 
then
    echo "Usage: script.sh dataset_name /path/to/parquet_files"
    exit 0
fi

if ! [ -f $data_file_or_files_path -o -d $data_file_or_files_path ]
then
    echo "Usage: script.sh dataset_name /path/to/parquet_files"
    exit 0
fi

clean_modelar_data() {
    # remove databases
    if [ -d $ModelarDB_Data/tables ]
    then
        rm -r $ModelarDB_Data/tables
    fi
}

clean_modelar_metadata() {
    # remove databases
    if [ -d $ModelarDB_Data/metadata ]
    then
        rm -r $ModelarDB_Data/metadata
    fi
}

start_modelardb() {
    $1 $ModelarDB_Data &> logs &
}

stop_modelardb() {
    pkill modelardbd
}

create_modelardb_data_path() {
    if ! [ -d $ModelarDB_Data ]
    then
        mkdir $ModelarDB_Data
    fi
}

main() {
    for error_bound in $error_bounds; do
        system_name=$(echo "$1" | rev | cut -d'/' -f1 | rev)
        cargo build --release --manifest-path $1/Cargo.toml
        start_modelardb $1/target/release/modelardbd
        results_file=$error_bound-$table_name-compression_results.log
        # timing ingestion process
        start=$SECONDS
        python3 $ingestion_script "127.0.0.1:9999" $table_name $data_file_or_files_path $error_bound
        # sleep for a minute or two to finish vacuuming
        duration=$((SECONDS-start))
        # write results of the python program logs to the common file
        sleep $sleep_for_vacuum
        echo "Compressed in $duration seconds" > $results_file
        compression_size=$(du -h -d0 $ModelarDB_Data)
        echo "Compression size: $compression_size" >> $results_file
        python3 $analysis_script "$ModelarDB_Data" "$table_name" "$table_name-$error_bound-$system_name.db"
        # stop ModelarDB
        stop_modelardb
        sleep $sleep_for_vacuum
        # We remove old data
        clean_modelar_data
        clean_modelar_metadata
    done
}

create_modelardb_data_path
# start new round with modified modelardb
for path in $modelardb_macaque_path; do
    echo "Starting: $path"
    main $path
done
