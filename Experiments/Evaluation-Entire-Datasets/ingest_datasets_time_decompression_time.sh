#!/bin/bash

# Exit when any command fails
set -e

table_name=$1
data_file_or_files_path=$2
error_bounds="0 0.01 0.1 1 5 10" # TODO check always
port="127.0.0.1:9999"
# time to sleep for vacuum
sleep_for_vacuum=5

ModelarDB_Data="~/modelardb_data"

modelardb_vanilla_path=../../ModelarDB-versions/ModelarDB
modelardb_macaque_path=../../ModelarDB-versions/ModelarDB-Macaque

decompression_test_script="decompression_test.py"
parquet_ingestor="../../Utilities/ingest_parquet_to_modelardb.py"

output_dir=$(pwd)

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
        for error_bound in $error_bounds; do
                cargo build --release --manifest-path $1/Cargo.toml
                start_modelardb $1/target/release/modelardbd
                system_name=$(echo "$1" | rev | cut -d'/' -f1 | rev)
                results_file=$error_bound-$table_name-$system_name-compression_results.log
                # timing ingestion process
                start=$SECONDS
                python3 $parquet_ingestor "127.0.0.1:9999" $table_name $data_file_or_files_path $error_bound
                # sleep for a minute or two to finish vacuuming
                duration=$((SECONDS-start))
                # write results of the python program logs to the common file
                sleep $sleep_for_vacuum
                echo "Compressed in $duration seconds" > $results_file
                compression_size=$(du -h -d0 $ModelarDB_Data)
                echo "Compression size: $compression_size" >> $results_file
                python3 $decompression_test_script $table_name $error_bound >> $results_file
                # stop ModelarDB
                stop_modelardb
                sleep $sleep_for_vacuum
                # We remove old data
                clean_modelar_data
        done
}

# stop_modelardb
clean_modelar_data
# start new round with modified modelardb
for path in $modelardb_vanilla_path $modelardb_macaque_path; do
    echo "Starting: $path"
    main $path
done
