#!/bin/bash

set -e

ModelarDB_vanilla_path="../../ModelarDB-versions/ModelarDB/"
ModelarDB_macaque_path="../../ModelarDB-versions/ModelarDB-Macaque/"
# Where ModelarDB stores data
ModelarDB_Data="/srv/data4/abduvoris/modelardb_data/"

# TODO: confirm that path to ModelarDB Utilities is correct
ingestion_script="../../Utilities/ingest_parquet_to_modelardb.py"
output_dir=$(pwd)

table_name=$1
data_dir=$2

error_bounds="0 0.01 0.1 1 5 10"
#error_bounds="0 0.01 0.1"
# ModelarDB's default port number
port="127.0.0.1:9999"
# time to sleep for vacuum
sleep_for_vacuum=5

pmc_only_patch="../../Experiments/Ablation-Experiment/pmc_only.patch"
swing_only_patch="../../Experiments/Ablation-Experiment/swing_only.patch"
pmc_and_swing_only_patch="../../Experiments/Ablation-Experiment/pmc_and_swing_only.patch"
gorilla_only_patch="../../Experiments/Ablation-Experiment/gorilla_only.patch"
macaque_only_patch="../../Experiments/Ablation-Experiment/macaque_only.patch"

if [ $# -ne 2 ]; 
then
    echo "Usage: script.sh dataset_name /path/to/parquet_files"
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

compress_error_bounds() {
    for patch in $macaque_only_patch; do
        git restore .
        git apply $patch
        patch_name=$(basename -s .patch "$patch")
        echo "Patch name is: $patch_name"
        for error_bound in $error_bounds; do
            # Ensure release build is done
            cargo build --release --manifest-path $1/Cargo.toml
            start_modelardb $1/target/release/modelardbd
            results_file=$patch_name-$error_bound-$table_name-compression_results.log
            # timing ingestion process
            start=$SECONDS
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
        done
    done
}

# sleep 60

# Main function
clean_modelar_data
# start new round with modified modelardb
cd $ModelarDB_macaque_path
for path in $ModelarDB_macaque_path; do
    echo "Starting: $path"
    # compress_error_bounds $path
    compress_error_bounds $path
done
