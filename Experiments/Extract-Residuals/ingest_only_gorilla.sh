#!/bin/bash

# exit on fail
set -e

error_bounds="0 0.01 0.1 5 1 10"

datasets="wind"

parquet_ingestor="$HOME/PlatypusMacaque/Utilities/ingest_parquet_to_modelardb.py"
processing_script="$HOME/PlatypusMacaque/Experiments/Extract-Residuals/filter_out_gorilla_indexes.py"

MODELARDB_Data_Path="~/modelardb_data"
# Assumes that repository was cloned to home path
ModelarDB_Path="$HOME/PlatypusMacaque/ModelarDB-versions/ModelarDB"
base_save_path="$HOME/PlatypusMacaque/Experiments/Extract-Residuals"


dataset_path=''

function get_dataset_path {
    case $1 in 
    "wind")
        dataset_path="../../data/wind.parquet"
        ;;
    esac   
}

function stop_mdb_and_clean {
    rm -r $1/*
    pkill modelardbd
    sleep 5
}

# MAIN function
for dataset in $datasets
do
    for eb in $error_bounds
    do  
        RUST_BACKTRACE=full $ModelarDB_Path/target/release/modelardbd $MODELARDB_Data_Path &> ModelarDB-Logs &
        table_name="$dataset"_"$eb"
        save_path="$base_save_path/$dataset-$eb"
        if [ ! -d $save_path ]
        then
            mkdir $save_path
        fi
        get_dataset_path "$dataset"
        echo "Ingesting dataset: $dataset_path"
        # run ingestion of apache parquet
        # long_dataset_name="longname$dataset"
        python3 "$parquet_ingestor" "127.0.0.1:9999" "$dataset" "$dataset_path" "$eb"
        echo "Ingestion of $dataset with $eb% is done"
        python3 "$processing_script" "$dataset_path" "$dataset" "$eb" "$save_path"
        echo "New datasets created from $dataset $eb"
        stop_mdb_and_clean "$MODELARDB_Data_Path"
    done
done
