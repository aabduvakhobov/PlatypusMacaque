#!/bin/bash

# exit on fail
set -e

error_bounds="0 0.01 0.1 1 5 10"

datasets="powerlog"

BASE_PATH=/home/abduvoris/Paper-2
ROOT_PATH=$(pwd)

MODELARDB_Data_Path="/home/abduvoris/ModelarDB-Home/ModelarDB-data-store"


compute_model_distribution_script="$BASE_PATH/PlatypusMacaque/Experiments/Ablation-Experiment/Ablation-Analysis/extract_data_point_statistics.py"
parquet_ingestor="$BASE_PATH/PlatypusMacaque/Utilities/ingest_parquet_to_modelardb.py"
storage_insights_analysis_script="$BASE_PATH/PlatypusMacaque/Utilities/compute_storage_insights.py"


# Assumes that repository was cloned to home path
ModelarDB_Macaque_Path="$BASE_PATH/PlatypusMacaque/ModelarDB-versions/ModelarDB-Macaque"
Macaque_Value_CNT_Change_Patch="$BASE_PATH/PlatypusMacaque/Experiments/Ablation-Experiment/Ablation-Analysis/ModelarDB-Macaque-Value-Distribution.patch"
base_save_path="$BASE_PATH/PlatypusMacaque/Experiments/Extract-Residuals"

dataset_path=''
function get_dataset_path {
    case $1 in 
    "wind")
        dataset_path="../../data/wind.parquet"
        ;;
    "powerlog")
        dataset_path="/home/abduvoris/Paper-2/Paper-2-General/data/Parquet/raw/powerlog/"
        ;;
    "turbinelog")
        dataset_path="/home/abduvoris/Paper-2/Paper-2-General/data/Parquet/raw/turbinelog/"
        ;;
    esac   
}

function stop_mdb_and_clean {
    pkill modelardbd
    sleep 5
    rm -r $1/*
}

clean_modelar_data() {
    # remove databases
    if [ -d $MODELARDB_Data_Path/tables ]
    then
        rm -r $MODELARDB_Data_Path/tables $MODELARDB_Data_Path/metadata
    fi
}


# temp variable dataset to be replaced with datasets in a loop
# MAIN function
clean_modelar_data
git restore $ModelarDB_Macaque_Path/
cd $ModelarDB_Macaque_Path
git apply $Macaque_Value_CNT_Change_Patch
cd $ROOT_PATH
for dataset in $datasets
do  
    for coefficient in 2 4 8 16
    do
        echo "Coefficient is: $coefficient"
        # Change coefficient 
        sed -E -i "83s/models::VALUE_SIZE_IN_BYTES as f32/models::VALUE_SIZE_IN_BYTES as f32 \/ $coefficient.0/" $ModelarDB_Macaque_Path/crates/modelardb_compression/src/compression.rs
        for eb in $error_bounds
        do  
            cargo build --release --manifest-path $ModelarDB_Macaque_Path/Cargo.toml
            RUST_BACKTRACE=full $ModelarDB_Macaque_Path/target/release/modelardbd $MODELARDB_Data_Path &> ModelarDB-Logs &
            get_dataset_path "$dataset"
            echo "Ingesting dataset: $dataset_path"
            # run ingestion of apache parquet
            # long_dataset_name="longname$dataset"
            python3 "$parquet_ingestor" "127.0.0.1:9999" "$dataset" "$dataset_path" "$eb"
            echo "Ingestion of $dataset with $eb% is done"
            python3 "$compute_model_distribution_script" "$dataset" "$eb" "$coefficient"
            # run storage insights
            # python3 $storage_insights_analysis_script "$MODELARDB_Data_Path" "$dataset" "$coefficient-$eb-$dataset.db" 
            # stop ModelarDB
            stop_mdb_and_clean "$MODELARDB_Data_Path"
        done
        git restore $ModelarDB_Macaque_Path/crates/modelardb_compression/src/compression.rs
    done
done
