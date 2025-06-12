#!/bin/bash

# exit on fail
set -e

# error_bounds="0 0.01 0.1 1 5 10"
error_bounds="0.1"
# datasets="BAMES FastlogTidTwo"
# datasets="Fastlog_d1_m1 Fastlog_d10_m12 Fastlog_d15_m1 Engie_active_power Engie_pitch_angle Engie_rotor_speed Engie_cor_nacelle_direction Engie_wind_speed Engie_cor_wind_direction"

# datasets="powerlog"
datasets="wind"


parquet_ingestor="/home/cs.aau.dk/zg03zi/ModelarDB-Home/ModelarDB-Utilities/Apache-Parquet-Loader/main.py"
get_all_data_points_script='/home/cs.aau.dk/zg03zi/Paper-2-General/Extract-ModelarDB-Gorilla-Values/get_all_data_points.py'
MODELARDB_Data_Path="/srv/data4/abduvoris/modelardb_data"
ModelarDB_Path="/home/cs.aau.dk/zg03zi/ModelarDB-Home/ModelarDB-RS"
save_path="/srv/data3/abduvoris/Paper-2-Datasets/segment_inspection/all_values"

function stop_mdb_and_clean {
    pkill modelardbd
    sleep 5
    rm -r $1/*
}

# temp variable dataset to be replaced with datasets in a loop
# dataset="sgre_2days" 
# MAIN function
stop_mdb_and_clean "$MODELARDB_Data_Path"
echo "Done cleaning and stopping"
