#!/bin/bash

# exit on fail
set -e

datasets="wind"

ModelarDB_Path=""
MODELARDB_Data_Path=""

function stop_mdb_and_clean {
    pkill modelardbd
    sleep 5
    rm -r $1/*
}

# temp variable dataset to be replaced with datasets in a loop 
# MAIN function
stop_mdb_and_clean "$MODELARDB_Data_Path"
rm $ModelarDB_Path/Segment-Logs
echo "Done cleaning and stopping"
