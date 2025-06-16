#!/bin/bash

# Exit when any command fails
set -e

ingestion_script=$1
dataset_name=$2
data_dir=$3


# time to sleep for vacuum
sleep_for_vacuum=5
# Set IoTDB_HOME to current dir
IoTDB_HOME=
output_dir=$(pwd)

print_usage() {
    echo "Usage: script.sh ingestion_py_script dataset_name /path/to/parquet_files"
}

if [ $# -ne 3 ]; 
then
    print_usage
    exit 0
fi

# if [ -d $data_dir ]; 
# then
#     print_usage
#     exit 0
# fi


clean_iotdb_data() {
    # remove databases
    if [ -d $IoTDB_HOME/data ]
    then
        rm -r $IoTDB_HOME/data $IoTDB_HOME/ext $IoTDB_HOME/logs
    fi
}

# remove databases

# clean IoTDB data
clean_iotdb_data

bash $IoTDB_HOME/sbin/start-confignode.sh -d
sleep 10
bash $IoTDB_HOME/sbin/start-datanode.sh -d

sleep 10
# timing ingestion process
start=$SECONDS
python3 $ingestion_script $dataset_name $data_dir
# sleep for a minute or two to finish vacuuming
duration=$((SECONDS-start))
# write results of the python program logs to the common file
mv app.log $dataset_name.log
sleep $sleep_for_vacuum
echo "Processed in $duration seconds" >> $output_dir/$dataset_name.log

bash $IoTDB_HOME/sbin/stop-confignode.sh
bash $IoTDB_HOME/sbin/stop-datanode.sh
sleep 10
du -h -d0 $IoTDB_HOME/data/ >> $output_dir/$dataset_name.log
# TODO write clean code for path
# Clean iotdb data again
clean_iotdb_data