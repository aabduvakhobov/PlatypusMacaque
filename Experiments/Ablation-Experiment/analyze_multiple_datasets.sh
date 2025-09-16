#!/bin/bash

set -e
 
path=""
get_dataset_path()
    if [ $1 = "powerlog" ]
    then 
        path=/srv/data3/abduvoris/Paper-2-Datasets/raw/powerlog/
    elif [ $1 = "turbinelog" ]
    then
        path=/srv/data3/abduvoris/Paper-2-Datasets/raw/turbinelog/
	elif [ $1 = "blued" ]
    then
        path=/srv/data6/abduvoris/Paper-2-Public-Datasets/BLUED/
    elif [ $1 = "neon_wind" ]
    then
        path=/srv/data3/abduvoris/Paper-2-Datasets/Public-Datasets/NEON_wind_2d_parquet/
    elif [ $1 = "wind" ]
    then
        path=/srv/data3/abduvoris/Paper-2-Datasets/Public-Datasets/Wind-Carlos/wind.parquet
    elif [ $1 = "redd" ]
    then
        path=/srv/data3/abduvoris/Paper-2-Datasets/Public-Datasets/REDD/REDD-RawData/high_freq_parquet/
    else
        echo "Provide correct dataset name[neon_wind, blued, powerlog or turbinelog]"
        exit 0
    fi

for dataset in "turbinelog" "powerlog"; do
    get_dataset_path $dataset
    bash analyze_ablation_experiment.sh $dataset $path
done