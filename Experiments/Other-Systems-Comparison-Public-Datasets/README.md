This section is dedicated to run experiments with [Apache IoTDB v1.3.3](https://iotdb.apache.org/UserGuide/V1.3.x/QuickStart/QuickStart_apache.html) and [TimescaleDB v2.16.1](https://github.com/timescale/timescaledb/releases/tag/2.16.1) using the public datasets.

## Apache IoTDB
1. Download Apache IoTDB v1.3.3 [binaries](https://archive.apache.org/dist/iotdb/1.3.3/)
2. Set `IoTDB_HOME` environment variable to point to IoTDB download folder
3. Install the needed python libraries
4. Run the bash [script](IoTDB/run_ingest_iotdb.sh)

## TimescaleDB
1. [Install TimescaleDB v2.16.1](https://github.com/timescale/timescaledb/releases/tag/2.16.1).
2. Install the needed python dependencies
3. Run the python [script](TimescaleDB/ingest_parquet_to_timescale.py)

## ModelarDB
Datasets can be evaluated using the standard bash scripts provided for another experiments like this [one](../../Experiments/Impact-Proposed-Methods/ingest_dataset.sh).

## Apache Parquet
1. Install the needed python libraries
2. Use scripts in the [Public-Datasets](./Public-Datasets) directory for each dataset. P.S. Wind dataset is already in parquet format and can be found [here](../../data/)

## Public Datasets
Information on the used public datasets can be found in the following [link](Public-Datasets/README.md).