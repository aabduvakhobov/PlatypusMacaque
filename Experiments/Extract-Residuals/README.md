Instructions regarding the extraction of Gorilla values from ModelarDB. To replicate this experiment, we ingest apply the patch file to vanilla ModelarDB version, so that when decompression PMC and Swing write sentinel values. Then the provided python script is used to filter out only the subset without sentinel values. 

# How to Run
1. Apply the [patch file](Patches/write_sentinel_value_for_swing_pmc_no_zero_rewrite.patch) to [ModelarDB](ModelarDB-versions/ModelarDB)
2. Activate `modelardb_env` conda environment
3. Run [ingest_only_gorilla.sh](Experiments/Extract-Residuals/ingest_only_gorilla.sh)
   
# Artifacts
- shell script for recursive run of ModelarDB: [ingest_only_gorilla.sh](Extract-ModelarDB-Gorilla-Values/ingest_only_gorilla.sh)
- python script for filtering out Gorilla values: [filter_out_gorilla_indexes.sh](Extract-ModelarDB-Gorilla-Values/filter_out_gorilla_indexes.py)

# Sentinel Values Used
We ensure that when deSentinel values are used for PMC and Swing for filtering out their values from MDB, so that Gorilla compressed values can be found.
PMC_MEAN = f32::INFINITY
Swing = f32::NEG_INFINITY

# Output Datasets
Output datasets are stored in: *~/modelardb_data*


