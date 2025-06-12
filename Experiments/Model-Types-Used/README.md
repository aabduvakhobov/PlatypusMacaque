# For replicating Section-V Experiments

Two experiments are performed: 1) Computing the share of compressed values for each model type; and 2) Computing the storage size share of segments for each model types

## Computing the share of compressed values for each model type
The artifacts are in the [Value-Distribution](Experiments/Model-Types-Used/Value-Distribution) directory.
1. Apply the [patch file](Experiments/Model-Types-Used/Value-Distribution/Patch/Segment-Analysis.patch) to [ModelarDB](ModelarDB-versions/ModelarDB) and compile with release mode
2. Run [ingest_and_compute_model_distribution.sh](Experiments/Model-Types-Used/Value-Distribution/ingest_and_compute_model_distribution.sh)
## Computing the storage size share of segments for each model types
1. Run [ingest_ebs_compute_insights.sh](Experiments/Model-Types-Used/Storage-Distribution/ingest_ebs_compute_insights.sh)