# PlatypusMacaque
Implementation and util scripts for ModelarDB's novel model fitting method _Platypus_ and novel compression method _Macaque_. 

__Platypus__ is a novel model fitting method for ModelarDB that improves [ModelarDB](https://github.com/ModelarData/ModelarDB-RS)'s compression by:
1. Optimizing the use of 's more efficient model types;
2. Removing the need to configure many other parameters other than the error bound. 

__Macaque__ is a novel time series compression method that includes 1) _MacaqueV_ for error-bounded value compression; 2) _MacaqueTS_ for specialized compression of regular and irregular timestamps.

## Prerequisites
1. [Install Rust](https://www.rust-lang.org/tools/install)
2. Create new virtual environment with [conda](https://www.anaconda.com/download/success) using the [requirements.txt](requirements.txt) file: `conda env create <modelardb_env> --file requirements.txt`

## Experiments

### 1. Extract Residuals
The experiment code to extract residuals and store them as Parquet files. [Detailed instructions](Experiments/Extract-Residuals/README.md).

### 2. Compute Value and Storage Distribution per Model Types
The experiment code to compute the shares of: 1) compressed values; and 2) storage sizes per model type. [Detailed instructions](Experiments/Model-Types-Used/README.md).


### Macaque on Elf
We implemented Macaque in the [Elf benchmark suite](https://github.com/Spatio-Temporal-Lab/elf).


## License
The code is licensed under version 2.0 of the Apache License.

This repository changes and is based on the following commit version of [ModelarDB](https://github.com/ModelarData/ModelarDB-RS/pull/287) which is released under version 2.0 of the Apache License.

