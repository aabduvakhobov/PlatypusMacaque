# Public Datasets
Overall 4 public datasets were used:
- NEON_wind_speed
- REDD
- Wind dataset
- BLUED

General idea is to use datasets with smaller SI and larger number of rows.

## NEON_wind_speed
__SI__: 2minutes
</br>__Row Count__: ~5M
</br>__Total Size in Parquet__: 251MB
</br>__Description__: [link](https://doi.org/10.48443/s9ya-zc81)
</br>__Processing script__: [link](preprocess_neon_dataset.py)

## REDD
__SI__: ~8ms
</br>__Row Count__: ~5M
</br>__Total Size in Parquet__: 636MB
</br>__Description__: [link](https://citeseerx.ist.psu.edu/document?repid=rep1&type=pdf&doi=d85a51e2978f4563ee74bf9a09d3219e03799819)
</br>__Processing script__: [link](preprocess_redd_dataset.py)

## Wind dataset
__SI__: 2minutes
</br>__Row Count__: 
</br>__Total Size in Parquet__: 16MB
</br>__Description__: Multivariate, 10 fields, data from wind turbines, [link](https://vbn.aau.dk/ws/portalfiles/portal/753239456/paper-102.pdf).
</br>__Processing script__: Already processed


## BLUED
__SI__: 83us (microseconds)
</br>__Row Count__: ~5M
</br>__Total Size in Parquet__: 636MB
</br>__Description__: [link](http://www.niculescu-mizil.org/KDD2012/forms/workshop/SustKDD12/doc/SustKDD12_4.pdf)
</br>__Processing script__: [link](preprocess_blue_dataset.py)