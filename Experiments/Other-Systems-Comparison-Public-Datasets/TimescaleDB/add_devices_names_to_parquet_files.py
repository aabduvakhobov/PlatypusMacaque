import os
import glob
import sys

import numpy as np
import pyarrow.parquet as parquet
import pyarrow


def get_files(path):
    if os.path.isdir(path):
        parquet_files = glob.glob(path + os.sep + "*.parquet")
        parquet_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        parquet_files = [path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")
    return parquet_files


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} path/to/parquet_files")
        sys.exit(0)
    # "postgresql://abduvoris:postgres@127.0.0.1:5432/powerlog"
    path = sys.argv[1]
    
    files = get_files(path)
    for file in files:
        device_name = file.split('/')[-1].replace('.parquet', '')
        table = parquet.read_table(file)
        devices = np.full(table.num_rows, device_name, dtype=object) 
        table = table.append_column('device', pyarrow.array(devices))
        parquet.write_table(table, file, compression='snappy')
        print(f'Saved: {file}')
