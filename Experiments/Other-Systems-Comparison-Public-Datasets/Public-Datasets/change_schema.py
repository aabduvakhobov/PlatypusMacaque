import sys
import glob
import os

import pyarrow
import pyarrow.parquet as parquet


def get_files(path):
    if os.path.isdir(path):
        parquet_files = glob.glob(path + os.sep + "*.parquet")
        parquet_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        parquet_files = [path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")
    return parquet_files


def comply_schema_with_mdb(table):
    columns = [] 
    for field in table.schema:
        print(field.name)
        safe_name = field.name.replace(" ", "_")
        if field.type in [
            pyarrow.timestamp("s"),
            pyarrow.timestamp("ms"),
            pyarrow.timestamp("us"),
            pyarrow.timestamp("ns"),
            pyarrow.timestamp("us", tz='UTC'),
            pyarrow.timestamp("ns", tz='UTC')
        ]:
            columns.append(pyarrow.field(safe_name, pyarrow.timestamp("ms")))
        else:
            columns.append(pyarrow.field(safe_name, pyarrow.float32()))
    return pyarrow.Table.from_arrays(table.columns, schema = pyarrow.schema(columns))


def main(path):
    # read the path to data
    files = get_files(path,)
    print(f"Finished extraction and got: {len(files)} files" )
    
    for file in files[16:]:
        table = parquet.read_table(file)
        table = comply_schema_with_mdb(table)
        parquet.write_table(table, file, compression='snappy')
        print(f"Saved: {file}")


if __name__ == "__main__":
    # exit if parameters are not as expected
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} /path/to/files ")
        sys.exit(1)       
    main(sys.argv[1],)