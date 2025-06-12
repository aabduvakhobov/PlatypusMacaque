"""
This script uses and changes the open source [script](https://github.com/ModelarData/Utilities/blob/main/ModelarDB-Analyze-Storage/main.py) from the commit [version](https://github.com/ModelarData/Utilities/commit/021f53a5d9bcf6d3c1ad9ec0ec135b1748b00304)
Authors: Søren Kejser Jensen and Christian Schmidt Godiksen.
"""
import os
import sys
import sqlite3
import tempfile
from dataclasses import dataclass
from collections import Counter

import pyarrow
from pyarrow import parquet
from pyarrow import Table
import pyarrow.compute as pc


# Must match IDs used by modelardb_compression.
MODEL_TYPE_ID_TO_NAME = ["PMC_Mean", "Swing", "Gorilla"]


@dataclass
class Configuration:
    data_folder: str
    model_table_name: str
    data_page_size: int = 16384
    row_group_size: int = 65536
    column_encoding: str = "PLAIN"
    compression: str = "ZSTD"
    use_dictionary: bool = False
    write_statistics: bool = False

    def model_table_path(self) -> str:
        return self.data_folder + os.sep + "tables" + os.sep + self.model_table_name


def list_and_process_files(configuration: Configuration, results: sqlite3.Connection):
    result_id = 1
    top = configuration.model_table_path()
    for dirpath, _dirnames, filenames in os.walk(top):
        for filename in filenames:
            if not filename.endswith(".parquet"):
                continue

            file_path = os.path.join(dirpath, filename)
            measure_file_and_its_columns(configuration, file_path, result_id, results)
            result_id += 1


def compute_model_size_with_python_size_in_bytes(configuration: Configuration, table: Table):
    result = dict()
    model_types =  table.column('model_type_id').to_pandas().unique()
    for model_type in model_types:
        if model_type == 2:
            # We need compute the size for Gorilla by also including all residuals from other models
            value_size = write_table(
                configuration, 
                table.filter(pc.field('model_type_id') == model_type).select(['min_value', 'max_value', 'values'])
                )
            residuals_size = write_table(
                configuration, 
                table.select(['residuals'])
                )
            result[model_type] = residuals_size + value_size
        else:
            result[model_type] = write_table(
                configuration, 
                table.filter(pc.field('model_type_id') == model_type).select(['min_value', 'max_value', 'values'])
                )
    return result


def measure_file_and_its_columns(
    configuration: Configuration, file_path: str, result_id, results: sqlite3.Connection
):
    table = parquet.read_table(file_path)

    field_column_str = file_path.split(os.sep)[-2]
    field_column = int(field_column_str[field_column_str.rfind("=") + 1 :])

    rust_size_in_bytes = os.path.getsize(file_path)
    python_size_in_bytes = write_table(configuration, table)

    model_types_used = Counter()
    # model_types_size_in_bytes = None
    python_size_in_bytes_per_column = Counter()
    for field in table.schema:
        column = table.column(field.name)
        if field.name == "model_type_id":
            for value in column:
                model_types_used[value.as_py()] += 1
        column_schema = pyarrow.schema(pyarrow.struct([field]))
        column_table = Table.from_arrays([column], schema=column_schema)
        python_size_in_bytes_per_column[field.name] = write_table(
            configuration, column_table
        )
        
    model_types_size_in_bytes = compute_model_size_with_python_size_in_bytes(configuration, table)
    _ = results.execute(
        f"INSERT INTO file VALUES({field_column}, {rust_size_in_bytes}, {python_size_in_bytes})"
    )
    # results.commit()
    
    for model_type_id, segment_count in model_types_used.items():
        _ = results.execute(
            f"INSERT INTO model_type_use VALUES({field_column}, {model_type_id}, {segment_count}, {model_types_size_in_bytes[model_type_id]})"
        )
        # results.commit()

    for column_index, (column_name, python_size_in_bytes) in enumerate(
        python_size_in_bytes_per_column.items()
    ):
        _ = results.execute(
            f"INSERT INTO file_column VALUES({field_column}, {column_index}, '{column_name}', {python_size_in_bytes})"
        )
    results.commit()


def write_table(configuration: Configuration, table: Table) -> int:
    with tempfile.NamedTemporaryFile() as temp_file_path:
        parquet.write_table(
            table,
            temp_file_path.name,
            data_page_size=configuration.data_page_size,
            row_group_size=configuration.row_group_size,
            column_encoding=configuration.column_encoding,
            compression=configuration.compression,
            use_dictionary=configuration.use_dictionary,
            write_statistics=configuration.write_statistics,
        )
        return os.path.getsize(temp_file_path.name)


def read_column_indices_column_names(
    data_folder: str, model_table_name: str
) -> dict[int, str]:
    model_table_field_columns = parquet.read_table(
        sys.argv[1] + "/metadata/model_table_field_columns",
        filters=[("table_name", "==", sys.argv[2])],
    )
    column_indices = model_table_field_columns.column("column_index")
    column_names = model_table_field_columns.column("column_name")

    column_indices_column_names = {}
    for index in range(0, model_table_field_columns.num_rows):
        column_index = column_indices[index].as_py()
        column_name = column_names[index].as_py()
        column_indices_column_names[column_index] = column_name
    return column_indices_column_names


def print_results(
    column_indices_column_names: dict[int, str], results: sqlite3.Connection
):
    field_columns = execute_and_return_value(
        "SELECT DISTINCT field_column FROM file ORDER BY field_column", results
    )
    for field_column in field_columns:
        model_types_used = execute_and_return_value(
            f"SELECT model_type_id, SUM(segment_count), SUM(python_size_in_bytes) FROM model_type_use WHERE field_column = {field_column} GROUP BY model_type_id ORDER BY model_type_id",
            results,
        )
        rust_size_in_bytes = execute_and_return_value(
            f"SELECT SUM(rust_size_in_bytes) FROM file WHERE field_column = {field_column}",
            results,
        )
        python_size_in_bytes = execute_and_return_value(
            f"SELECT SUM(python_size_in_bytes) FROM file WHERE field_column = {field_column}",
            results,
        )
        python_size_in_bytes_per_column = execute_and_return_value(
            f"SELECT column_name, SUM(python_size_in_bytes) FROM file_column WHERE field_column = {field_column} GROUP BY column_index ORDER BY column_index",
            results,
        )

        print_total_size_in_bytes(
            field_column,
            column_indices_column_names[field_column],
            model_types_used,
            rust_size_in_bytes,
            python_size_in_bytes,
            python_size_in_bytes_per_column,
        )

    model_types_used = execute_and_return_value(
        f"SELECT model_type_id, SUM(segment_count), SUM(python_size_in_bytes) FROM model_type_use GROUP BY model_type_id ORDER BY model_type_id",
        results,
    )
    rust_size_in_bytes = execute_and_return_value(
        f"SELECT SUM(rust_size_in_bytes) FROM file", results
    )
    python_size_in_bytes = execute_and_return_value(
        f"SELECT SUM(python_size_in_bytes) FROM file", results
    )
    python_size_in_bytes_per_column = execute_and_return_value(
        f"SELECT column_name, SUM(python_size_in_bytes) FROM file_column GROUP BY column_index ORDER BY column_index",
        results,
    )

    print_total_size_in_bytes(
        "All",
        "Summed",
        model_types_used,
        rust_size_in_bytes,
        python_size_in_bytes,
        python_size_in_bytes_per_column,
    )


def execute_and_return_value(query: str, results: sqlite3.Connection):
    cursor = results.execute(query)
    values = cursor.fetchall()
    cursor.close()

    if len(values) == 1 and len(values[0]) == 1:
        return values[0][0]
    elif len(values) > 1 and len(values[0]) == 1:
        return list(map(lambda value: value[0], values))
    elif len(values[0]) == 3:
        # used for model_type_use results and can be 
        return {tup[0]: {"segment_size" : tup[1], "bytes" : tup[2]}  for tup in values}
    else:
        return dict(values)


def print_total_size_in_bytes(
    field_column: int,
    field_name: str,
    model_types_used: list[int],
    rust_size_in_bytes: int,
    python_size_in_bytes: int,
    python_size_in_bytes_per_column: dict[str, int],
):
    print(f"Field Column: {field_column} - {field_name}")
    print("------------------------------------------")

    for model_type_id, count in model_types_used.items():
        model_type_name = MODEL_TYPE_ID_TO_NAME[model_type_id]
        print(f"- {model_type_name} {count['segment_size']:>15} Segments, {count['bytes']:>5} B")
    print("------------------------------------------")

    summed_size_in_bytes = 0
    for column, size in python_size_in_bytes_per_column.items():
        print(f"- {column:<25} {bytes_to_mib(size):>10} MiB")
        summed_size_in_bytes += size

    print("------------------------------------------")
    print(f"- Summed Size {bytes_to_mib(summed_size_in_bytes):>24} MiB")
    print(f"- Python Size {bytes_to_mib(python_size_in_bytes):>24} MiB")
    print(f"- Rust Size {bytes_to_mib(rust_size_in_bytes):>26} MiB")
    print()


def bytes_to_mib(size_in_bytes: int) -> float:
    return round(size_in_bytes / 1024 / 1024, 2)


def main():
    if len(sys.argv) != 3 and len(sys.argv) != 4:
        print(f"Usage: python3 {__file__} data_folder model_table_name [database_file]")
        # print(f"{len(sys.argv)}")
        return
    database_file = sys.argv[3] if len(sys.argv) == 4 else ":memory:"
    # All results are stored in SQLite to simplify aggregating them.
    results: sqlite3.Connection = sqlite3.connect(database_file)
    _ = results.execute(
        """CREATE TABLE file(field_column INTEGER, rust_size_in_bytes INTEGER, python_size_in_bytes INTEGER) STRICT"""
    )
    _ = results.execute(
        """CREATE TABLE model_type_use(field_column INTEGER, model_type_id INTEGER, segment_count INTEGER, python_size_in_bytes INTEGER) STRICT"""
    )
    _ = results.execute(
        """CREATE TABLE file_column(field_column INTEGER, column_index INTEGER, column_name TEXT, python_size_in_bytes INTEGER) STRICT"""
    )
    results.commit()

    configuration = Configuration(sys.argv[1], sys.argv[2])
    list_and_process_files(configuration, results)

    column_indices_column_names = read_column_indices_column_names(
        sys.argv[1], sys.argv[2]
    )
    print_results(column_indices_column_names, results)


if __name__ == "__main__":
    main()