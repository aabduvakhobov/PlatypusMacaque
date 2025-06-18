"""This script is for preprocessing segment logs from ModelarDb and store them in separate file

Returns:
    _type_: _description_
"""
import sys

import pandas as pd

SELECT_ONE_QUERY = "SELECT {} FROM {};"

def get_safe_col_name(col_name):
    return col_name.lower().replace(" ", "_").replace(".", "")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} path/to/logs dataset_name error_bound")
        sys.exit(1)

    dataset_name=sys.argv[1]
    original_data_path=sys.argv[2]

    error_bound=float(sys.argv[3])
    save_path=sys.argv[4]