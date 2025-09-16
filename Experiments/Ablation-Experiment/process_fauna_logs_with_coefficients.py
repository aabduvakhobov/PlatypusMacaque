import re
import sys
import os
import glob
import csv


def get_log_files(path):
    if os.path.isdir(path):
        log_files = glob.glob(path + os.sep + "*compression_results.log")
        log_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        log_files = [path]
    else:
        raise ValueError("log_file_or_folder is not a file or a folder")
    return log_files  


def main(files, dataset_name):
    rows = []
    for file in files:        
        # Example: 4-1-turbinelog-compression_results.log
        match = file.split("/")[-1].split("-")
        compression_name = 'Fauna'
        coefficient = int(match[0])    # e.g., "pmc_and_swing"
        number = float(match[1])            # e.g., 1
        dataset = match[2]               # e.g., "powerlog"
        
        
        # Open and read the log file
        with open(file, "r") as f:
            log = f.read()

        # Extract compressed time
        compressed_time_match = re.search(r'Compressed in (\d+(?:\.\d+)?) seconds', log)
        compressed_time = float(compressed_time_match.group(1)) if compressed_time_match else None

        # Extract compression size (supporting decimal numbers)
        compression_size_match = re.search(r'Compression size: (\d+(?:\.\d+)?[KMGTP]?)', log)
        compression_size = compression_size_match.group(1) if compression_size_match else None
        
        rows.append([compression_name, number, dataset, coefficient, compression_size, compressed_time])
    output_csv = dataset_name + "_fauna_coefs.csv"
    # Write to CSV
    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["compression_name", "number", "dataset", 'division_coefficient', 'compression_size', 'compressed_time'])
        writer.writerows(rows)

    print(f"Saved parsed results to {output_csv}")

    return 0

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} path/to/logs dataset_name[powerlog or turbinelog]")
        sys.exit(1)
    files = get_log_files(sys.argv[1])
    dataset_name = sys.argv[2]
    files = [f for f in files if dataset_name in f]
    main(files, dataset_name)
