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


def match_system_name(name):
    if "vanilla" in name:
        return 'vanilla'
    elif 'Gorilla' in name or "Macaque" in name:
        return 'macaque'
    else:
        return "NA"
    

def main(files, dataset_name):
    output = [['error_bound', 'system_name', 'compressed_time_in_seconds', 'compression_size', 'decompression_time_in_seconds']]
    for file in files:  
        # Open and read the log file
        with open(file, "r") as f:
            log = f.read()

        # Extract compressed time
        compressed_time_match = re.search(r'Compressed in (\d+(?:\.\d+)?) seconds', log)
        compressed_time = float(compressed_time_match.group(1)) if compressed_time_match else None

        # Extract compression size (supporting decimal numbers)
        compression_size_match = re.search(r'Compression size: (\d+(?:\.\d+)?[KMGTP]?)', log)
        compression_size = compression_size_match.group(1) if compression_size_match else None

        # Extract decompression time
        decompression_time_match = re.search(r'Decompression time: (\d+(?:\.\d+)?) s', log)
        decompression_time = float(decompression_time_match.group(1)) if decompression_time_match else None
        
        system_type = match_system_name(file) 
        # print(file)
        error_bound = float(file.split('/')[-1].split("-")[0])
        
        output.append([error_bound, system_type, compressed_time, compression_size, decompression_time])
        # Print results
        # print(f"Compressed time: {compressed_time} s")
        # print(f"Compression size: {compression_size}")
        # print(f"Decompression time: {decompression_time} s")

    with open(f"{dataset_name}.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(output)
    print(f"{dataset_name}.csv was saved!")


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[2] not in ['powerlog', 'turbinelog']:
        print(f"Usage: {sys.argv[0]} path/to/logs dataset_name[powerlog or turbinelog]")
        sys.exit(1)
    dataset_name = sys.argv[2]
    files = get_log_files(sys.argv[1])
    files = [f for f in files if dataset_name in f]
    main(files, dataset_name)
