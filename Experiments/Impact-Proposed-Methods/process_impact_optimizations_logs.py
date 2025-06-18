import re
import sys
import os
import glob
import csv


def get_log_files(path):
    if os.path.isdir(path):
        log_files = glob.glob(path + os.sep + "*compression_results*.log")
        log_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        log_files = [path]
    else:
        raise ValueError("log_file_or_folder is not a file or a folder")
    return log_files    


def match_system_name(name):
    if "vanilla" in name:
        return 'vanilla'
    elif 'Value-Rewrite' in name:
        return 'value-rewrite'
    elif "Bit-Rewrite" in name:
        return 'bit-rewrite'
    else:
        return "macaque"


def get_dataset_name(name):
    return name

def main(files,):
    output = [['dataset_name', 'error_bound', 'system_name', 'compressed_time_in_seconds', 'compression_size',]]
    for file in files:  
        # Open and read the log file
        with open(file, "r") as f:
            log = f.read()
            
        # Regex patterns
        time_match = re.search(r'Compressed in (\d+)\s+seconds', log)
        size_match = re.search(r'Compression size:\s+([\d.]+[KMG]?)\s+(.+)', log)

        if time_match and size_match:
            compression_time = int(time_match.group(1))
            compression_size = size_match.group(1)
            output_path = size_match.group(2).strip()
        else:
            raise ValueError("Log file doesn't match expected format.")
        # # Extract compressed time
        file_name = file.split('/')[-1]
        dataset_name = file_name.split("-")[1]
        system_type = match_system_name(file) 
        # print(file)
        error_bound = float(file_name.split("-")[0])
        
        output.append([dataset_name, error_bound, system_type, compression_time, compression_size, ])
    
    with open(f"processed_logs.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(output)
    print(f"processed_logs.csv was saved!")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} path/to/logs")
        sys.exit(1)
    # dataset_name = sys.argv[2]
    files = get_log_files(sys.argv[1])
    # files = [f for f in files if dataset_name in f]
    main(files,)