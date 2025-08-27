import re
import sys
import os
import glob
import csv
import pandas as pd


def get_log_files(path, target):
    if os.path.isdir(path) and target == 'compression_size':
        log_files = glob.glob(path + os.sep + "*compression_results.log")
        log_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isdir(path) and target == 'memory_use':
        log_files = glob.glob(path + os.sep + "*mem_usage.log")
        log_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        log_files = [path]
    else:
        raise ValueError("log_file_or_folder is not a file or a folder")
    return log_files  


def process_compression_size(files):
    rows = []
    for file in files:        
        # Example: pmc_and_swing_only-1-powerlog-compression_results.log
        match = file.split("-")
        
        batch_size = int(match[0].split('/')[-1]) * 1024 # e.g., "pmc_and_swing"
        error_bound = float(match[1])            # e.g., 1
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
        
        rows.append([dataset, batch_size, error_bound, compression_size, compressed_time])
    output_csv = "compression_size_output.csv"
    # Write to CSV
    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["dataset", "batch_size", "error_bound", 'compression_size', 'compressed_time'])
        writer.writerows(rows)

    print(f"Saved parsed results to {output_csv}")

    return 0


def process_memory_use(files):    
    for input_file in files:
        match = input_file.split('/')[-1].split("-")
        dataset = match[0]
        error_bound = float(match[1])
        batch_size = int(match[2]) * 1024
        
        output_file = f"{dataset}_{error_bound}_{batch_size}_mem_usage.csv"      
        rows = []

        with open(input_file, "r", encoding="utf-8", errors="ignore") as f, \
            open(output_file, "w", newline="", encoding="utf-8") as g:
            w = csv.writer(g)
            w.writerow(["Timestamp", "PID", "RSS(KB)", "VSZ(KB)", "%MEM", "CMD"])

            for line in f:
                line = line.strip()
                if not line:
                    continue
                # skip header-ish lines (e.g., "Timestamp   PID   ...", or malformed "imestamp ...")
                if ("PID" in line and "RSS" in line and "VSZ" in line) or line.lower().startswith("timestamp") or line.lower().startswith("imestamp"):
                    continue

                # Split into at most 7 fields: date, time, pid, rss, vsz, pmem, cmd(rest)
                parts = line.split(None, 6)
                if len(parts) < 7:
                    # not enough fields, skip or warn
                    # print("Skipping unparsable line:", line)
                    continue

                date, time, pid, rss, vsz, pmem, cmd = parts
                timestamp = f"{date} {time}"
                w.writerow([timestamp, pid, rss, vsz, pmem, cmd])

def merge_mem_usage_logs(files):
    df_main = pd.DataFrame()
    for f in files:
        f_name = f.split('/')[-1].split('_')
        datasetname = f_name[0]
        error_bound = float(f_name[1])
        batch_size = int(f_name[2])
        df = pd.read_csv(f)
        df['error_bound'] = error_bound
        df['dataset_name'] = datasetname
        df['batch_size'] = batch_size
        df_main = pd.concat([df_main, df])
    df_main.to_csv('./merged_mem_usage.csv')


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[2] not in ['compression_size', 'memory_use']:
        print(f"Usage: {sys.argv[0]} path/to/logs target[compression_size or memory_use]")
        sys.exit(1)
    target = sys.argv[2]
    files = get_log_files(sys.argv[1], target)
    # files = [f for f in files if dataset_name in f]
    if target == 'compression_size':
        process_compression_size(files)
    elif target== 'memory_use':
        process_memory_use(files)
    else:
        print("Not implemented")