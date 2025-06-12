"""This script merges datasets created from model distribution and writes one CSV file

"""
import os
import pandas as pd
import sys

def get_files(path):
    ff = []
    for dir, _, files in os.walk(path):
        for file in files:
            ff.append(dir + "/" + file)
    return ff

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} path/to/model_distribution_csv_files")
        sys.exit(0)
    
    input_path = sys.argv[1]
    files = get_files(input_path)
    
    model_distribution = pd.DataFrame()
    for file in files:
        if 'model_distribution_result_' not in file: continue
        df = pd.read_csv(file)
        model_distribution = pd.concat([model_distribution, df])
    
    model_distribution = model_distribution.reset_index(drop=True)
    model_distribution.to_csv('model_distribution_result.csv')
    print(f"Success: model_distribution_result.csv was saved for df with shape: {model_distribution.shape}")
