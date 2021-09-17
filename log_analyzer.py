from os import error
from pathlib import Path

log_dir = '/Users/imaneski/airflow/logs/marketvol'
file_list = Path(log_dir).rglob('*.log')


def analyze_file(file):
    error_list = []
    with open(file, 'r') as f:
        for line in f:
            if 'ERROR' in line:
                error_list.append(line)
    
    return len(error_list), error_list

for file in file_list:
    count, errors = analyze_file(file)
    print(count, errors)