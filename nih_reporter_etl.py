import pandas as pd
from prefect import flow, task
import os

# @task(log_prints=True, retries=3)
def download_file(data_year: int, data_type: str = "projects", save_dir_pref: str= 'data') -> None:
    save_dir = f'{save_dir_pref}/{data_type}'
    url = f"https://reporter.nih.gov/exporter/{data_type}/download/{data_year}"
    file_path = f"{save_dir}/{data_year}.zip"
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    os.system(f'wget -O {file_path} {url}')

download_file(data_year= 2020)