import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
import wget
import shutil

@task(log_prints=True, retries=3)
def download_file(
    data_year: int,
    data_type: str = "projects",
    save_dir_prefix: str = "data",
    replace_existing_file: bool = False,
) -> str:
    """Ths function downloads data file from NIH RePORTER as specified by data_year and data_type to directory specified by save_dir_prefix.
    See https://report.nih.gov/faqs for further details.
    Input:
        data_year: Year of data to be downloaded.
        data_type: Data type to be downloaded. Acceptable data types are: "projects", "abstracts", "publications", "linktables".
        save_dir_prefix: Prefix of the directory where data will be saved. By default, data will be saved in a folder named 'data'. The folder will be created if not present.
        replace_existing_file: A boolean flag. Set False by default. If set True, existing file(s) will be discarded and will be replaced by downloaded files.
    Output:
        Outputs saved file path (or existing file path) as a string.
    """
    # import wget
    data_type_options = ["projects", "abstracts", "publications", "linktables"]

    if data_type not in data_type_options:
        raise ValueError(
            f"data_type not within valid options. Valid options are {data_type_options}"
        )
    save_dir = f"{save_dir_prefix}/{data_type}"
    url = f"https://reporter.nih.gov/exporter/{data_type}/download/{data_year}"
    file_path = f"{save_dir}/{data_year}.zip"

    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)

    if os.path.exists(file_path) and not replace_existing_file:
        print(f"File already present in {file_path}. Skipping download.")
        return file_path

    if os.path.exists(file_path) and replace_existing_file:
        print(f"Deleting pre-existing file at: {file_path}")
        os.remove(file_path)
    filename = wget.download(url=url, out=file_path)

    print(
        f"Downlaod of {data_type}-{data_year} was successful. Downloaded file name: {filename}"
    )
    return file_path

@task(log_prints=True)
def unzip_file(file_path: str) -> str:
    """This function unpacks compressed file.
    Input:
        file_path: file_path as string.
    Output:
        file_dir: Directory of extracted files, as string.
    """
    # import shutil
    file_dir = "/".join(file_path.split("/")[:-1]) + "/" + "extracted/"
    shutil.unpack_archive(filename=file_path, extract_dir=file_dir)
    print(f"File unpack successful for {file_path}.")
    return file_dir

@task(log_prints=True)
def zip_to_parquet(file_path: str) -> str:
    """This function takes file_path (of zip compressed dataframe) as input. Reads files as pandas dataframe and returns the dataframe.
    Input:
        file_path: path to zip compressed csv files.
    Output:
        String: Path to saved parquet file.
    """
    try:
        df = pd.read_csv(
            file_path, compression="zip", low_memory=False, encoding="latin-1"
        )
        print("Parse engine was default.")
    except:
        df = pd.read_csv(
            file_path, compression="zip", encoding="latin-1", sep='","', engine="python"
        )
        column_headers = [x.replace('"', "") for x in df.columns]
        df.columns = column_headers
        print(
            "Parse engine was python, with custom separator: [sep='",
            "',engine='python']",
        )
    print(f"Total number of rows read: {len(df)}")

    parquet_folder = "/".join(file_path.split("/")[:-1]) + "/" + "extracted/"
    parquet_path = parquet_folder + file_path.split("/")[-1] + ".parquet"
    if not os.path.isdir(parquet_folder):
        print(f"{parquet_folder} was absent. Creating the folder/directory.")
        os.makedirs(parquet_folder)

    # Write data to parquet file.
    df.to_parquet(parquet_path)
    return parquet_path

@task(log_prints=True, retries=3)
def write_gcs(path: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow(log_prints=True)
def fetch_and_save_parquet(
    data_year, data_type, save_dir_prefix, replace_existing_file: bool = False
) -> str:
    '''Subflow to download and process single file.'''
    file_path = download_file(
        data_type=data_type,
        data_year=data_year,
        save_dir_prefix=save_dir_prefix,
        replace_existing_file=replace_existing_file,
    )
    saved_path = zip_to_parquet(file_path=file_path)
    print(f"Parquet file saved at: {saved_path}")
    return saved_path

@flow(log_prints=True, description="Fetch data from NIH RePORTER to Data Lake.")
def nih_reporter_dw(
    data_years: list[int] = list(range(1985, 2022, 1)),
    data_types: list = ["projects", "abstracts", "publications", "linktables"],
    save_dir_prefix: str = "data",
    replace_existing_file: bool = False,
    write_to_gcs : bool = False
):
    '''Main flow to download all NIH RePORTER files.'''
    for data_type in data_types:
        for year in data_years:
            saved_file_path = fetch_and_save_parquet(
                data_year=year, data_type=data_type, save_dir_prefix=save_dir_prefix
            )
            if write_to_gcs:
                write_gcs(saved_file_path)

if __name__ == "__main__":
    nih_reporter_dw(write_to_gcs=True)