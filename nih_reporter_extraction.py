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


@task(log_prints=True, retries=0)
def write_gcs(path: str, overwrite_file_in_gcs: bool = False) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    if overwrite_file_in_gcs:
        "If overwrite is true, file presence will not be checked. Local file will be directly uploaded."
        gcs_block.upload_from_path(from_path=path, to_path=path, timeout=14400)
        print(f"File written at GCS bucket at: {path}")
        return

    # This block will execute when overwrite_file_in_gcs is set False.
    # First, it will check whether the file is already present in GCS block. This has to be similar folder organization from the root of local folder. For example, if the file is in 'A/B/C/d.parquet' local directory, then in GCS the file will be uploaded at 'A/B/C/D.parquet' in GCS.
    # Second, if the file is present, upload will be skipped.
    # Third, if the file is absent, the file will be uploaded.
    file_dir = "/".join(path.split("/")[:-1])
    fname = path.split("/")[-1]
    files_present_in_bucket = gcs_block.list_blobs(file_dir)
    files_present_in_bucket = [
        blob.name for blob in files_present_in_bucket
    ]  # Blob object ref: https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob
    print(
        f"Files aready present in bucket. Name of blobs already present are are\n: {files_present_in_bucket}"
    )

    # Checking if parquet file is already present in GCS bucket. If present, file upload will be skipped.
    if path in files_present_in_bucket and overwrite_file_in_gcs == False:
        print(
            f"File named {fname} is already present in GCS @ {files_present_in_bucket}"
        )
        return
    gcs_block.upload_from_path(
        from_path=path, to_path=path, timeout=14400
    )  # Had to add 14400 s (4 hrs) timeout since upload kept on getting timed out with my slow upload speed broadband.
    print(f"File write at GCS successful; {path}")
    return

@task(log_prints=True)
def delete_file(fpath: str) -> None:
    """This function delete the file as specified by file_path.
    Input:
        file_path: path of the file to be removed.
    Return:
        Returns None.
    """
    print(fpath)
    os.remove(fpath)
    print(f"File: {fpath} deleted.")
    return

@flow(log_prints=True)
def fetch_and_save_parquet(
    data_year, data_type, save_dir_prefix, replace_existing_file: bool = False, gcs_write: bool = False
) -> str:
    """Subflow to download and process single file, if not already present."""
    file_path = download_file(
        data_type=data_type,
        data_year=data_year,
        save_dir_prefix=save_dir_prefix,
        replace_existing_file=replace_existing_file,
    )

    # Checking whether parquet file already exists. If present, zip to parquet conversion is skipped. Saved parquet location is returned.
    parquet_folder = "/".join(file_path.split("/")[:-1]) + "/" + "extracted/"
    parquet_path = parquet_folder + file_path.split("/")[-1] + ".parquet"
    saved_path = parquet_path
    if os.path.exists(saved_path) and replace_existing_file == False:
        print(f"Parquet file already preset at {saved_path}")
        return saved_path
    saved_path = zip_to_parquet(file_path=file_path)
    print(f"Parquet file locally saved at: {saved_path}")
    if gcs_write:
        print(f"Writing file to GCS @ {saved_path}")
        write_gcs(path= saved_path, overwrite_file_in_gcs=False)
        print(f"Write to google cloud storage was successful @ {saved_path}")
    return saved_path


@flow(log_prints=True, description="Fetch data from NIH RePORTER to Data Lake.")
def nih_reporter_dw(
    data_years: list[int] = list(range(1985, 2022, 1)),
    data_types: list = ["projects", "abstracts", "publications", "linktables"],
    save_dir_prefix: str = "nih_reporter_data",
    replace_existing_file: bool = False,
    write_to_gcs: bool = False,
):
    """Main flow to download all NIH RePORTER files."""
    for data_type in data_types:
        for year in data_years:
            saved_file_path = fetch_and_save_parquet(
                data_year=year, data_type=data_type, save_dir_prefix=save_dir_prefix, gcs_write=True
            )
            print(f"Data fetch and write succesful for data type: {data_type} and year: {year}.\n Save location was {saved_file_path}")

            # Deleting raw file (compressed).
            compressed_file_path_prefix = '/'.join(saved_file_path.split('/')[:-2])
            compressed_file_name = saved_file_path.split('/')[-1].rsplit('.', 1)[0]
            compressed_file_path = compressed_file_path_prefix + '/' + compressed_file_name
            print(f'Deleting compressed file at {compressed_file_path}')
            delete_file(compressed_file_path)


if __name__ == "__main__":
    nih_reporter_dw(data_years=[1985, 1986, 1987, 1988], 
                    data_types=["projects"], 
                    write_to_gcs=True
                    )
