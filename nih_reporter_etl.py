import pandas as pd

# from prefect import flow, task
import os
import wget
import shutil

# @task(log_prints=True, retries=3)
def download_file(
    data_year: int,
    data_type: str = "projects",
    save_dir_pref: str = "data",
    replace_existing_file=False,
) -> str:

    data_type_options = ["projects", "abstracts", "publications", "linktables"]

    if data_type not in data_type_options:
        raise ValueError(
            f"data_type not within valid options. Valid options are {data_type_options}"
        )
    save_dir = f"{save_dir_pref}/{data_type}"
    url = f"https://reporter.nih.gov/exporter/{data_type}/download/{data_year}"
    file_path = f"{save_dir}/{data_year}.zip"

    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    # os.system(f'wget -O {file_path} {url}')

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


download_file(data_year=2020)


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
    print(f'File unpack successful for {file_path}.')
    return file_dir


def zip_to_parquet(file_path: str) -> str:
        '''This function takes file_path (of zip compressed dataframe) as input. Reads files as pandas dataframe and returns the dataframe.
        Input:
            file_path: path to zip compressed csv files.
        Output:
            String: Path to saved parquet file.
        '''
        try:
            df = pd.read_csv(file_path, compression='zip', low_memory=False, encoding ='latin-1')
            print('Parse engine was default.')
        except:
            df = pd.read_csv(file_path, compression='zip', encoding ='latin-1', sep='","',engine='python')
            column_headers = [x.replace('"', '') for x in df.columns]
            df.columns = column_headers
            print("Parse engine was python, with custom separator: [sep='","',engine='python']")
        print(f'Total number of rows read: {len(df)}')
        parquet_path = "/".join(file_path.split("/")[:-1]) + "/" + "extracted/" + file_path.split('/')[-1] + '.parquet'
        df.to_parquet(parquet_path)
        return parquet_path

# def convert_to_parquet(df : pd.DataFrame, save_dir : str) -> str:
#     df.to_parquet(save_dir)

data_type_dict = {
    "APPLICATION_ID": "int64",
    "ACTIVITY": str,
    "ADMINISTERING_IC": str,
    "APPLICATION_TYPE": "Int64",
    "ARRA_FUNDED": str,
    "AWARD_NOTICE_DATE": str,
    "BUDGET_START": str,
    "BUDGET_END": str,
    "CFDA_CODE": "Int64",
    "CORE_PROJECT_NUM": str,
    "ED_INST_TYPE": str,
    "FOA_NUMBER": str,
    "FULL_PROJECT_NUM": str,
    "FUNDING_ICs": str,
    "FUNDING_MECHANISM": str,
    "FY": "int64",
    "IC_NAME": str,
    "NIH_SPENDING_CATS": str,
    "ORG_CITY": str,
    "ORG_COUNTRY": str,
    "ORG_DEPT": str,
    "ORG_DISTRICT": "Int64",
    "ORG_DUNS": str,
    "ORG_FIPS": str,
    "ORG_IPF_CODE": "Int64",
    "ORG_NAME": str,
    "ORG_STATE": str,
    "ORG_ZIPCODE": str,
    "PHR": str,
    "PI_IDS": str,
    "PI_NAMEs": str,
    "PROGRAM_OFFICER_NAME": str,
    "PROJECT_START": str,
    "PROJECT_END": str,
    "PROJECT_TERMS": str,
    "PROJECT_TITLE": str,
    "SERIAL_NUMBER": "Int64",
    "STUDY_SECTION": str,
    "STUDY_SECTION_NAME": str,
    "SUBPROJECT_ID": "Int64",
    "SUFFIX": str,
    "SUPPORT_YEAR": "Int64",
    "DIRECT_COST_AMT": "Int64",
    "INDIRECT_COST_AMT": "Int64",
    "TOTAL_COST": "Int64",
    "TOTAL_COST_SUB_PROJECT": "Int64",
}

# df = pd.read_csv('data/projects/RePORTER_PRJ_C_FY2020.csv', encoding='latin-1', dtype= data_type_dict)
df = pd.read_csv(
    "data/projects/2020.zip",
    compression="zip",
    encoding="latin-1",
    dtype=data_type_dict,
)
print(df.shape)
