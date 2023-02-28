import wget
import os
import shutil, gzip
import pandas as pd
import lxml.etree as ET
from datetime import datetime

start_time = datetime.now()

def download_file(
    url: str,
    save_dir_prefix: str = "pubmed_data",
    replace_existing_file: bool = False,
) -> str:
    """Ths function downloads data file from from provided URL.
    See https://report.nih.gov/faqs for further details.
    Input:
        url: URL to be downloaded.
        save_dir_prefix: Prefix of the directory where data will be saved. By default, data will be saved in a folder named 'pubmed_data'. The folder will be created if not present.
        replace_existing_file: A boolean flag. Set False by default. If set True, existing file(s) will be discarded and will be replaced by downloaded files.
    Output:
        Outputs saved file path (or existing file path) as a string.
    """

    save_dir = f"{save_dir_prefix}/staging"  # Downloaded files will be saved in a subfolder named "staging".
    fname = url.split("/")[
        -1
    ]  # Fname inferred from the URL. Assumption: Text after the last backslash (/) in the URL represents filename.
    file_path = f"{save_dir}/{fname}"

    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)

    if os.path.exists(file_path) and not replace_existing_file:
        print(
            f"File already present in {file_path}. Skipping download. Set *replace_existing_file = True* if you want to download again and replace."
        )
        return file_path

    if os.path.exists(file_path) and replace_existing_file:
        print(f"Deleting pre-existing file at: {file_path}")
        os.remove(file_path)
    filename = wget.download(url=url, out=file_path)

    print(f"Downlaod of {url} was successful. Downloaded file name: {filename}")
    return file_path


def uncompress_file(file_path: str) -> str:
    """This function unpacks compressed file.
    Input:
        file_path: file_path as string.
    Output:
        extracted_file_path: Directory of extracted files, as string.
    """
    # import shutil
    extracted_folder_path = "/".join(file_path.split("/")[:-2]) + "/" + "extracted/"
    extracted_file_name = file_path.split("/")[-1].rsplit(".", 1)[0]
    extracted_file_path = extracted_folder_path + extracted_file_name

    if not os.path.isdir(extracted_folder_path):
        os.makedirs(extracted_folder_path)

    # shutil.unpack_archive(filename=file_path, extract_dir=extracted_file_path, format='gzip')
    with gzip.open(file_path, "r") as f_in, open(extracted_file_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    print(f"File unpack successful for {file_path}.")
    return extracted_file_path

def pmid_to_doi_linktable(xml_file_path : str) -> None:
    '''Reads pubmed xml file, grabs PMID:DOI pairs and returns a dataframe with PMIDs-DOIs. Rows with empty DOIs are dropped before returning dataframe.'''
    article_list = []
    for event, element in ET.iterparse(xml_file_path, tag="ArticleIdList", events=("end",)):
        article_id_pubmed = element.xpath('.//ArticleId[@IdType="pubmed"]')[0].text
        try: 
            # Assumption: All entries in PubMed has PMID, but some might not have DOI.
            article_id_doi = element.xpath('.//ArticleId[@IdType="doi"]')[0].text
        except:
            article_id_doi = None
        article_list.append((article_id_pubmed, article_id_doi))
    df = pd.DataFrame(article_list).dropna()
    df.columns = ['PMID', 'DOI']
    parquet_path_prefix = "/".join(xml_file_path.split('/')[:-2]) + '/' + 'data_lake/'
    parquet_fname = xml_file_path.split("/")[-1].rsplit(".", 1)[0] + '.parquet'
    parquet_file_path = parquet_path_prefix + parquet_fname
    if not os.path.isdir(parquet_path_prefix):
        os.makedirs(parquet_path_prefix)
    df.to_parquet(path=parquet_file_path)
    print(f'Processed data ({parquet_fname}) saved at {parquet_file_path}')
    return

def delete_file(file_path : str) -> None:
    '''This function delete the file as specified by file_path.
    Input:
        file_path: path of the file to be removed.
    Return:
        Returns none.
    '''
    os.remove(file_path)
    print(f'File: {file_path} deleted.')
    return


if __name__ == "__main__":
    downloaded_file_path = download_file(
        url="https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed23n0002.xml.gz"
    )
    uncompressed_file_path = uncompress_file(file_path=downloaded_file_path)
    print(f"Extracted file saved at {uncompressed_file_path}")
    pmid_to_doi_linktable(uncompressed_file_path)
    print(f'Removing raw files from the staging folder.')
    delete_file(uncompress_file)
    delete_file(download_file)

end_time = datetime.now()

print(f'Total time of code run: {end_time-start_time} s.')