import wget
import os
import shutil, gzip
import pandas as pd
import lxml.etree as ET
from datetime import datetime
from lxml import html
import requests

start_time = datetime.now()


def download_file(
    url: str,
    save_dir_prefix: str = "pubmed_data",
    replace_existing_file: bool = False,
) -> str:
    """Ths function downloads data file from from provided URL.
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


def pmid_to_doi_linktable(xml_file_path: str) -> None:
    """Reads pubmed xml file, grabs PMID:DOI pairs and returns a dataframe with PMIDs-DOIs. Rows with empty DOIs are dropped before returning dataframe."""
    article_list = []
    for event, element in ET.iterparse(
        xml_file_path, tag="ArticleIdList", events=("end",)
    ):
        article_id_pubmed = element.xpath('.//ArticleId[@IdType="pubmed"]')[0].text
        try:
            # Assumption: All entries in PubMed has PMID, but some might not have DOI.
            article_id_doi = element.xpath('.//ArticleId[@IdType="doi"]')[0].text
        except:
            article_id_doi = None
        article_list.append((article_id_pubmed, article_id_doi))
        # Clearing element to save memory; Ref: https://pranavk.me/python/parsing-xml-efficiently-with-python/; https://stackoverflow.com/a/7171543
        # print('Clearing {e}'.format(e=ET.tostring(element_or_tree=element)))
        element.clear()
        for ancestor in element.xpath('ancestor-or-self::*'):
            # print('Checking ancestor: {a}'.format(a=ancestor.tag))
            while ancestor.getprevious() is not None:
                # print(
                #     'Deleting {p}'.format(p=(ancestor.getparent()[0]).tag))
                del ancestor.getparent()[0]
        # while element.getprevious() is not None:
        #     del element.getparent()[0]
    df = pd.DataFrame(article_list).dropna()
    df.columns = ["PMID", "DOI"]
    parquet_path_prefix = "/".join(xml_file_path.split("/")[:-2]) + "/" + "data_lake/"
    parquet_fname = xml_file_path.split("/")[-1].rsplit(".", 1)[0] + ".parquet"
    parquet_file_path = parquet_path_prefix + parquet_fname
    if not os.path.isdir(parquet_path_prefix):
        os.makedirs(parquet_path_prefix)
    df.to_parquet(path=parquet_file_path)
    print(f"Processed data ({parquet_fname}) saved at {parquet_file_path}")
    return


def delete_file(fpath: str) -> None:
    """This function delete the file as specified by file_path.
    Input:
        file_path: path of the file to be removed.
    Return:
        Returns none.
    """
    print(fpath)
    os.remove(fpath)
    print(f"File: {fpath} deleted.")
    return


def el_single_link(url) -> None:
    """Extraction and load function for single URL."""
    downloaded_file_path = download_file(url=url)
    uncompressed_file_path = uncompress_file(file_path=downloaded_file_path)
    print(f"Extracted file saved at {uncompressed_file_path}")
    pmid_to_doi_linktable(uncompressed_file_path)
    print(f"Removing raw files from the staging folder.")
    delete_file(uncompressed_file_path)
    delete_file(downloaded_file_path)
    return


def get_xml_gz_links(baseline_url):
    pubmed_ftp_page = requests.get(baseline_url)
    html_parse = html.fromstring(pubmed_ftp_page.text)
    all_links = list(html_parse.iterlinks())
    xml_gz_links = [c for (a, b, c, d) in all_links if c.endswith(".xml.gz")]
    return xml_gz_links


def data_lake_presence_check(file_name: str) -> bool:
    """For a provided .xml.gz file name, this function will look for corresponding parquet file. If present, True will be returned. Otherwise False."""
    parquet_prefix = "pubmed_data/data_lake/"
    parquet_path = parquet_prefix + file_name.strip(".xml.gz") + ".parquet"
    if os.path.exists(parquet_path):
        return True
    return False


def main_function(baseline_url: str = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/", presence_check : bool = True):
    xml_gz_links = get_xml_gz_links(baseline_url=baseline_url)
    n=250
    for lnk in xml_gz_links[n:n+100]:
        if presence_check and data_lake_presence_check(lnk):
            print(
                f"Corresponding parquet file already present in the data lake for: {lnk}."
            )
            continue
        dl_url = baseline_url + lnk
        el_single_link(url=dl_url)
    return


if __name__ == "__main__":
    main_function()

end_time = datetime.now()

print(f"Total time of code run: {end_time-start_time} s.")