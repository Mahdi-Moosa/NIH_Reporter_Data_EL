# Objective
Extract and load NIH-funded research data to cloud datalake.

# Description
This repository is hosts the EL pipeline.

# Py-scripts in the folder:

* nih_reporter_extraction.py: Pipeline to extract, transform and load NIH reporter data to local/cloud data lake.
* nih_reporter_extraction_prefect.py: Prefect orchestration of the pipeline.
* combine_to_single_parquet.py: Combine all .parquet files (~200 kB each) into single .parquet file (~450 MB).
* pubmed_pmid_to_doi.py: Get pmid to doi data from pubmed.
* pubmed_pmid_to_doi_prefect.py: Prefect orchestration of pubmed_pmid_to_doi.py.

# Steps
* Get year x data from NIH reporter (zip file).
* unzip file, read as pandas df, save as parquet file.
* Upload parquet file to Google Cloud Storage.

# Orchestration tool

Prefect: https://docs.prefect.io/

# Notes on prefect parallel run (Prefect 2)

* Parallel flow run:
    * Run parallel as deployment; link: https://discourse.prefect.io/t/how-can-i-run-multiple-subflows-or-child-flows-in-parallel/96/12
    * Create async function that will call multiple async subflows: https://discourse.prefect.io/t/how-can-i-run-multiple-subflows-or-child-flows-in-parallel/96/10
* Parallel task run:
    * Dask task runner: https://github.com/tekumara/prefect-demo/blob/main/flows/dask_flow.py (Point to note: Task runner works with only tasks, not with flows.)

