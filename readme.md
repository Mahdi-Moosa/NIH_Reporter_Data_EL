# Objective
Extract and load NIH-funded research data to cloud datalake.

# Description
This repository is hosts the EL pipeline.

# Steps
* Get year x data from NIH reporter (zip file).
* unzip file, read as pandas df, save as parquet file.
* Upload parquet file to Google Cloud Storage.

# Orchestration tool

Prefect: https://docs.prefect.io/

# Notes on prefect parallel run (Prefect 2)

* Run parallel as deployment; link: https://discourse.prefect.io/t/how-can-i-run-multiple-subflows-or-child-flows-in-parallel/96/12
* Create async function that will call multiple async subflows: https://discourse.prefect.io/t/how-can-i-run-multiple-subflows-or-child-flows-in-parallel/96/10
* Dask task runner: https://github.com/tekumara/prefect-demo/blob/main/flows/dask_flow.py (Point to note: Task runner works with only tasks, not with flows.)

