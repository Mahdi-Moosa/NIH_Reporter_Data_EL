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