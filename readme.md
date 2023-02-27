# Objective
Create a database of twitter handles for all NIH funded PI's.

# Description
This repository is hosts the pipeline.

# Steps
* Get year x data from NIH reporter (zip file).
* unzip file, read as pandas df, save as parquet file.
* Repartition parquet file, if required.