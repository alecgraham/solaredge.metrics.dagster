# SolarEdge Metrics DAGster

This demo project explores DAGster functionality with a pipeline to collect data from the SolarEdge API, store it in ADLS, and load to the /data directory for use in a Chart.js chart.

## Pipeline

![DAGster Graph](https://github.com/alecgraham/solaredge.metrics.dagster/blob/main/img/dagster_lineage.PNG)

The pipeline follows 4 main steps.

1. **Save raw api data as a json file.**  The pipeline uploads data for each date between 2023-07-01 and T-1 as a distinct .json file in the raw/incoming directory in ADLS.
2. **Split raw data into multiple datasets.**  The pipeline processes any files in the raw/incoming directory by reading each metric into a polars dataframe and uploading a .parquet file for each dataframe into the relevant data/ directory in ADLS.
3. **Create Net Consumption dataset.**  The pipeline joins production and consumption datasets to create net consumption dataset and uploads it as a .parquet file to ADLS.
4. **Download data for Chart.js**  The pipeline downloads the Net Consumption dataset and syncs it to /data directory in github to use with Chart.js


## Chart
The SolarEdge app shows production and consumption by day, billing cycle, or calendar year.  Duke Energy net metering resets annually on 7/1.  The chart is intended to allow comparison of production and consumption for a full net metering cycle.