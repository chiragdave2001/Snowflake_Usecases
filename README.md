# Snowpipe Usecase

## Overview

This folder demonstrates an automated data ingestion and transformation pipeline in Snowflake, utilizing Snowpipe, streams, and a DAG of tasks. The flow efficiently loads and processes data from cloud storage into final Data Warehouse (DWH) tables.

## Flow Description

1. **Create a Stage**
   - Set up an external stage (e.g., an AWS S3 bucket) to hold the raw data files to be ingested.

2. **Create a Pipe**
   - Define a Snowpipe to automatically load data from files in the stage into staging tables in Snowflake.

3. **Create Streams on Staging Tables**
   - Implement streams on the staging tables to track new and changed data, enabling incremental and efficient data processing.

4. **Create a DAG of Tasks**
   - Design a Directed Acyclic Graph (DAG) of Snowflake tasks:
     - Tasks read the latest data from the streams.
     - Perform required transformations on this data.
     - Load the processed data into the final target DWH tables.

5. **End-to-End Automation**
   - This setup ensures that new files placed in the stage are automatically ingested, processed, transformed, and made available in your DWH tables with minimal latency.

## Files

- SQL scripts for creating the stage, pipe, streams, and tasks.
- Example transformation code.
- Configuration files supporting the workflow.

Refer to each script for specific implementation

