# Snowpipe Usecase

## Overview

This folder demonstrates an automated data ingestion and transformation pipeline in Snowflake, utilizing Snowpipe, streams, and a DAG of tasks. The flow efficiently loads and processes data from cloud storage into final Data Warehouse (DWH) tables.

## Flow Description

1. **Create a Stage**
   - Set up an external stage (e.g., an AWS S3 bucket) to hold the raw data files to be ingested. Upload files in the stage.
<img width="961" height="467" alt="image" src="https://github.com/user-attachments/assets/d63af1e7-94c2-4bb2-9823-5aaae4cb7e49" />

2. **Create a Pipe**
   - Define a Snowpipe to automatically load data from files in the stage into staging tables in Snowflake.
<img width="948" height="363" alt="image" src="https://github.com/user-attachments/assets/7fa77bfa-d842-46ec-b58c-5d9521b8ba6f" />

3. **Create Streams on Staging Tables**
   - Implement streams on the staging tables to track new and changed data, enabling incremental and efficient data processing.
<img width="945" height="355" alt="image" src="https://github.com/user-attachments/assets/e81f6f34-f1ba-4e0a-941e-cf6435205307" />

4. **Create a DAG of Tasks**
   - Design a Directed Acyclic Graph (DAG) of Snowflake tasks:
     - Tasks read the latest data from the streams.
     - Perform required transformations on this data.
     - Load the processed data into the final target DWH tables.
<img width="731" height="405" alt="image" src="https://github.com/user-attachments/assets/2de7503e-36b6-4f6e-bbca-cc9831de0d96" />

5. **End-to-End Automation**
   - This setup ensures that new files placed in the stage are automatically ingested, processed, transformed, and made available in your DWH tables with minimal latency.

## Files

- SQL scripts for creating the stage, pipe, streams, and tasks.
- Example transformation code.
- Configuration files supporting the workflow.

Refer to each script for specific implementation

