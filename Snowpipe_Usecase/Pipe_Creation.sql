use database snowpipe_demo;
use schema snowpipe_schema;


CREATE OR REPLACE FILE FORMAT my_csv_format 
  TYPE = 'CSV' 
  FIELD_DELIMITER = ',' 
  SKIP_HEADER = 1;
  
  
create or replace stage snowpipe_stage;


create or replace pipe SNOWPIPE_DEMO.SNOWPIPE_SCHEMA.CUSTOMER_PIPE auto_ingest=true as copy into stg_customer
FROM @SNOWPIPE_STAGE 
FILE_FORMAT = MY_CSV_FORMAT
PATTERN ='.*customer.*\.csv';


create or replace pipe SNOWPIPE_DEMO.SNOWPIPE_SCHEMA.ORDER_ITEM_PIPE auto_ingest=true as copy into stg_order_item
from @snowpipe_stage
FILE_FORMAT = my_csv_format
pattern = '.*order_item.*\.csv';


create or replace pipe SNOWPIPE_DEMO.SNOWPIPE_SCHEMA.ORDER_PIPE auto_ingest=true as copy into stg_order
from @snowpipe_stage
FILE_FORMAT = my_csv_format
pattern = '.*order.*\.csv';


create or replace pipe SNOWPIPE_DEMO.SNOWPIPE_SCHEMA.PAYMENT_PIPE auto_ingest=true as copy into stg_payment
from @snowpipe_stage
FILE_FORMAT = my_csv_format
pattern = '.*payment.*\.csv';


create or replace pipe SNOWPIPE_DEMO.SNOWPIPE_SCHEMA.PRODUCT_PIPE auto_ingest=true as copy into stg_product
from @snowpipe_stage
FILE_FORMAT = my_csv_format
pattern = '.*product.*\.csv';