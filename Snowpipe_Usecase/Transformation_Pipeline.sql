--Streams Creation
create or replace stream SNOWPIPE_DEMO.DWH_SCHEMA.CUSTOMER_STREAM on table STG_CUSTOMER;

create or replace stream SNOWPIPE_DEMO.DWH_SCHEMA.ORDER_ITEM_STREAM on table STG_ORDER_ITEM;

create or replace stream SNOWPIPE_DEMO.DWH_SCHEMA.ORDER_ITEM_STREAM on table STG_ORDER_ITEM;

create or replace stream SNOWPIPE_DEMO.DWH_SCHEMA.PAYMENT_STREAM on table STG_PAYMENT;

create or replace stream SNOWPIPE_DEMO.DWH_SCHEMA.PRODUCT_STREAM on table STG_PRODUCT;



--DAG Tasks Creation
create or replace task SNOWPIPE_DEMO.DWH_SCHEMA.CUSTOMER_TASK
	warehouse=SNOWPIPE_WH
	schedule='USING CRON  0 * * * * America/Los_Angeles'
	as BEGIN

    IF (SYSTEM$STREAM_HAS_DATA('CUSTOMER_STREAM'))
    THEN

            create OR REPLACE temporary table temp_cust as 
            select distinct cs.CUSTOMER_KEY, CS.FIRST_NAME, CS.LAST_NAME, CS.EMAIL,CS.SIGNUP_DATE
            from customer_stream cs left join dwh_customer dc
            on dc.customer_key = cs.customer_key
            where METADATA$ACTION = 'INSERT'
            and (Not(cs.email = dc.email) or dc.email is null);

            UPDATE DWH_CUSTOMER DC
            SET DC.UPDATED_DATE = CURRENT_DATE()
            WHERE DC.UPDATED_DATE IS NULL
            AND DC.CUSTOMER_KEY IN (SELECT TC.CUSTOMER_KEY
                                    FROM TEMP_CUST TC 
                                    WHERE TC.CUSTOMER_KEY = DC.CUSTOMER_KEY);
        
        
            MERGE  INTO dwh_customer DC
            USING ( SELECT 0 as id,TC.*  FROM TEMP_CUST TC) SC ON (SC.ID = DC.ID) 
            WHEN NOT  MATCHED  THEN  INSERT (CUSTOMER_KEY, FIRST_NAME, LAST_NAME, EMAIL, SIGNUP_DATE, CREATED_DATE) VALUES (SC.CUSTOMER_KEY, SC.FIRST_NAME, SC.LAST_NAME, SC.EMAIL, SC.SIGNUP_DATE, CURRENT_DATE); 
        
    END IF;

END;


create or replace task SNOWPIPE_DEMO.DWH_SCHEMA.PRODUCT_TASK
	warehouse=SNOWPIPE_WH
	after SNOWPIPE_DEMO.DWH_SCHEMA.CUSTOMER_TASK
	as begin
    IF (SYSTEM$STREAM_HAS_DATA('PRODUCT_STREAM'))
    THEN
        create OR REPLACE temporary table temp_prod as 
            select distinct ps.PRODUCT_KEY, ps.PRODUCT_NAME, ps.CATEGORY, ps.PRICE
            from product_stream ps left join dwh_product dp
            on dp.product_key = ps.product_key
            where METADATA$ACTION = 'INSERT'
            and (Not(ps.price = dp.price) or dp.price is null);
    
        UPDATE DWH_PRODUCT DC
        SET DC.UPDATED_DATE = CURRENT_DATE()
        WHERE DC.UPDATED_DATE IS NULL
        AND DC.PRODUCT_KEY IN (SELECT TC.PRODUCT_KEY
                                FROM temp_prod TC 
                                WHERE TC.PRODUCT_KEY = DC.PRODUCT_KEY);
    
    
        MERGE  INTO DWH_PRODUCT DC
        USING ( SELECT 0 AS ID, TC.*  FROM temp_prod TC) SC ON (SC.ID = DC.ID) 
        WHEN NOT  MATCHED  THEN  INSERT (PRODUCT_KEY, PRODUCT_NAME, CATEGORY, PRICE, CREATED_DATE) VALUES (SC.PRODUCT_KEY, SC.PRODUCT_NAME, SC.CATEGORY, SC.PRICE, CURRENT_DATE); 
        
    END IF;

end;


create or replace task SNOWPIPE_DEMO.DWH_SCHEMA.ORDER_TASK
	warehouse=SNOWPIPE_WH
	after SNOWPIPE_DEMO.DWH_SCHEMA.PRODUCT_TASK
	as begin
    IF (SYSTEM$STREAM_HAS_DATA('ORDER_STREAM'))
    THEN
        create OR REPLACE temporary table temp_ORDER as select * from ORDER_STREAM where METADATA$ACTION = 'INSERT';
    
    
        MERGE  INTO DWH_ORDER DC
        USING ( SELECT 0 AS ID, DC.ID AS DC_CUSTOMER_KEY, TC.ORDER_KEY,  TC.ORDER_DATE, TC.TOTAL_AMOUNT
                FROM temp_ORDER TC left join DWH_CUSTOMER DC
                ON TC.CUSTOMER_KEY = DC.CUSTOMER_KEY
                WHERE DC.UPDATED_DATE IS NULL) SC 
        ON (SC.ID = DC.ID) 
        WHEN NOT  MATCHED  THEN  INSERT (ORDER_KEY, CUSTOMER_KEY, ORDER_DATE, TOTAL_AMOUNT, CREATED_DATE) VALUES (SC.ORDER_KEY, SC.DC_CUSTOMER_KEY, SC.ORDER_DATE, SC.TOTAL_AMOUNT, CURRENT_DATE); 
        
    END IF;

end;


create or replace task SNOWPIPE_DEMO.DWH_SCHEMA.ORDER_ITEM_TASK
	warehouse=SNOWPIPE_WH
	after SNOWPIPE_DEMO.DWH_SCHEMA.ORDER_TASK
	as begin
    IF (SYSTEM$STREAM_HAS_DATA('ORDER_ITEM_STREAM'))
    THEN
        create OR REPLACE temporary table TEMP_ORDER_ITEM as select * from ORDER_ITEM_STREAM where METADATA$ACTION = 'INSERT';

    
        MERGE  INTO DWH_ORDER_ITEM DC
        USING ( SELECT 0 AS ID, DO.ID AS DO_ORDER_KEY, DP.ID AS DP_PRODUCT_KEY, TC.QUANTITY, TC.UNIT_PRICE,TC.order_item_key
                FROM TEMP_ORDER_ITEM TC LEFT JOIN DWH_ORDER DO
                    ON TC.ORDER_KEY = DO.ORDER_KEY
                  LEFT JOIN DWH_PRODUCT DP
                    ON DP.PRODUCT_KEY = TC.PRODUCT_KEY
                WHERE DP.UPDATED_DATE IS NULL) SC
        ON (SC.ID = DC.ID) 
        WHEN NOT  MATCHED  THEN  INSERT (ORDER_ITEM_KEY, ORDER_KEY, PRODUCT_KEY, QUANTITY, UNIT_PRICE, CREATED_DATE) VALUES (SC.ORDER_ITEM_KEY, SC.DO_ORDER_KEY, SC.DP_PRODUCT_KEY, SC.QUANTITY, SC.UNIT_PRICE, CURRENT_DATE); 
        
    END IF;

end;


create or replace task SNOWPIPE_DEMO.DWH_SCHEMA.PAYMENT_TASK
	warehouse=SNOWPIPE_WH
	after SNOWPIPE_DEMO.DWH_SCHEMA.ORDER_TASK
	as begin
    IF (SYSTEM$STREAM_HAS_DATA('PAYMENT_STREAM'))
    THEN
        create OR REPLACE temporary table TEMP_PAYMENT as select * from PAYMENT_STREAM where METADATA$ACTION = 'INSERT';
    
    
    
        MERGE  INTO DWH_PAYMENT DC
        USING ( SELECT 0 AS ID, DO.ID AS DO_ORDER_KEY, TC.PAYMENT_KEY, TC.PAYMENT_DATE,TC.AMOUNT, TC.METHOD
                FROM TEMP_PAYMENT TC LEFT JOIN DWH_ORDER DO
                ON TC.ORDER_KEY = DO.ORDER_KEY) SC
        ON (SC.ID = DC.ID) 
        WHEN NOT  MATCHED  THEN  INSERT (PAYMENT_KEY, ORDER_KEY, PAYMENT_DATE, AMOUNT, METHOD, CREATED_DATE) VALUES (SC.PAYMENT_KEY, SC.DO_ORDER_KEY, SC.PAYMENT_DATE, SC.AMOUNT, SC.METHOD, CURRENT_DATE); 
        
    END IF;

end;
