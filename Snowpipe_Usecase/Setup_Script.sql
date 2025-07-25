Create database snowpipe_demo;
create schema snowpipe_schema;

create role snowpipe_role;

create warehouse snowpipe_wh;

grant usage on warehouse snowpipe_wh to role snowpipe_role;

grant role snowpipe_role to user chirag;

grant role snowpipe_role to role sysadmin;


grant create pipe on schema snowpipe_schema to role snowpipe_role;
grant create file format on schema snowpipe_schema to role snowpipe_role;

grant usage on database snowpipe_demo to role snowpipe_role;
grant usage on schema snowpipe_schema to role snowpipe_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SNOWPIPE_DEMO.SNOWPIPE_SCHEMA TO ROLE SNOWPIPE_ROLE;
GRANT INSERT ON FUTURE TABLES IN SCHEMA SNOWPIPE_DEMO.SNOWPIPE_SCHEMA TO ROLE SNOWPIPE_ROLE;

GRANT CREATE TABLE ON SCHEMA SNOWPIPE_SCHEMA TO ROLE SNOWPIPE_ROLE;

GRANT CREATE stage ON SCHEMA snowpipe_SCHEMA TO ROLE SNOWPIPE_ROLE;



CREATE SCHEMA DWH_SCHEMA;

GRANT CREATE TABLE ON SCHEMA DWH_SCHEMA TO ROLE SNOWPIPE_ROLE;
GRANT CREATE STREAM ON SCHEMA DWH_SCHEMA TO ROLE SNOWPIPE_ROLE;
GRANT CREATE TASK ON SCHEMA DWH_SCHEMA TO ROLE SNOWPIPE_ROLE;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE snowpipe_role;
