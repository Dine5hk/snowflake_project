CREATE WAREHOUSE IF NOT EXISTS Billing_wh
  COMMENT = 'Billing data warehouse for testing'
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

create database telecom_db;

create or replace schema billing_raw;
create or replace schema billing_staging;
create or replace schema billing_transformation;

show schemas;

use database telecom_db;
use schema billing_raw;

CREATE OR REPLACE FILE FORMAT billing_raw.CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1;

create or replace storage integration billing_int
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = "arn:aws:iam::905418117571:role/telecom"
storage_allowed_locations = ("s3://telecom-airtel/Billing/");

create or replace storage integration billing_int
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = "arn:aws:iam::905418117571:role/telecom"
storage_allowed_locations = ("s3://telecom-airtel/Billing/");

create or replace storage integration billing_int
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = "arn:aws:iam::905418117571:role/telecom"
storage_allowed_locations = ("s3://telecom-airtel/Billing/");

create or replace stage billing_stage
url = 's3://telecom-airtel/Billing/'
storage_integration = billing_int
file_format = csv_format;


desc stage billing_stage;

CREATE OR REPLACE TABLE billing_raw (
    bill_id        STRING,
    customer_id    STRING,
    phone_number   STRING,
    bill_amount    STRING,
    tax_amount     STRING,
    billing_date   DATE,
    payment_status STRING,

    --  SCD TYPE 2 COLUMNS
    created_on     timestamp,
    ended_on       timestamp,
    is_active      BOOLEAN
);

LIST @billing_stage;

COPY INTO billing_raw
FROM @billing_stage
FILE_FORMAT = (
  FORMAT_NAME = csv_format
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';



select * from billing_raw limit 10;



    
