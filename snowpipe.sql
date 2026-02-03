
CREATE OR REPLACE PIPE billing_pipe
AUTO_INGEST = TRUE
AS
COPY INTO billing_raw
FROM @billing_stage
FILE_FORMAT = (FORMAT_NAME = csv_format
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
ON_ERROR = 'CONTINUE';


DESC PIPE billing_pipe;

SHOW PIPES;

SELECT *
FROM TABLE(
  INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'BILLING_RAW',
    START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
  )
);


select * from billing_raw limit 10;

select * from billing_staging.billing_stg limit 50;