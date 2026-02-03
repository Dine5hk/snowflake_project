
--- Raw Layer

CREATE OR REPLACE STREAM billing_raw_stream
ON TABLE billing_raw.billing_raw
APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK billing_staging
WAREHOUSE = Billing_wh
WHEN SYSTEM$STREAM_HAS_DATA('billing_raw_stream')
AS
INSERT INTO billing_staging.billing_stg
SELECT
    bill_id,
    customer_id,
    phone_number,
    TRY_TO_NUMBER(bill_amount),
    TRY_TO_NUMBER(tax_amount),
    billing_date,
    UPPER(payment_status),
    created_on AS load_ts
FROM billing_raw_stream
WHERE bill_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND phone_number IS NOT NULL
  AND bill_amount IS NOT NULL;

ALTER TASK billing_staging RESUME;



--- staging layer
CREATE OR REPLACE STREAM billing_stg_stream
ON TABLE billing_staging.billing_stg;




--- Raw Layer

CREATE OR REPLACE STREAM billing_raw_stream
ON TABLE billing_raw.billing_raw
APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK billing_staging
WAREHOUSE = Billing_wh
WHEN SYSTEM$STREAM_HAS_DATA('billing_raw_stream')
AS
INSERT INTO billing_staging.billing_stg
SELECT
    bill_id,
    customer_id,
    phone_number,
    TRY_TO_NUMBER(bill_amount),
    TRY_TO_NUMBER(tax_amount),
    billing_date,
    UPPER(payment_status),
    created_on AS load_ts
FROM billing_raw_stream
WHERE bill_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND phone_number IS NOT NULL
  AND bill_amount IS NOT NULL;

ALTER TASK billing_staging RESUME;





