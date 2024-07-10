WITH stg_payroll_casa AS (
  SELECT
    rec_type,
    batch_id,
    TO_DATE(batch_date, 'ddMMyyyy') AS batch_date,
    serial_number,
    category_of_interface,
    currency,
    tran_type,
    tran_sub_type,
    TO_DATE(tran_date, 'ddMMyyyy') AS tran_date,
    TO_DATE(value_date, 'ddMMyyyy') AS value_date,
    initiating_sol,
    TO_DATE(gi_date, 'ddMMyyyy') AS gi_date,
    CAST(REPLACE(tran_amount, ',', '') AS double) AS tran_amount,
    act_no,
    tran_particular,
    part_tran_type,
    office_account_placeholder,
    bank_id,
    company_account,
    branch,
    company_code,
    handoff_name,
    existasof
  FROM {{ source('raw_rcbc', 'payroll_casa') }}
  WHERE existasof {{ var('incremental_predicates') }}
)


SELECT * 
FROM stg_payroll_casa