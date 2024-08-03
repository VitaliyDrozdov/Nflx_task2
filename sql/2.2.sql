SELECT
    *
FROM
    dm.loan_holiday_info
WHERE
    agreement_rk IS NULL
    and account_rk IS NULL
    and client_rk IS null
    and department_rk IS null
    and product_rk IS NULL
    and product_name IS NULL
    and deal_type_cd IS NULL
    AND deal_start_date IS null
    and deal_name IS NULL
    AND deal_number IS NULL
    and deal_sum IS NULL
    and loan_holiday_finish_date IS null
    and loan_holiday_fact_finish_date IS NULL
