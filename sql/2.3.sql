WITH account_balance_previous_day AS (
    SELECT
        account_rk,
        effective_date,
        LAG(account_out_sum) OVER (PARTITION BY account_rk ORDER BY effective_date) AS prev_account_out_sum
    FROM
        rd.account_balance
),
corrected_account_balance AS (
    SELECT
        ab.account_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum,
        COALESCE(abp.prev_account_out_sum, 0) AS prev_account_out_sum,
        CASE
            WHEN ab.account_in_sum IS DISTINCT FROM COALESCE(abp.prev_account_out_sum, 0) THEN COALESCE(abp.prev_account_out_sum, 0)
            ELSE ab.account_in_sum
        END AS correct_account_in_sum
    FROM
        rd.account_balance ab
    LEFT JOIN
        account_balance_previous_day abp
    ON
        ab.account_rk = abp.account_rk
        AND ab.effective_date = abp.effective_date
)
SELECT
*
FROM
    corrected_account_balance;
