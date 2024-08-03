WITH current_day_balance AS (
    SELECT
        account_rk,
        effective_date,
        account_in_sum,
        LAG(effective_date) OVER (PARTITION BY account_rk ORDER BY effective_date) AS prev_effective_date
    FROM
        rd.account_balance
),
previous_day_balance AS (
    SELECT
        ab.account_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum,
        cdb.account_in_sum AS current_day_in_sum,
        cdb.effective_date AS next_effective_date,
        CASE
            WHEN cdb.account_in_sum IS DISTINCT FROM ab.account_out_sum THEN cdb.account_in_sum
            ELSE ab.account_out_sum
        END AS correct_account_out_sum
    FROM
        rd.account_balance ab
    LEFT JOIN
        current_day_balance cdb
    ON
        ab.account_rk = cdb.account_rk
        AND ab.effective_date = cdb.prev_effective_date
)
SELECT
    account_rk,
    effective_date,
    account_in_sum,
    account_out_sum,
    correct_account_out_sum
FROM
    previous_day_balance;
