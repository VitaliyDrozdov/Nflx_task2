CREATE OR REPLACE PROCEDURE correct_account_balance()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH account_balance_previous_day AS (
        SELECT
            account_rk,
            effective_date,
            LAG(account_out_sum) OVER (PARTITION BY account_rk ORDER BY effective_date) AS prev_account_out_sum
        FROM
            rd.account_balance
    )
    UPDATE rd.account_balance ab
    SET account_in_sum = COALESCE(abp.prev_account_out_sum, 0)
    FROM account_balance_previous_day abp
    WHERE ab.account_rk = abp.account_rk
      AND ab.effective_date = abp.effective_date
      AND ab.account_in_sum IS DISTINCT FROM COALESCE(abp.prev_account_out_sum, 0);
END;
$$;


CALL correct_account_balance();
