CREATE OR REPLACE FUNCTION correct_account_balance()
RETURNS VOID AS $$
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
$$ LANGUAGE plpgsql;

-- С возвратом данным:
CREATE OR REPLACE FUNCTION correct_account_balance()
RETURNS TABLE (account_rk BIGINT, effective_date DATE, corrected_in_sum NUMERIC) AS $$
BEGIN
    RETURN QUERY
    WITH account_balance_previous_day AS (
        SELECT
            account_rk,
            effective_date,
            LAG(account_out_sum) OVER (PARTITION BY account_rk ORDER BY effective_date) AS prev_account_out_sum
        FROM
            rd.account_balance
    ),
    updated_rows AS (
        UPDATE rd.account_balance ab
        SET account_in_sum = COALESCE(abp.prev_account_out_sum, 0)
        FROM account_balance_previous_day abp
        WHERE ab.account_rk = abp.account_rk
          AND ab.effective_date = abp.effective_date
          AND ab.account_in_sum IS DISTINCT FROM COALESCE(abp.prev_account_out_sum, 0)
        RETURNING ab.account_rk, ab.effective_date, COALESCE(abp.prev_account_out_sum, 0) AS corrected_in_sum
    )
    SELECT account_rk, effective_date, corrected_in_sum FROM updated_rows;
END;
$$ LANGUAGE plpgsql;
