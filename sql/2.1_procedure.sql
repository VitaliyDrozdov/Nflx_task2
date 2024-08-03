CREATE OR REPLACE PROCEDURE remove_duplicate_clients()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH duplicates AS (
        SELECT
            ctid,
            client_rk,
            effective_from_date,
            row_number() OVER (
                PARTITION BY client_rk, effective_from_date
                ORDER BY client_rk
            ) AS rnum
        FROM
            dm.client c
    )
    DELETE FROM dm.client
    WHERE ctid IN (
        SELECT
            ctid
        FROM
            duplicates
        WHERE
            rnum > 1
    );

    RAISE NOTICE 'Удаление дубликатов завершено.';
END;
$$;

call remove_duplicate_clients();
