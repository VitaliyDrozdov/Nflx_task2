with
    duplicates as (
        select
            ctid,
            client_rk,
            effective_from_date,
            row_number() over (
                partition by
                    client_rk,
                    effective_from_date
                order by
                    client_rk
            ) as rnum
        from
            dm.client c
    )
DELETE from dm.client
where
    ctid in (
        select
            ctid
        from
            duplicates
        where
            rnum > 1
    );

-- Для проверки:
-- SELECT
--     ctid,
--     client_rk,
--     effective_from_date,
--     rnum
-- FROM
--     (
--         SELECT
--             ctid,
--             client_rk,
--             effective_from_date,
--             row_number() OVER (
--                 PARTITION BY
--                     client_rk,
--                     effective_from_date
--                 ORDER BY
--                     client_rk
--             ) as rnum
--         FROM
--             dm.client c
--     ) subquery
-- WHERE
--     rnum > 1;
