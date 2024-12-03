-- HAVE A TOTAL OF FOUR 'RAW' TABLES
-- 1. commodities: FACT
-- 2. reddit posts: FACT
-- 3. sentiments: FACT
-- 4. model: DIM

WITH src_commodities AS (
    SELECT * FROM tunacome.afdbt_commodities
)
SELECT
    date,
    name,
    open_value,
    high_value,
    low_value,
    close_value,
    volume
FROM
    src_commodities