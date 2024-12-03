WITH src_model AS (
    SELECT * FROM tunacome.afdbt_model
)
SELECT
    model_name,
    model_version
FROM
    src_model