WITH src_sentiment AS (
    SELECT * FROM tunacome.afdbt_sentiment
)
SELECT
    title,
    content,
    sentiment,
    created_utc,
    inference_no,
    model_variant,
    inferred_on
FROM
    src_sentiment