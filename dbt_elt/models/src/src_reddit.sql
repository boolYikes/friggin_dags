WITH src_reddit AS (
    SELECT * FROM tunacome.afdbt_reddit
)
SELECT
    title,
    content,
    score,
    num_comments,
    created_utc,
    thumbnail_link,
    category,
    up_votes,
    up_ratio,
    subreddit,
    author,
    link,
    search_key,
    collected_on
FROM
    src_reddit