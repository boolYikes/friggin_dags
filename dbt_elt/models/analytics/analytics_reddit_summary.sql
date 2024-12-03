{{
    config(
        materialized = 'incremental',
        on_schema_change='fail'
    )
}}
with
r as (select * from {{ ref('src_reddit') }})
select 
    to_char(r.created_utc, 'YYYY-MM-DD') as created_utc, 
    r.search_key, 
    sum(r.up_votes) as votes, 
    sum(r.num_comments) as comments, 
    count(*) as cnt,
    avg(s.sentiment) as sentiment
from
    r
join
    {{ ref('src_sentiment') }} s
on 
    s.title = r.title and s.content = r.content and s.created_utc = r.created_utc
{% if is_incremental() %}
    AND r.created_utc > (select max(r.created_utc) from {{ this }}) -- looks like a syntax for new data?
{% endif %}
group by to_char(r.created_utc, 'YYYY-MM-DD'), r.search_key
order by r.created_utc asc, r.search_key asc;