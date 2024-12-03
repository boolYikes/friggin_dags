{{
    config(
        materialized = 'incremental',
        on_schema_change='fail'
    )
}}
with
r as (select to_date(to_char(created_utc, 'YYYY-MM-DD'), 'YYYY-MM-DD') as created_utc, search_key, up_votes, num_comments, title, content from {{ ref('src_reddit') }}),
s as (select to_date(to_char(created_utc, 'YYYY-MM-DD'), 'YYYY-MM-DD') as created_utc, sentiment, content, title from {{ ref('src_sentiment') }})
select 
    r.created_utc, 
    r.search_key, 
    sum(r.up_votes) as up_votes, 
    sum(r.num_comments) as comments, 
    count(*) as cases_count,
    avg(s.sentiment) as sentiment
from
    r
join
    s
on 
    s.title = r.title and s.content = r.content and s.created_utc = r.created_utc
where r.created_utc is not null
{% if is_incremental() %}
    and r.created_utc > (select max(r.created_utc) from {{ this }}) -- looks like a syntax for new data?
{% endif %}
group by r.created_utc, r.search_key
order by r.created_utc asc, r.search_key asc