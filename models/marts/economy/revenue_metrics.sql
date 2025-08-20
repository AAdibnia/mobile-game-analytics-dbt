{{
  config(
    materialized='table'
  )
}}

with purchase_stats as (
  select
    purchase_date_only,
    item_type,
    purchase_size_category,
    count(*) as purchase_count,
    sum(amount_usd) as total_revenue,
    count(distinct player_id) as unique_buyers,
    avg(amount_usd) as avg_purchase_amount
  from {{ ref('stg_purchases') }}
  group by 1,2,3
),

daily_totals as (
  select 
    purchase_date_only,
    sum(total_revenue) as daily_revenue,
    sum(unique_buyers) as daily_buyers,
    sum(purchase_count) as daily_purchases
  from purchase_stats
  group by 1
),

player_base as (
  select 
    count(distinct player_id) as total_players,
    count(distinct case when total_spent > 0 then player_id end) as paying_players
  from {{ ref('player_summary') }}
)

select 
  dt.purchase_date_only,
  dt.daily_revenue,
  dt.daily_buyers, 
  dt.daily_purchases,
  dt.daily_revenue / pb.total_players as arpu,
  case when pb.paying_players > 0 
       then dt.daily_revenue / pb.paying_players 
       else 0 end as arppu,
  dt.daily_buyers::float / pb.total_players as conversion_rate
from daily_totals dt
cross join player_base pb
order by dt.purchase_date_only
