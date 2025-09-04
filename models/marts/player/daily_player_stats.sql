/*
    Daily Player Statistics (Incremental Model)
    
    Purpose: Efficiently aggregate daily player activity metrics
    Sources: stg_sessions, stg_purchases
    
    Business Logic:
    - Combine session and purchase data by player and date
    - Use incremental strategy for performance on large datasets
    - Track daily engagement and monetization patterns
*/

{{
  config(
    materialized='incremental',
    unique_key=['player_id', 'stats_date'],
    on_schema_change='fail',
    description='Daily aggregated player statistics optimized for performance'
  )
}}

with daily_sessions as (
  select 
    player_id,
    session_date,
    count(*) as sessions_count,
    sum(session_duration_minutes) as total_session_minutes,
    sum(actions_taken) as total_actions
  from {{ ref('stg_sessions') }}
  {% if is_incremental() %}
    where session_date > (select max(stats_date) from {{ this }})
  {% endif %}
  group by player_id, session_date
),

daily_purchases as (
  select 
    player_id,
    purchase_date_only as purchase_date,
    count(*) as purchases_count,
    sum(amount_usd) as total_spent
  from {{ ref('stg_purchases') }}
  {% if is_incremental() %}
    where purchase_date_only > (select max(stats_date) from {{ this }})
  {% endif %}
  group by player_id, purchase_date_only
)

select
  coalesce(s.player_id, p.player_id) as player_id,
  coalesce(s.session_date, p.purchase_date) as stats_date,
  coalesce(s.sessions_count, 0) as sessions_count,
  coalesce(s.total_session_minutes, 0) as total_session_minutes,
  coalesce(s.total_actions, 0) as total_actions,
  coalesce(p.purchases_count, 0) as purchases_count,
  coalesce(p.total_spent, 0) as total_spent
from daily_sessions s
full outer join daily_purchases p 
  on s.player_id = p.player_id 
  and s.session_date = p.purchase_date
