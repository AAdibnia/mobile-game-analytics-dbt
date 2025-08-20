{{
  config(
    materialized='table'
  )
}}

with first_sessions as (
  select 
    player_id,
    min(session_date) as first_session_date
  from {{ ref('stg_sessions') }}
  group by player_id
),

player_cohorts as (
  select 
    p.player_id,
    p.registration_date,
    fs.first_session_date,
    date_trunc('week', p.registration_date) as cohort_week
  from {{ ref('stg_players') }} p
  left join first_sessions fs on p.player_id = fs.player_id
),

retention_data as (
  select 
    pc.cohort_week,
    count(distinct pc.player_id) as cohort_size,
    count(distinct case when s.session_date = pc.first_session_date then pc.player_id end) as day_0_users,
    count(distinct case when s.session_date = pc.first_session_date + interval '1 day' then pc.player_id end) as day_1_users,
    count(distinct case when s.session_date = pc.first_session_date + interval '7 days' then pc.player_id end) as day_7_users,
    count(distinct case when s.session_date = pc.first_session_date + interval '30 days' then pc.player_id end) as day_30_users
  from player_cohorts pc
  left join {{ ref('stg_sessions') }} s on pc.player_id = s.player_id
  group by pc.cohort_week
)

select 
  cohort_week,
  cohort_size,
  day_0_users,
  day_1_users,
  day_7_users, 
  day_30_users,
  case when cohort_size > 0 then day_1_users::float / cohort_size else 0 end as day_1_retention,
  case when cohort_size > 0 then day_7_users::float / cohort_size else 0 end as day_7_retention,
  case when cohort_size > 0 then day_30_users::float / cohort_size else 0 end as day_30_retention
from retention_data
order by cohort_week
