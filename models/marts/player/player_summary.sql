/*
    Player Summary Model
    
    Purpose: Complete player lifecycle and behavior summary for analytics
    Sources: stg_players, stg_sessions, stg_purchases, stg_levels
    
    Business Logic:
    - Aggregate all player activity across sessions, purchases, and levels
    - Calculate key player metrics (LTV, engagement, progression)
    - Classify players by monetization status and engagement level
    
    Author: Ali Adibnia
    Created: 2025
*/

{{
  config(
    materialized='table',
    description='Complete player lifecycle and behavior summary'
  )
}}

with player_stats as (
  select
    p.player_id,
    p.registration_date,
    p.country,
    p.platform,
    p.player_tier,
    p.current_level,
    
    -- Session metrics
    count(distinct s.session_id) as total_sessions,
    sum(s.session_duration_minutes) as total_session_minutes,
    avg(s.session_duration_minutes) as avg_session_minutes,
    
    -- Purchase metrics  
    count(distinct pur.purchase_id) as total_purchases,
    coalesce(sum(pur.amount_usd), 0) as total_spent,
    
    -- Level progression
    count(distinct l.level_id) as levels_attempted,
    sum(case when l.completed then 1 else 0 end) as levels_completed
    
  from {{ ref('stg_players') }} p
  left join {{ ref('stg_sessions') }} s on p.player_id = s.player_id
  left join {{ ref('stg_purchases') }} pur on p.player_id = pur.player_id  
  left join {{ ref('stg_levels') }} l on p.player_id = l.player_id
  group by 1,2,3,4,5,6
)

select 
  *,
  -- Derived metrics
  case when total_purchases > 0 then total_spent / total_purchases else 0 end as avg_purchase_amount,
  case when levels_attempted > 0 then levels_completed::float / levels_attempted else 0 end as level_completion_rate,
  case when total_spent > 0 then 'Paying' else 'Free' end as monetization_status
from player_stats
