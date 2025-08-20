{{
  config(
    materialized='view'
  )
}}

select
    player_id,
    registration_date::date as registration_date,
    country,
    platform,
    current_level,
    vip_status,
    updated_at::timestamp as updated_at,
    
    -- Derived fields
    case 
        when vip_status = 'free' then 'Free'
        when vip_status = 'premium' then 'Premium' 
        when vip_status = 'vip' then 'VIP'
        else 'Unknown'
    end as player_tier,
    
    case 
        when current_level <= 10 then 'Beginner'
        when current_level <= 30 then 'Intermediate'
        when current_level <= 50 then 'Advanced'
        else 'Expert'
    end as player_level_tier,
    
    -- Days since registration  
    date_diff('day', registration_date::date, current_date()) as days_since_registration

from {{ ref('players') }}
