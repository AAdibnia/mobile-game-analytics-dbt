/*
    Player Profile Staging Model
    
    Purpose: Clean and standardize raw player registration data
    Source: {{ ref('players') }}
    
    Business Logic:
    - Standardize VIP status categories for consistent reporting
    - Create player level tiers for segmentation analysis
    - Calculate player tenure for lifecycle analysis
    
    Author: Ali Adibnia
    Created: 2025
*/

{{
  config(
    materialized='view',
    description='Cleaned player profile data with derived business metrics'
  )
}}

select
    -- Primary identifiers
    player_id,
    registration_date::date as registration_date,
    country,
    platform,
    current_level,
    vip_status,
    updated_at::timestamp as updated_at,
    
    -- Business tier classifications
    case 
        when vip_status = 'free' then 'Free'
        when vip_status = 'premium' then 'Premium' 
        when vip_status = 'vip' then 'VIP'
        else 'Unknown'
    end as player_tier,
    
    -- Level-based player segmentation
    case 
        when current_level <= 10 then 'Beginner'
        when current_level <= 30 then 'Intermediate'
        when current_level <= 50 then 'Advanced'
        else 'Expert'
    end as player_level_tier,
    
    -- Player lifecycle metrics
    date_diff('day', registration_date::date, current_date()) as days_since_registration

from {{ ref('players') }}
