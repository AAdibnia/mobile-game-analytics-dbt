/*
    Level Progression Staging Model
    
    Purpose: Clean and analyze level completion and progression data
    Source: {{ ref('levels') }}
    
    Business Logic:
    - Track level completion rates and performance metrics
    - Categorize attempts by completion speed and difficulty
    - Calculate performance ratios for progression analysis
    - Support funnel analysis and difficulty balancing
*/

{{
  config(
    materialized='view',
    description='Cleaned level progression data with performance metrics'
  )
}}

select
    -- Primary identifiers
    level_id,
    player_id,
    level_number,
    attempt_date::timestamp as attempt_date,
    completed,
    completion_time_seconds,
    score,
    
    -- Time-based derived fields
    attempt_date::date as attempt_date_only,
    
    -- Performance categorization
    case 
        when completed and completion_time_seconds <= 180 then 'Fast'
        when completed and completion_time_seconds <= 360 then 'Normal'
        when completed then 'Slow'
        else 'Failed'
    end as completion_category,
    
    -- Performance efficiency metrics
    case when completed and completion_time_seconds > 0
         then score / completion_time_seconds::float 
         else 0 end as score_per_second,
         
    -- Level difficulty classification
    case 
        when level_number <= 10 then 'Tutorial'
        when level_number <= 25 then 'Easy'
        when level_number <= 50 then 'Medium'
        else 'Hard'
    end as difficulty_tier

from {{ ref('levels') }}
