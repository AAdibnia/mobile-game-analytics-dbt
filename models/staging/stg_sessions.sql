/*
    Session Data Staging Model
    
    Purpose: Clean and enhance raw session tracking data
    Source: {{ ref('sessions') }}
    
    Business Logic:
    - Calculate session duration and engagement metrics
    - Categorize sessions by length and activity level
    - Filter out invalid sessions (end before start)
    - Add derived engagement and time-based metrics
    
    Author: Ali Adibnia
    Created: 2025
*/

{{
  config(
    materialized='view',
    description='Cleaned session data with engagement and duration metrics'
  )
}}

select
    -- Primary identifiers
    session_id,
    player_id,
    session_start::timestamp as session_start,
    session_end::timestamp as session_end,
    actions_taken,
    levels_attempted,
    
    -- Time-based derived fields
    date_diff('minute', session_start::timestamp, session_end::timestamp) as session_duration_minutes,
    session_start::date as session_date,
    
    -- Session categorization
    case 
        when date_diff('minute', session_start::timestamp, session_end::timestamp) <= 5 then 'Short'
        when date_diff('minute', session_start::timestamp, session_end::timestamp) <= 30 then 'Medium'
        else 'Long'
    end as session_length_category,
    
    -- Engagement metrics
    case when actions_taken > 0 and date_diff('minute', session_start::timestamp, session_end::timestamp) > 0
         then actions_taken / date_diff('minute', session_start::timestamp, session_end::timestamp)::float 
         else 0 end as actions_per_minute

from {{ ref('sessions') }}
where session_end > session_start
