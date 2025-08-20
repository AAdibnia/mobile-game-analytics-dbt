{{
  config(
    materialized='view'
  )
}}

select
    session_id,
    player_id,
    session_start::timestamp as session_start,
    session_end::timestamp as session_end,
    actions_taken,
    levels_attempted,
    
    -- Derived fields
    date_diff('minute', session_start::timestamp, session_end::timestamp) as session_duration_minutes,
    session_start::date as session_date,
    
    case 
        when date_diff('minute', session_start::timestamp, session_end::timestamp) <= 5 then 'Short'
        when date_diff('minute', session_start::timestamp, session_end::timestamp) <= 30 then 'Medium'
        else 'Long'
    end as session_length_category,
    
    -- Engagement metrics
    case when actions_taken > 0 
         then actions_taken / date_diff('minute', session_start::timestamp, session_end::timestamp)::float 
         else 0 end as actions_per_minute

from {{ ref('sessions') }}
where session_end > session_start
