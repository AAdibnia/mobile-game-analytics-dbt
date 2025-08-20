-- Test to ensure session data integrity and business logic
-- Checks for impossible session scenarios that could indicate data quality issues

with session_validation as (
    select 
        session_id,
        player_id,
        session_start,
        session_end,
        session_duration_minutes,
        actions_taken,
        
        -- Flag problematic sessions
        case when session_end <= session_start then 'Invalid end time'
             when session_duration_minutes > 480 then 'Session too long (>8hrs)'
             when session_duration_minutes <= 0 then 'Zero or negative duration'
             when actions_taken < 0 then 'Negative actions'
             when actions_taken > 1000 then 'Unrealistic action count'
             else null
        end as validation_issue
        
    from {{ ref('stg_sessions') }}
)

select 
    session_id,
    player_id,
    validation_issue,
    session_duration_minutes,
    actions_taken
from session_validation
where validation_issue is not null

-- This test should return 0 rows if all session data passes validation
