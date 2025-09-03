/*
    Player Profile Snapshot (SCD Type 2)
    
    Purpose: Track historical changes in player profiles over time
    Source: stg_players
    
    Business Logic:
    - Capture player level progression and VIP status changes
    - Enable historical analysis of player journey evolution
    - Support point-in-time reporting for business metrics
    
    SCD Strategy: Timestamp-based change detection
*/

{% snapshot player_profile_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='player_id',
    strategy='timestamp',
    updated_at='updated_at',
    description='Historical tracking of player profile changes'
  )
}}

select
    player_id,
    registration_date,
    country,
    platform,
    current_level,
    vip_status,
    player_tier,
    player_level_tier,
    days_since_registration,
    updated_at
from {{ ref('stg_players') }}

{% endsnapshot %}
