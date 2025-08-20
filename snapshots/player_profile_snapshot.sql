{% snapshot player_profile_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='player_id',
    strategy='timestamp',
    updated_at='updated_at'
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
