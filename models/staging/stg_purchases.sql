{{
  config(
    materialized='view'
  )
}}

select
    purchase_id,
    player_id,
    purchase_date::timestamp as purchase_date,
    item_type,
    item_name,
    amount_usd,
    currency_type,
    
    -- Derived fields
    purchase_date::date as purchase_date_only,
    
    case 
        when amount_usd < 5 then 'Micro'
        when amount_usd < 20 then 'Small'
        when amount_usd < 50 then 'Medium'
        else 'Large'
    end as purchase_size_category,
    
    case 
        when item_type = 'currency' then 'Monetization'
        when item_type = 'weapon' then 'Progression'
        when item_type = 'booster' then 'Convenience'
        else 'Other'
    end as purchase_motivation,
    
    -- Price tiers for analysis
    case 
        when amount_usd <= 2.99 then 'Low'
        when amount_usd <= 9.99 then 'Mid'
        else 'High'
    end as price_tier

from {{ ref('purchases') }}
