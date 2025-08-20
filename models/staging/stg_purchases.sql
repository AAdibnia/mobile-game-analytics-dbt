/*
    Purchase Transaction Staging Model
    
    Purpose: Clean and categorize in-app purchase data
    Source: {{ ref('purchases') }}
    
    Business Logic:
    - Standardize purchase amounts and categorize by size
    - Classify purchases by motivation and price tier
    - Add business-relevant groupings for monetization analysis
    - Support ARPU/ARPPU and conversion rate calculations
    
    Author: Ali Adibnia
    Created: 2025
*/

{{
  config(
    materialized='view',
    description='Cleaned purchase data with monetization categorizations'
  )
}}

select
    -- Primary identifiers
    purchase_id,
    player_id,
    purchase_date::timestamp as purchase_date,
    item_type,
    item_name,
    amount_usd,
    currency_type,
    
    -- Time-based derived fields
    purchase_date::date as purchase_date_only,
    
    -- Purchase size categorization
    case 
        when amount_usd < 5 then 'Micro'
        when amount_usd < 20 then 'Small'
        when amount_usd < 50 then 'Medium'
        else 'Large'
    end as purchase_size_category,
    
    -- Purchase motivation analysis
    case 
        when item_type = 'currency' then 'Monetization'
        when item_type = 'weapon' then 'Progression'
        when item_type = 'booster' then 'Convenience'
        else 'Other'
    end as purchase_motivation,
    
    -- Price tier classification
    case 
        when amount_usd <= 2.99 then 'Low'
        when amount_usd <= 9.99 then 'Mid'
        else 'High'
    end as price_tier

from {{ ref('purchases') }}
