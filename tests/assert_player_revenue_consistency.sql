/*
    Data Quality Test: Revenue Consistency
    
    Purpose: Ensure revenue calculations are consistent across models
    Business Rule: Total spending in player_summary must match sum of purchases
    
    Test Logic:
    - Compare aggregated spending from daily_player_stats vs raw purchases
    - Flag any discrepancies > $0.01 (accounting for rounding)
    - Should return 0 rows if all revenue calculations are accurate
    
    Author: Ali Adibnia
    Created: 2025
*/

select 
    ps.player_id,
    ps.total_spent as player_summary_revenue,
    coalesce(sum(p.amount_usd), 0) as actual_purchase_total,
    abs(ps.total_spent - coalesce(sum(p.amount_usd), 0)) as revenue_difference

from {{ ref('daily_player_stats') }} dps
join (
    select 
        player_id,
        sum(total_spent) as total_spent
    from {{ ref('daily_player_stats') }}
    group by player_id
) ps on dps.player_id = ps.player_id
left join {{ ref('stg_purchases') }} p on ps.player_id = p.player_id
group by 1, 2
having abs(ps.total_spent - coalesce(sum(p.amount_usd), 0)) > 0.01

-- This test should return 0 rows if revenue calculations are consistent
