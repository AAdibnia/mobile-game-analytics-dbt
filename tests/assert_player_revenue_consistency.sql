-- Test to ensure revenue calculations are consistent between models
-- This test checks that total revenue in player_summary matches purchases data

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
