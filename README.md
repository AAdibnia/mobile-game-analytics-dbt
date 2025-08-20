# Mobile Game Analytics - dbt Project

A dbt project analyzing player behavior and game economy for a mobile RPG game.

## Project Overview

This project demonstrates key gaming analytics patterns using dbt:
- Player lifecycle and retention analysis
- Game economy and monetization metrics  
- Session behavior and engagement tracking
- Level progression analytics

## Data Sources

1. **players** - Player registration and profile data
2. **sessions** - Game session logs with duration and actions
3. **purchases** - In-app purchase transactions
4. **levels** - Level completion events and progression

## Key Models

### Staging
- Clean and standardize raw data from 4 sources

### Marts
- **Player Analytics**: Daily active users, retention cohorts, player lifetime value
- **Economy Analytics**: Revenue metrics, purchase funnels, ARPU/ARPPU

### Advanced Features
- **Incremental Model**: Daily player stats for performance optimization
- **Snapshot**: Player profile changes tracking (SCD Type 2)

## Getting Started

1. Install dbt with DuckDB adapter: `pip install dbt-duckdb`
2. Run the project: `dbt run`
3. Generate docs: `dbt docs generate && dbt docs serve`

## Business Metrics

- **DAU/MAU**: Daily and monthly active users
- **Retention**: Day 1, 7, 30 retention rates  
- **ARPU/ARPPU**: Average revenue per user/paying user
- **LTV**: Player lifetime value by acquisition cohort
- **Progression**: Level completion rates and progression funnels
