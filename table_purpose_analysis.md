# NWSL Database Table Purpose & Analytical Use Case Analysis

## Executive Summary

Based on analysis of the PostgreSQL database structure and data patterns, I've identified the distinct purposes and analytical value of the key tables in your NWSL analytics database.

## Table Purpose Definitions

### 1. **match_team** Table (3,070 rows)
**PURPOSE**: Team-level match performance aggregates

This table captures **team-level statistics for each match**, with exactly 2 rows per match (home and away teams). It serves as the primary source for:

- **Team performance metrics**: Goals, possession %, passing accuracy, defensive actions
- **Match outcomes**: Win/Draw/Loss results from team perspective
- **Team tactical analysis**: Possession-based play, defensive solidity, attacking efficiency
- **Historical team trends**: Season-by-season team evolution

**Key Characteristics**:
- One row per team per match (2 rows per match_id)
- Links to team_season via UUID foreign key
- Contains aggregated team statistics (not player-level detail)
- Data completeness varies by year (possession data starts 2019, xG largely missing)

### 2. **match_player_summary** Table (43,572 rows)
**PURPOSE**: Individual player performance records

This table captures **player-level performance data for each match**, with ~28 rows per match (players from both teams). It serves as the primary source for:

- **Player performance tracking**: Goals, assists, xG, passing, defensive actions
- **Playing time analysis**: Minutes played, substitution patterns
- **Player development**: Career progression, form analysis
- **Position-specific metrics**: Different stats relevant for different positions
- **Team composition**: Who played, in what positions, for how long

**Key Characteristics**:
- One row per player per match appearance
- Links to player master data and team_season
- Contains detailed individual statistics
- Includes both starters and substitutes

### 3. **match** Table (1,563 rows)
**PURPOSE**: Core match metadata and relationships

This table serves as the **central fact table** that defines:
- When matches occurred
- Which teams played (home/away)
- Match type and context
- Final scores and xG totals

## Analytical Use Cases

### Team Performance Analysis
**Primary Table**: `match_team`
**Supporting Tables**: `team_season`, `match`

**Questions Answered**:
- What is a team's home vs away performance?
- How has team possession % correlated with results?
- Which teams are overperforming/underperforming xG?
- What are team defensive vs offensive strengths?
- How do teams perform in different match types (regular/playoff)?

### Player Performance Analysis
**Primary Table**: `match_player_summary`
**Supporting Tables**: `player`, `match`

**Questions Answered**:
- Who are the top scorers/assisters?
- Which players have the best xG conversion rates?
- How do players perform in different positions?
- What is a player's contribution over a season?
- Which players are most efficient passers/defenders?

### Match Outcome Prediction
**Primary Tables**: Both `match_team` and `match_player_summary`

**Questions Answered**:
- Based on team form, what's the likely outcome?
- How do specific player availabilities affect predictions?
- What tactical matchups favor certain outcomes?
- How do venue and historical H2H affect results?

### Historical Trends
**All Tables Combined**

**Questions Answered**:
- How has the league evolved tactically (possession, pressing)?
- Are games becoming higher/lower scoring?
- How has player usage changed (minutes, positions)?
- What is the impact of rule changes or new teams?

### Comparative Analysis
**Both Tables with Different Perspectives**

**Questions Answered**:
- Team performance vs sum of player performances
- Expected vs actual outcomes at team and player level
- Cross-team player comparisons
- League-wide benchmarking

## How Tables Work Together

```
match (core facts)
  ├── match_team (team aggregates)
  │     └── Provides team-level view of each match
  └── match_player_summary (player details)
        └── Provides player-level granularity

team_season + player provide dimensional context
```

### Complementary Relationship
- **match_team**: Answers "How did the TEAM perform?"
- **match_player_summary**: Answers "How did INDIVIDUALS contribute?"
- Together: Complete picture of match performance at multiple levels

## Optimal Structure Recommendations

### Essential Columns for match_team
**MUST HAVE**:
- match_id, team_season_id (relationships)
- goals, result (outcomes)
- possession_pct, passing_acc_pct (team style)
- defensive metrics (tackles, interceptions, clearances)
- attacking metrics (shots, corners, crosses)

**NICE TO HAVE**:
- xG (when available)
- Formation/tactical setup

**REDUNDANT/QUESTIONABLE**:
- match_date (duplicates match table)
- season_id (derivable from team_season)
- match_type/subtype (duplicates match table)

### Essential Columns for match_player_summary
**MUST HAVE**:
- match_id, player_id, team_season_id (relationships)
- minutes_played, position
- Core stats: goals, assists, shots, passes
- Advanced metrics: xG, xA, progressive actions

**NICE TO HAVE**:
- Defensive actions by position
- Heat map data
- Substitution timing

**REDUNDANT/QUESTIONABLE**:
- player_name (should reference player table)
- age (calculate from player.dob and match.date)
- season_id (derivable from match)

### Missing Data That Would Enhance Analysis

**For match_team**:
- Formation used
- Manager/coach
- Team xG (currently 99.93% NULL)
- Shots faced/allowed
- Pressing intensity metrics

**For match_player_summary**:
- Substitution minute (not just total minutes)
- Distance covered
- Sprint count
- Positional heat maps
- Pass direction/progression

**For both**:
- Standardized team_id across seasons (currently using team_season_id)
- Match importance/context flags
- Injury/suspension indicators

## Data Quality Observations

### Strengths
- Complete player-team linkage (no orphans)
- Goals perfectly reconcile between team and player levels (347/348 matches)
- Comprehensive coverage 2013-2025
- Position data well-populated

### Weaknesses
- xG data 99.93% missing in match_team
- Possession/tactical data only from 2019 onwards
- 28 matches without match_team records
- Some derived fields stored redundantly

## Migration Strategy Implications

Based on this analysis, the migration should:

1. **Preserve the distinction** between team-level and player-level analytics
2. **Eliminate redundancy** by removing duplicate fields
3. **Enhance relationships** with proper foreign keys and constraints
4. **Add missing analytical fields** identified above
5. **Create materialized views** for common aggregations
6. **Implement data quality checks** for completeness

## Recommended Next Steps

1. **Clean Architecture**:
   - Remove redundant columns from both tables
   - Ensure all relationships use UUIDs consistently
   - Add missing foreign key constraints

2. **Enhanced Analytics**:
   - Create derived tables for common aggregations
   - Add calculated fields for advanced metrics
   - Implement slowly changing dimensions for team/player attributes

3. **Data Completeness**:
   - Backfill xG data where possible
   - Standardize NULL handling
   - Add data quality monitoring

This structure supports both real-time match analysis and historical trend analysis, providing flexibility for various analytical use cases while maintaining data integrity and performance.