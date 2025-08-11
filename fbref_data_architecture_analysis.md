# FBref HTML Source Data Architecture Analysis

## Overview
We have ~1,565 HTML match files from FBref containing detailed match statistics. Each file represents one match and contains multiple data tables.

## FBref HTML Data Structure

### Source Tables Found in HTML Files:
1. **Player Summary Tables** - `stats_{team_id}_summary`
   - Individual player performance stats for each team
   - ~23 columns: Player, #, Nation, Pos, Age, Min, Goals, Assists, PK, Shots, Cards, Fouls, etc.
   - Contains FBref player IDs in `data-append-csv` attribute (e.g., "baf27d3e")

2. **Goalkeeper Tables** - `keeper_stats_{team_id}`
   - Specialized goalkeeper statistics
   - Separate from regular player stats

3. **Team-Level Summary Data** (implied)
   - Match result, score, venue information
   - Team totals and aggregated statistics

## Current Database Architecture Issues

### Problems Identified:
1. **Redundant Table Structure**
   - `match_player_summary` (43,572 rows) - individual player stats
   - `match_team` (3,070 rows) - team-level match data
   - These tables have overlapping but separate responsibilities

2. **Primary Key Inconsistency**
   - Both tables use TEXT-based custom IDs instead of UUIDs
   - Pattern: "mps_{hex}" for player summaries, "mt_{hex}" for team data

3. **Data Type Issues**  
   - Age stored as TEXT instead of INTEGER in match_player_summary
   - Column ordering poor for DBeaver viewing

## Optimal Database Architecture Recommendations

### Hierarchical Data Structure:
```
Match (1) -> Match_Team (2) -> Match_Player_Summary (22+)
    ↓             ↓                    ↓
 1 match    2 teams per match    11+ players per team
```

### Proposed Table Relationships:

#### 1. **match** table (already exists)
- Core match metadata: date, venue, season, competition
- Primary Key: UUID
- FBref match_id preserved as TEXT field

#### 2. **match_team** table (rename from current)
- Team-level match statistics and results  
- Primary Key: UUID (migrate from TEXT)
- Foreign Keys: match_uuid, team_season_id (already UUIDs)
- Columns: score, possession, shots, cards_yellow, cards_red, etc.

#### 3. **match_player_summary** table  
- Individual player performance stats
- Primary Key: UUID (migrate from TEXT)
- Foreign Keys: match_team_uuid, player_uuid, season_uuid
- Fix: Age as INTEGER, not TEXT
- All individual stats: goals, assists, minutes, shots, fouls, etc.

### Key Improvements:
1. **Consistent UUID Primary Keys** across all tables
2. **Proper Foreign Key Relationships** linking match -> team -> player hierarchy
3. **Correct Data Types** (age as INTEGER)
4. **Optimized Column Ordering** for DBeaver usability
5. **Preserved FBref IDs** for data lineage and future scraping

## Migration Priority:
1. **match_team** first (smaller, simpler, good test case)
2. **match_player_summary** second (apply lessons learned)
3. **Column reordering** for both tables

## Data Quality Notes:
- Both tables have excellent referential integrity (no orphans)
- 26 matches have player data but missing team summary data (investigate)
- FBref player IDs in HTML match current database player_id values
- All foreign key relationships are valid

This architecture aligns with FBref's hierarchical match structure while providing the UUID consistency and data quality improvements we've established in other tables.