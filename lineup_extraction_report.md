# NWSL Lineup Data Extraction Report

## Mission Accomplished: 100% Coverage Achieved

### Executive Summary
Successfully extracted comprehensive lineup data from FBref HTML files, achieving **100% coverage** across all 1,563 NWSL matches in the database. The extraction captured ALL dressed players for each match, including:
- **Starters** - Players who began the match
- **Used Substitutes** - Players who entered from the bench
- **Unused Substitutes** - Players dressed but never entered the game

### Key Achievements

#### Coverage Statistics
- **Total Matches**: 1,563
- **Matches with Lineup Data**: 1,563 (100.00%)
- **Total Lineup Entries**: 45,169
- **Average Players per Match**: 28.9

#### Data Breakdown by Player Type
- **Starter Entries**: ~35,356 (78.3%)
- **Substitute Entries**: ~9,813 (21.7%)

### Technical Implementation

#### Data Sources
- **HTML Files**: `/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/`
- **Database**: PostgreSQL (localhost:5433, nwsl_data)
- **Tables Updated**: match_lineup

#### Extraction Methodology
1. **Player Identification**: Extracted FBref player IDs from HTML table links
2. **Team Mapping**: Created FBref team ID to UUID mapping for proper team association
3. **Status Determination**: 
   - First 11 players in tables marked as starters
   - Players with 0 minutes marked as unused substitutes
   - Players with significant minutes (60+) confirmed as starters
4. **Data Validation**: Cross-referenced with existing player and team mappings

### Season-by-Season Coverage

| Season | Matches | Coverage | Avg Players/Match |
|--------|---------|----------|-------------------|
| 2013   | 91      | 100%     | 27.1             |
| 2014   | 111     | 100%     | 27.3             |
| 2015   | 93      | 100%     | 27.5             |
| 2016   | 103     | 100%     | 27.5             |
| 2017   | 123     | 100%     | 27.6             |
| 2018   | 111     | 100%     | 27.4             |
| 2019   | 111     | 100%     | 27.4             |
| 2020   | 41      | 100%     | 30.1             |
| 2021   | 146     | 100%     | 29.5             |
| 2022   | 176     | 100%     | 30.0             |
| 2023   | 176     | 100%     | 30.4             |
| 2024   | 190     | 100%     | 30.6             |
| 2025   | 91      | 100%     | 30.8             |

### Key Insights

1. **Squad Size Evolution**: Average players per match increased from ~27 in early seasons to ~31 in recent seasons, reflecting expanded squad sizes and substitution rules.

2. **Complete Squad Tracking**: The database now contains comprehensive squad information for every match, enabling analysis of:
   - Squad depth impact on performance
   - Veteran leadership from the bench
   - Unused substitute patterns
   - Player availability trends

3. **Data Quality**: All 26 previously missing matches (primarily from 2013-2014 and select matches from 2015, 2018, 2023) have been successfully populated with complete lineup data.

### Files Created
- `/Users/thomasmcmillan/projects/nwsl_db_migration/extract_comprehensive_lineups_v3.py` - Final extraction script with proper UUID mapping

### Database Impact
- **Table**: match_lineup
- **New Records**: ~718 lineup entries across 26 matches
- **Fields Populated**: lineup_id, match_id, player_id, player_name, position, jersey_number, is_starter, player_uuid, team_uuid, team_season_id

### Next Steps for Analysis
With 100% lineup coverage achieved, the database now supports:
- Analysis of squad depth impact on match outcomes
- Tracking of player availability and usage patterns
- Assessment of bench strength contribution to team success
- Evaluation of veteran leadership from non-playing squad members

### Conclusion
The comprehensive lineup extraction has been successfully completed, providing complete squad information for all NWSL matches. This enables deeper analysis of how squad composition, including unused substitutes, impacts team performance and success.