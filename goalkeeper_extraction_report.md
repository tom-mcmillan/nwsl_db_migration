# Goalkeeper Data Extraction Report

## Executive Summary
Successfully extracted and inserted goalkeeper performance data from FBref HTML files to achieve near-complete database coverage.

## Initial State
- **Total matches**: 1,563
- **Matches with goalkeeper data**: 1,467 (93.9%)
- **Missing goalkeeper data**: 96 matches (6.1%)

### Missing Data Breakdown
- **2015 season**: 93 matches (0% goalkeeper coverage) - COMPLETE MISSING
- **2014 season**: 1 match missing (99.6% coverage)
- **2021 season**: 2 matches missing (99.3% coverage)

## Extraction Process

### Technical Approach
1. **HTML Structure Analysis**: Identified that goalkeeper data in 2013-2018 files is embedded within player summary tables rather than dedicated goalkeeper tables
2. **Data Extraction**: Used BeautifulSoup to parse HTML and pandas to convert tables to DataFrames
3. **Player Identification**: Filtered rows where Position column contains "GK"
4. **FBref ID Mapping**: Extracted player FBref hex IDs from data-append-csv attributes in table rows

### Key Findings
- 2015 matches use summary tables (format: `stats_{team_id}_summary`)
- Goalkeeper data embedded in general player statistics tables
- Successfully extracted player names, minutes played, and basic performance metrics
- FBref player IDs successfully extracted from HTML row attributes

## Final Results

### Database Coverage
- **Total matches**: 1,563
- **Matches with goalkeeper data**: 1,561 (99.87%)
- **Remaining missing**: 2 matches (0.13%)

### Successful Insertions
- **2014 season**: 1 match successfully processed (100% of missing)
- **2015 season**: 93 matches successfully processed (100% of missing)
- **2021 season**: 0 matches processed (2 matches appear to be forfeits with no player data)

### Remaining Gaps
Two 2021 matches remain without goalkeeper data:
- `770ec49c` (2021-09-04): 3-0 result but no player data in HTML (likely forfeit)
- `0bbd1959` (2021-09-12): 0-3 result but no player data in HTML (likely forfeit)

## Data Quality Notes

### Successfully Extracted Fields
- Player name
- Player FBref ID
- Team FBref ID
- Minutes played
- Basic performance metrics (goals, assists, cards)

### Limited Data for Historical Seasons
2015 season data from summary tables contains limited goalkeeper-specific statistics compared to modern dedicated keeper tables. Available metrics are primarily:
- Minutes played
- Yellow/red cards
- Basic involvement stats

### Data Integrity
- All extracted player IDs properly mapped to existing player records
- Team season IDs correctly linked
- No duplicate records created (used ON CONFLICT handling)

## Technical Implementation

### Files Created
1. `/Users/thomasmcmillan/projects/nwsl_db_migration/analyze_gk_html.py` - HTML structure analysis tool
2. `/Users/thomasmcmillan/projects/nwsl_db_migration/extract_missing_goalkeeper_data.py` - Main extraction and insertion script

### Database Updates
- Table: `match_goalkeeper_performance`
- Records added: ~190 (2 goalkeepers per match for 94 successfully processed matches)
- Primary key: Generated using MD5 hash of match_id + player_id

## Recommendations

1. **Forfeit Handling**: The 2 remaining 2021 matches appear to be forfeits. Consider marking these specially in the database rather than expecting goalkeeper data.

2. **Data Enrichment**: For the 2015 season, consider supplementing the basic goalkeeper data with team-level statistics if more detailed individual goalkeeper metrics are needed.

3. **Validation**: Run periodic checks to ensure goalkeeper coverage remains high as new matches are added to the database.

## Conclusion

Successfully achieved 99.87% goalkeeper data coverage, up from 93.9%. The extraction process successfully handled the varying HTML structures across different seasons. The only remaining gaps are 2 matches that appear to be forfeits with no actual player participation data available.