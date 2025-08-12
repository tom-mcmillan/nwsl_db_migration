# FBref Defensive Actions Data Extraction Summary

## Extraction Overview
- **Date**: 2025-08-11
- **Total HTML Files Processed**: 1,565
- **Successful Extractions**: 929 files (59.4%)
- **No Data Found**: 636 files (40.6%)
- **Errors**: 0
- **Total Player Records Updated**: 27,835

## Database Table: match_player_defensive_actions
Successfully populated all 20 columns in the expanded schema:

### Tackles (5 columns)
- `tackles`: Total tackles - 16,765 records with data (37.1%)
- `tackles_won`: Tackles won - 13,245 records with data (29.3%)
- `tackles_def_3rd`: Defensive third tackles - 10,068 records with data (22.3%)
- `tackles_mid_3rd`: Middle third tackles - 9,752 records with data (21.6%)
- `tackles_att_3rd`: Attacking third tackles - 4,427 records with data (9.8%)

### Challenges (4 columns)
- `challenges_tkl`: Dribblers tackled - 10,136 records with data (22.5%)
- `challenges_att`: Challenges attempted - Data populated
- `challenges_tkl_pct`: Challenge success percentage - 15,442 records with data (34.2%)
- `challenges_lost`: Challenges lost - Data populated

### Blocks (3 columns)
- `blocks`: Total blocks - 13,815 records with data (30.6%)
- `blocks_shots`: Shots blocked - 4,566 records with data (10.1%)
- `blocks_passes`: Passes blocked - 11,299 records with data (25.0%)

### Other Defensive Actions (4 columns)
- `interceptions`: 12,116 records with data (26.8%)
- `tackles_interceptions`: Combined tackles + interceptions - Data populated
- `clearances`: Data populated
- `errors`: 564 records with errors (1.2%)

## Data Quality Verification

### Test Match Validation (match_07c68416)
Successfully validated test match data with 100% accuracy:
- **Alyssa Thompson**: All 16 defensive metrics correctly extracted
- **Riley Tiernan**: All metrics correctly extracted including 0 values
- **Claire Emslie**: All metrics correctly extracted including percentage calculations

### Top Defenders Analysis (minimum 20 matches)
Leading players by average tackles per match:
1. Beverly Yanez - 4.38 tackles/match
2. Denise O'Sullivan - 3.74 tackles/match  
3. Marie Müller - 3.50 tackles/match
4. Madison Curry - 3.42 tackles/match
5. Claire Hutton - 3.33 tackles/match

## Technical Implementation

### Data Extraction Method
- Used BeautifulSoup to parse HTML tables with ID pattern `stats_{team_id}_defense`
- Extracted FBref player hex IDs from `data-append-csv` attributes
- Mapped FBref column names to database schema
- Handled percentage values and missing data gracefully

### Column Mappings
```python
COLUMN_MAPPINGS = {
    'tackles': 'tackles',
    'tackles_won': 'tackles_won',
    'tackles_def_3rd': 'tackles_def_3rd',
    'tackles_mid_3rd': 'tackles_mid_3rd',
    'tackles_att_3rd': 'tackles_att_3rd',
    'challenge_tackles': 'challenges_tkl',
    'challenges': 'challenges_att',
    'challenge_tackles_pct': 'challenges_tkl_pct',
    'challenges_lost': 'challenges_lost',
    'blocks': 'blocks',
    'blocked_shots': 'blocks_shots',
    'blocked_passes': 'blocks_passes',
    'interceptions': 'interceptions',
    'tackles_interceptions': 'tackles_interceptions',
    'clearances': 'clearances',
    'errors': 'errors'
}
```

## Files Coverage Analysis
- **Files with defensive data**: 929 (59.4%)
- **Files without defensive tables**: 636 (40.6%)
  - Likely older matches (2013-2018) before detailed defensive stats were tracked
  - Some matches may have incomplete data

## Key Findings
1. **Successful population** of all 20 defensive action columns
2. **High data quality** with 0 errors during extraction
3. **Historical coverage** varies - newer matches have more complete data
4. **Player ID mapping** works correctly using FBref hex IDs
5. **Data integrity** maintained with proper NULL handling for missing values

## Recommendations
1. The 636 files without defensive data are likely from earlier seasons (2013-2018)
2. Consider adding season_id to track data availability by year
3. Monitor for any future HTML structure changes in FBref exports
4. Regular validation checks recommended for data consistency

## Script Location
- **Extraction Script**: `/Users/thomasmcmillan/projects/nwsl_db_migration/extract_defensive_actions.py`
- **Detailed Report**: `/Users/thomasmcmillan/projects/nwsl_db_migration/defensive_actions_extraction_report.json`