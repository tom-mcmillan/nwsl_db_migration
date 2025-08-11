# FBref HTML Data Analysis Summary

## Executive Summary

Analysis of 1,565 FBref HTML match files reveals significant opportunities to fill database gaps. The data shows a clear evolution from basic statistics (2013-2018) to comprehensive advanced metrics (2019-2025).

## Key Findings

### 1. Data Availability Timeline

| Period | Years | Available Data | Coverage |
|--------|-------|----------------|----------|
| **Early Era** | 2013-2018 | Summary stats, Goalkeeper data | 100% matches have basic data |
| **Transition** | 2019-2021 | Full advanced stats introduced | 100% have all table types |
| **Modern Era** | 2022-2025 | Complete data suite | 100% comprehensive coverage |

### 2. Current Database Gaps vs Available Data

| Database Table | Current Status | HTML Data Available | Extractable Matches |
|----------------|---------------|-------------------|-------------------|
| **match_player_passing** | EMPTY | 2019-2025 | ~931 matches |
| **match_player_possession** | EMPTY | 2019-2025 | ~931 matches |
| **match_player_defensive_actions** | EMPTY | 2019-2025 | ~931 matches |
| **match_player_misc** | EMPTY | 2019-2025 | ~931 matches |
| **match_player_pass_types** | EMPTY | 2019-2025 | ~931 matches |
| **match_goalkeeper_summary** | 2,918 records | 2013-2025 | ALL matches (gaps exist) |
| **match_shot** | 23,333 records | 2019-2025 | ~931 matches |
| **match_team** | 3,070 records | Limited in HTML | Verify alternate source |

### 3. HTML Table Structure

Each match file contains tables with consistent ID patterns:
- `stats_{team_hex_id}_summary` - Player match statistics
- `stats_{team_hex_id}_passing` - Passing details (2019+)
- `stats_{team_hex_id}_possession` - Possession stats (2019+)
- `stats_{team_hex_id}_defense` - Defensive actions (2019+)
- `stats_{team_hex_id}_misc` - Miscellaneous stats (2019+)
- `keeper_stats_{team_hex_id}` - Goalkeeper statistics
- `shots_{team_hex_id}` or `shots_all` - Shot-by-shot data (2019+)

### 4. Data Quality Observations

**Strengths:**
- Consistent table structure within each era
- Player and team hex IDs embedded in table IDs
- Complete coverage for recent seasons
- Multi-level column headers preserve context

**Challenges:**
- Column format evolved over time (simpler in 2013-2018)
- Multi-level pandas columns require special handling
- Player hex IDs need extraction from data-append-csv attributes
- Some early years have minimal goalkeeper columns (8 vs 24 in modern)

## Extraction Priority Recommendations

### TIER 1: Immediate High-Impact Extraction
**Goal:** Fill critical gaps with readily available data

1. **match_goalkeeper_summary** gaps
   - Available: ALL years (2013-2025)
   - Current: Only 2,918 of ~3,100 expected records
   - Impact: Completes fundamental statistics

2. **match_player_summary** verification
   - Available: ALL years
   - Current: 43,572 records (verify completeness)
   - Impact: Ensures core data integrity

3. **match_shot** for recent years
   - Available: 2019-2025 (931 matches)
   - Current: 23,333 records (verify gaps)
   - Impact: Advanced analytics capability

### TIER 2: Advanced Statistics Extraction
**Goal:** Populate empty advanced tables for modern era

4. **match_player_passing**
   - Available: 2019-2025
   - Extract ~28 columns per player per match
   - ~27,000 new records expected

5. **match_player_possession**
   - Available: 2019-2025
   - Extract ~28 columns including touches, dribbles
   - ~27,000 new records expected

6. **match_player_defensive_actions**
   - Available: 2019-2025
   - Extract ~22 columns including tackles, blocks
   - ~27,000 new records expected

7. **match_player_misc**
   - Available: 2019-2025
   - Extract ~22 columns including fouls, offsides
   - ~27,000 new records expected

### TIER 3: Pass Types Completion
8. **match_player_pass_types**
   - Available: 2019-2025
   - Extract ~21 columns for pass variety
   - ~27,000 new records expected

### TIER 4: Historical Gaps (Lower Priority)
9. **Pre-2019 enhanced data**
   - Limited to basic stats only
   - Consider if worth partial extraction

## Technical Implementation Notes

### HTML Parsing Strategy
```python
# Key patterns identified:
1. Use BeautifulSoup with table ID selection
2. Extract team hex IDs from table IDs: 
   re.search(r'stats_([a-f0-9]{8})_', table_id)
3. Handle multi-level columns from pd.read_html()
4. Extract player hex IDs from tr[data-append-csv]
```

### Data Validation Requirements
- Verify FBref hex ID format (8 character alphanumeric)
- Map FBref player/team IDs to database UUIDs
- Handle missing data gracefully (especially pre-2019)
- Validate numeric fields before insertion

### Expected Data Volume
- **Total extractable records:** ~135,000 new player stat records
- **Matches with advanced stats:** 931 (2019-2025)
- **Average players per match:** ~29 (both teams)
- **Tables to populate:** 5 empty + 2 partial

## Next Steps

1. **Implement Tier 1 extraction** for immediate gap filling
2. **Create ID mapping table** for FBref hex to database UUID conversion
3. **Build extraction pipeline** with proper error handling
4. **Validate extracted data** against known match results
5. **Schedule batch processing** by season for efficiency

## File Samples for Testing

- **Early era (2013):** `match_01213dc2.html` - Basic stats only
- **Transition (2019):** `match_01cdf2c9.html` - First advanced stats
- **Modern era (2025):** `match_008e301f.html` - Complete data suite

## Database Connection
```bash
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d nwsl_data
```

## HTML Files Location
```
/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/
```

Total files: 1,565 matching 1,563 database matches (2 extra files to investigate)