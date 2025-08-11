# PostgreSQL Table Analysis Report
## Analysis Date: 2025-08-10

## 1. MATCH_PLAYER_SUMMARY TABLE

### Table Structure
- **Primary Key**: `match_player_summary_id` (TEXT, 12 chars, prefix: "mps_")
- **Total Rows**: 43,572
- **Total Columns**: 38 columns

### Key Columns and Data Types
```sql
match_player_summary_id  TEXT    -- Primary key (e.g., "mps_2eedc615")
match_player_id          TEXT    -- Foreign key to match_player
match_id                 TEXT    -- Foreign key to match
player_id                TEXT    -- Foreign key to player
player_name              TEXT
shirt_number             BIGINT
position                 TEXT
age                      TEXT    -- Should be INTEGER
minutes_played           BIGINT
-- 20+ statistical columns (goals, assists, xg, etc.) as BIGINT/REAL
season_id                BIGINT  -- Foreign key to season
player_uuid              UUID    -- Already migrated to UUID
season_uuid              UUID    -- Already migrated to UUID
team_season_id           UUID    -- Foreign key to team_season
```

### Data Quality Assessment
- **No NULL issues in critical columns**: All key foreign keys are populated
- **Missing data (acceptable)**:
  - shirt_number: 5,898 NULLs (13.5%)
  - position: 62 NULLs (0.14%)
  - age: 199 NULLs (0.46%)
- **No duplicate primary keys**: All 43,572 PKs are unique
- **No orphan records**: All foreign keys are valid
- **Age field issue**: Stored as TEXT but contains integer values (should be migrated to INTEGER)

### Foreign Key Relationships
- `match_player_id` → `match_player.match_player_id`
- `match_id` → `match.match_id`
- `player_id` → `player.player_id`
- `season_id` → `season.season_id`
- `team_season_id` → `team_season.id`

### UUID Migration Status
- ✅ `player_uuid` - Already migrated
- ✅ `season_uuid` - Already migrated
- ✅ `team_season_id` - Already migrated
- ❌ `match_player_summary_id` - Still using TEXT with prefix
- ❌ References to match, player, match_player still use TEXT IDs

---

## 2. MATCH_TEAM TABLE

### Table Structure
- **Primary Key**: `match_team_id` (TEXT, 11 chars, prefix: "mt_")
- **Total Rows**: 3,070
- **Total Columns**: 26 columns

### Key Columns and Data Types
```sql
match_team_id       TEXT     -- Primary key (e.g., "mt_302c0ddc")
match_id            TEXT     -- Foreign key to match
match_date          DATE
goals               INTEGER
result              TEXT     -- W/L/D
match_type_name     TEXT
match_subtype_name  TEXT
season_id           INTEGER  -- Foreign key to season
-- Multiple statistics columns (possession_pct, passing_acc_pct, etc.) as INTEGER
xg                  NUMERIC
team_season_id      UUID     -- Foreign key to team_season
```

### Data Quality Assessment
- **No NULL issues in critical columns**: All key fields populated
- **Missing data (minimal)**:
  - result: 50 NULLs (1.6%)
  - xg: 3,068 NULLs (99.9% - likely old data before xG tracking)
- **No duplicate primary keys**: All 3,070 PKs are unique
- **No orphan records**: All foreign keys are valid
- **Proper team count**: Exactly 2 teams per match (as expected)

### Foreign Key Relationships
- `match_id` → `match.match_id` (implicit, no FK constraint)
- `team_season_id` → `team_season.id`

### UUID Migration Status
- ✅ `team_season_id` - Already migrated
- ❌ `match_team_id` - Still using TEXT with prefix
- ❌ `match_id` reference still uses TEXT

---

## 3. TABLE RELATIONSHIP ANALYSIS

### Match Coverage
- **Matches in match_player_summary**: 1,561 unique matches
- **Matches in match_team**: 1,535 unique matches
- **Matches in both tables**: 1,535 matches
- **Matches only in match_player_summary**: 26 matches (1.7%)
  - These 26 matches have player data but no team summary data
  - Likely incomplete data imports or special match types

### Data Volume Comparison
- **match_player_summary**: 43,572 rows (~28 players per match average)
- **match_team**: 3,070 rows (exactly 2 per match)
- **Ratio**: ~14:1 (expected given roster sizes)

---

## 4. COLUMN ORDERING ISSUES FOR DBEAVER

### Current Issues
Both tables have poor column ordering for viewing in DBeaver:

**match_player_summary**: 
- IDs and UUIDs are scattered throughout
- Statistical columns not grouped logically
- Metadata mixed with statistics

**match_team**:
- Similar issues with scattered IDs
- Statistics not grouped by category

### Recommended Column Order

**match_player_summary** (optimized for viewing):
```sql
1. Identity columns: match_player_summary_id, match_id, player_id, player_name
2. Match context: season_id, match_player_id, team_season_id
3. Player info: shirt_number, position, age, minutes_played
4. Core stats: goals, assists, shots, shots_on_target
5. Advanced stats: xg, npxg, xag
6. Disciplinary: yellow_cards, red_cards
7. Other stats: remaining columns
8. UUIDs at end: player_uuid, season_uuid
```

**match_team** (optimized for viewing):
```sql
1. Identity: match_team_id, match_id, team_season_id
2. Match info: match_date, season_id, match_type_name, match_subtype_name
3. Result: goals, result
4. Possession stats: possession_pct, passing_acc_pct, touches
5. Defensive stats: tackles, interceptions, clearances
6. Other stats: remaining columns
7. Advanced: xg
```

---

## 5. RECOMMENDATIONS

### Priority 1: Data Type Corrections (LOW RISK)
1. **Convert `age` column** in match_player_summary from TEXT to INTEGER
   - Currently stores integer values as text
   - Simple ALTER TABLE operation
   - No data loss risk

### Priority 2: Primary Key Migration to UUID (MEDIUM RISK)
Both tables should migrate to UUID primary keys for consistency:

1. **match_player_summary table**:
   - Add new `id` UUID column
   - Keep `match_player_summary_id` as natural key
   - Update foreign key references in dependent tables

2. **match_team table**:
   - Add new `id` UUID column  
   - Keep `match_team_id` as natural key
   - Update foreign key references in dependent tables

### Priority 3: Column Reordering (LOW RISK)
Reorder columns in both tables for better usability in DBeaver:
- Group related columns together
- Put most important columns first
- Move UUIDs to the end

### Priority 4: Data Completeness (LOW PRIORITY)
- Investigate 26 matches with player data but no team data
- Consider adding missing team summaries or flagging incomplete matches

### Priority 5: Add Missing Constraints (MEDIUM RISK)
- Add explicit foreign key constraint for `match_team.match_id`
- Consider adding CHECK constraints for valid values (e.g., result IN ('W','L','D'))

---

## 6. MIGRATION PRIORITY

### Recommended Order:
1. **Start with match_team** (smaller, simpler)
   - Only 3,070 rows
   - Fewer columns
   - Good test case for migration process

2. **Then migrate match_player_summary**
   - Larger dataset (43,572 rows)
   - More complex with more relationships
   - Apply lessons learned from match_team

### Next Steps:
1. Create backup of both tables
2. Write migration script for match_team UUID primary key
3. Test migration on match_team
4. Apply column reordering to match_team
5. Repeat process for match_player_summary
6. Update any dependent views or queries