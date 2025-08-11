# Match Team Table Analysis

## Current Table Structure

### Schema Definition
- **Table Name**: `match_team`
- **Primary Key**: `match_team_id` (TEXT) - Format: "mt_" + 8 character hex string
- **Total Records**: 3,070 (representing 1,535 matches × 2 teams)

### Column Analysis

| Column | Type | Nullable | Current Usage | Notes |
|--------|------|----------|---------------|-------|
| match_team_id | TEXT | NOT NULL | 100% filled | Primary key, "mt_" prefix format |
| match_id | TEXT | YES | 100% filled | Should have FK to match table |
| match_date | DATE | YES | 100% filled | Denormalized from match table |
| goals | INTEGER | YES | 100% filled | Core stat |
| result | TEXT | YES | 98.4% filled | W/L/D values |
| match_type_name | TEXT | YES | 100% filled | Denormalized from match_type |
| match_subtype_name | TEXT | YES | Variable | Often NULL for regular season |
| season_id | INTEGER | YES | 100% filled | Denormalized from season |
| team_season_id | UUID | YES | 100% filled | FK to team_season table |
| **Possession Stats** ||||
| possession_pct | INTEGER | YES | 60.4% filled | Missing for older seasons |
| passing_acc_pct | INTEGER | YES | 60.4% filled | Missing for older seasons |
| sot_pct | INTEGER | YES | 60.4% filled | Shots on target percentage |
| saves_pct | INTEGER | YES | 59.3% filled | Goalkeeper stat |
| **Match Events** ||||
| fouls | INTEGER | YES | 60.4% filled | |
| corners | INTEGER | YES | 60.3% filled | |
| crosses | INTEGER | YES | 60.4% filled | |
| touches | INTEGER | YES | 60.4% filled | |
| tackles | INTEGER | YES | 60.4% filled | |
| interceptions | INTEGER | YES | 60.4% filled | |
| aerials_won | INTEGER | YES | 60.4% filled | |
| clearances | INTEGER | YES | 60.4% filled | |
| offsides | INTEGER | YES | 60.4% filled | |
| goal_kicks | INTEGER | YES | 60.4% filled | |
| throw_ins | INTEGER | YES | 60.4% filled | |
| long_balls | INTEGER | YES | 60.4% filled | |
| xg | NUMERIC | YES | 0.07% filled | Only 2 records in 2025 have xG data |

## Data Quality Assessment

### Strengths
1. **Referential Integrity**: All team_season_id values have valid foreign keys
2. **Data Consistency**: Every match has exactly 2 team records (home and away)
3. **No Orphans**: All match_id values exist in the match table
4. **Complete Core Data**: Goals, results, and match identifiers are 100% populated

### Issues Identified

1. **Missing Foreign Key Constraint**: No FK from match_id to match table
2. **Heavy Denormalization**: match_date, season_id, match_type_name stored redundantly
3. **Inconsistent Stats Coverage**: Advanced stats only available from ~2019 onwards
4. **xG Data Gap**: Expected Goals (xG) almost entirely missing (only 2 records)
5. **No Natural Key Preservation**: Missing FBref team IDs in match context
6. **Limited Indexing**: Only primary key index exists

## Season Coverage Analysis

| Season | Records | Matches | Teams | Stats Coverage |
|--------|---------|---------|-------|----------------|
| 2025 | 182 | 91 | 14 | Partial (xG: 2 records) |
| 2024 | 380 | 190 | 14 | Full stats, no xG |
| 2023 | 348 | 174 | 12 | Full stats, no xG |
| 2022 | 352 | 176 | 12 | Full stats, no xG |
| 2021 | 288 | 144 | 10 | Full stats, no xG |
| 2020 | 82 | 41 | 9 | Full stats, no xG |
| 2019-2013 | 1,438 | 719 | 8-10 | Basic stats only |

## Alignment with FBref Source Data

Based on FBref HTML structure, this table represents the **Team Stats** section from match pages, which includes:
- Basic match information (score, result)
- Possession statistics
- Passing metrics
- Defensive actions
- Set pieces and match events

### Current Alignment Issues
1. **Missing Home/Away Indicator**: FBref clearly distinguishes home vs away teams
2. **No Venue Information**: Though available in FBref match data
3. **Missing Manager/Formation**: FBref includes tactical setup
4. **Incomplete xG Data**: FBref provides xG for recent seasons

## Recommended Improvements

### 1. Rename Table for Clarity
```sql
ALTER TABLE match_team RENAME TO match_team_stats;
```
**Rationale**: Better describes the table's purpose as team-level match statistics

### 2. Add Missing Constraints
```sql
-- Add foreign key to match table
ALTER TABLE match_team_stats 
ADD CONSTRAINT fk_mts_match 
FOREIGN KEY (match_id) REFERENCES match(match_id);

-- Add check constraint for result values
ALTER TABLE match_team_stats 
ADD CONSTRAINT chk_result_values 
CHECK (result IN ('W', 'L', 'D') OR result IS NULL);

-- Add check constraints for percentage fields
ALTER TABLE match_team_stats
ADD CONSTRAINT chk_possession_pct CHECK (possession_pct BETWEEN 0 AND 100),
ADD CONSTRAINT chk_passing_acc_pct CHECK (passing_acc_pct BETWEEN 0 AND 100),
ADD CONSTRAINT chk_sot_pct CHECK (sot_pct BETWEEN 0 AND 100),
ADD CONSTRAINT chk_saves_pct CHECK (saves_pct BETWEEN 0 AND 100);
```

### 3. Add Missing Columns
```sql
ALTER TABLE match_team_stats
ADD COLUMN is_home BOOLEAN,
ADD COLUMN opponent_team_season_id UUID REFERENCES team_season(id),
ADD COLUMN formation TEXT,
ADD COLUMN manager_name TEXT,
ADD COLUMN fbref_match_team_id TEXT; -- Preserve FBref's identifier if available
```

### 4. Create Performance Indexes
```sql
-- Index for common query patterns
CREATE INDEX idx_mts_match_id ON match_team_stats(match_id);
CREATE INDEX idx_mts_team_season_id ON match_team_stats(team_season_id);
CREATE INDEX idx_mts_match_date ON match_team_stats(match_date);
CREATE INDEX idx_mts_season_id ON match_team_stats(season_id);

-- Composite index for team performance queries
CREATE INDEX idx_mts_team_season_date 
ON match_team_stats(team_season_id, match_date DESC);
```

### 5. Consider Normalization
Remove denormalized columns that can be joined from related tables:
- `match_date` (available from match table)
- `season_id` (available via match → season)
- `match_type_name` and `match_subtype_name` (available via match → match_type)

### 6. Data Type Optimization
Consider changing percentage fields from INTEGER to NUMERIC(5,2) for better precision:
```sql
ALTER TABLE match_team_stats
ALTER COLUMN possession_pct TYPE NUMERIC(5,2),
ALTER COLUMN passing_acc_pct TYPE NUMERIC(5,2),
ALTER COLUMN sot_pct TYPE NUMERIC(5,2),
ALTER COLUMN saves_pct TYPE NUMERIC(5,2);
```

## Migration Strategy

### Phase 1: Immediate Improvements
1. Add missing foreign key constraint to match table
2. Add check constraints for data validation
3. Create performance indexes

### Phase 2: Schema Enhancement
1. Rename table to match_team_stats
2. Add is_home and opponent_team_season_id columns
3. Add FBref ID preservation column

### Phase 3: Data Quality
1. Backfill xG data where available from FBref
2. Populate is_home flag based on match table relationships
3. Link opponent teams for easier querying

### Phase 4: Optimization
1. Remove denormalized columns after verifying join performance
2. Convert percentage fields to NUMERIC type
3. Implement partitioning by season if needed for performance

## SQL Migration Script Foundation

```sql
-- Phase 1: Add constraints and indexes
ALTER TABLE match_team 
ADD CONSTRAINT fk_mt_match 
FOREIGN KEY (match_id) REFERENCES match(match_id);

CREATE INDEX idx_mt_match_id ON match_team(match_id);
CREATE INDEX idx_mt_team_season_id ON match_team(team_season_id);
CREATE INDEX idx_mt_season_date ON match_team(season_id, match_date);

-- Phase 2: Enhance schema
ALTER TABLE match_team RENAME TO match_team_stats;

ALTER TABLE match_team_stats
ADD COLUMN is_home BOOLEAN,
ADD COLUMN opponent_team_season_id UUID,
ADD COLUMN fbref_match_team_id TEXT;

-- Phase 3: Populate new columns
UPDATE match_team_stats mts
SET is_home = (
    SELECT CASE 
        WHEN m.home_team_season_id = mts.team_season_id THEN TRUE
        WHEN m.away_team_season_id = mts.team_season_id THEN FALSE
    END
    FROM match m
    WHERE m.match_id = mts.match_id
);

-- Set opponent team
UPDATE match_team_stats mts
SET opponent_team_season_id = (
    SELECT CASE 
        WHEN m.home_team_season_id = mts.team_season_id THEN m.away_team_season_id
        WHEN m.away_team_season_id = mts.team_season_id THEN m.home_team_season_id
    END
    FROM match m
    WHERE m.match_id = mts.match_id
);
```

## Conclusion

The match_team table is well-structured but would benefit from:
1. **Better naming** (match_team_stats) for clarity
2. **Additional constraints** for data integrity
3. **Strategic indexes** for query performance
4. **Home/away context** for easier analysis
5. **Reduced denormalization** where appropriate

The table successfully captures FBref's team-level match statistics but lacks some contextual information that would make queries simpler and more intuitive.