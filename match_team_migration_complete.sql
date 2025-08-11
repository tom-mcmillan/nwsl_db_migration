-- =====================================================
-- COMPLETE MATCH_TEAM TO MATCH_TEAM_STATS MIGRATION SCRIPT
-- Purpose: Transform match_team table for optimal team performance analysis
-- Author: PostgreSQL Migration Specialist
-- Date: 2025-08-10
-- =====================================================

-- This script is idempotent and can be safely re-run

BEGIN;

-- =====================================================
-- PHASE 1: CREATE NEW TABLE STRUCTURE
-- =====================================================

DROP TABLE IF EXISTS match_team_stats CASCADE;

CREATE TABLE match_team_stats (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Foreign Keys and Relationships
    match_id TEXT NOT NULL,
    team_season_id UUID NOT NULL,
    opponent_team_season_id UUID,
    
    -- Match Context
    is_home BOOLEAN NOT NULL,
    match_date DATE NOT NULL,
    season_id INTEGER NOT NULL,
    
    -- Match Classification
    match_type_name TEXT,
    match_subtype_name TEXT,
    
    -- Match Outcome
    goals INTEGER,
    goals_against INTEGER,
    result TEXT CHECK (result IN ('W', 'L', 'D')),
    
    -- Possession and Control
    possession_pct INTEGER CHECK (possession_pct >= 0 AND possession_pct <= 100),
    touches INTEGER CHECK (touches >= 0),
    
    -- Passing Performance
    passing_acc_pct INTEGER CHECK (passing_acc_pct >= 0 AND passing_acc_pct <= 100),
    crosses INTEGER CHECK (crosses >= 0),
    long_balls INTEGER CHECK (long_balls >= 0),
    
    -- Shooting Performance
    sot_pct INTEGER CHECK (sot_pct >= 0 AND sot_pct <= 100),
    xg NUMERIC(4,2),
    
    -- Defensive Performance
    saves_pct INTEGER CHECK (saves_pct >= 0 AND saves_pct <= 100),
    tackles INTEGER CHECK (tackles >= 0),
    interceptions INTEGER CHECK (interceptions >= 0),
    clearances INTEGER CHECK (clearances >= 0),
    aerials_won INTEGER CHECK (aerials_won >= 0),
    
    -- Discipline and Set Pieces
    fouls INTEGER CHECK (fouls >= 0),
    corners INTEGER CHECK (corners >= 0),
    offsides INTEGER CHECK (offsides >= 0),
    
    -- Restarts
    goal_kicks INTEGER CHECK (goal_kicks >= 0),
    throw_ins INTEGER CHECK (throw_ins >= 0),
    
    -- Metadata
    fbref_match_team_id TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add constraints
ALTER TABLE match_team_stats ADD CONSTRAINT check_self_opponent 
    CHECK (team_season_id != opponent_team_season_id);

-- =====================================================
-- PHASE 2: MIGRATE DATA
-- =====================================================

INSERT INTO match_team_stats (
    match_id,
    team_season_id,
    opponent_team_season_id,
    is_home,
    match_date,
    season_id,
    match_type_name,
    match_subtype_name,
    goals,
    goals_against,
    result,
    possession_pct,
    touches,
    passing_acc_pct,
    crosses,
    long_balls,
    sot_pct,
    xg,
    saves_pct,
    tackles,
    interceptions,
    clearances,
    aerials_won,
    fouls,
    corners,
    offsides,
    goal_kicks,
    throw_ins,
    fbref_match_team_id
)
SELECT 
    mt.match_id,
    mt.team_season_id,
    CASE 
        WHEN m.home_team_season_id = mt.team_season_id THEN m.away_team_season_id
        WHEN m.away_team_season_id = mt.team_season_id THEN m.home_team_season_id
        ELSE NULL
    END as opponent_team_season_id,
    (m.home_team_season_id = mt.team_season_id) as is_home,
    mt.match_date,
    mt.season_id,
    mt.match_type_name,
    mt.match_subtype_name,
    mt.goals,
    opp.goals as goals_against,
    mt.result,
    mt.possession_pct,
    mt.touches,
    mt.passing_acc_pct,
    mt.crosses,
    mt.long_balls,
    mt.sot_pct,
    mt.xg,
    mt.saves_pct,
    mt.tackles,
    mt.interceptions,
    mt.clearances,
    mt.aerials_won,
    mt.fouls,
    mt.corners,
    mt.offsides,
    mt.goal_kicks,
    mt.throw_ins,
    mt.match_team_id as fbref_match_team_id
FROM match_team mt
JOIN match m ON mt.match_id = m.match_id
LEFT JOIN match_team opp ON mt.match_id = opp.match_id 
    AND mt.match_team_id != opp.match_team_id;

-- =====================================================
-- PHASE 3: ADD FOREIGN KEYS AND INDEXES
-- =====================================================

-- Foreign Keys
ALTER TABLE match_team_stats 
    ADD CONSTRAINT fk_match_team_stats_match 
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE;

ALTER TABLE match_team_stats 
    ADD CONSTRAINT fk_match_team_stats_team_season 
    FOREIGN KEY (team_season_id) REFERENCES team_season(id) ON DELETE CASCADE;

ALTER TABLE match_team_stats 
    ADD CONSTRAINT fk_match_team_stats_opponent 
    FOREIGN KEY (opponent_team_season_id) REFERENCES team_season(id) ON DELETE CASCADE;

-- Performance Indexes
CREATE INDEX idx_match_team_stats_match_id ON match_team_stats(match_id);
CREATE INDEX idx_match_team_stats_team_season ON match_team_stats(team_season_id);
CREATE INDEX idx_match_team_stats_opponent ON match_team_stats(opponent_team_season_id);
CREATE INDEX idx_match_team_stats_date ON match_team_stats(match_date);
CREATE INDEX idx_match_team_stats_season ON match_team_stats(season_id);
CREATE INDEX idx_match_team_stats_home_away ON match_team_stats(is_home);
CREATE INDEX idx_match_team_stats_result ON match_team_stats(result);
CREATE INDEX idx_match_team_stats_match_type ON match_team_stats(match_type_name, match_subtype_name);

-- Composite Indexes for common queries
CREATE INDEX idx_match_team_stats_team_home ON match_team_stats(team_season_id, is_home);
CREATE INDEX idx_match_team_stats_team_date ON match_team_stats(team_season_id, match_date);
CREATE INDEX idx_match_team_stats_h2h ON match_team_stats(team_season_id, opponent_team_season_id);

-- =====================================================
-- PHASE 4: ADD DOCUMENTATION
-- =====================================================

COMMENT ON TABLE match_team_stats IS 'Team performance statistics per match, optimized for tactical and collective performance analysis';
COMMENT ON COLUMN match_team_stats.id IS 'Unique identifier (UUID) for each team match performance record';
COMMENT ON COLUMN match_team_stats.match_id IS 'Foreign key to match table';
COMMENT ON COLUMN match_team_stats.team_season_id IS 'Foreign key to team_season table identifying the team';
COMMENT ON COLUMN match_team_stats.opponent_team_season_id IS 'Direct link to opponent for head-to-head analysis';
COMMENT ON COLUMN match_team_stats.is_home IS 'TRUE if team played at home, FALSE if away';
COMMENT ON COLUMN match_team_stats.match_date IS 'Date of the match (denormalized for performance)';
COMMENT ON COLUMN match_team_stats.season_id IS 'Season identifier (denormalized for aggregations)';
COMMENT ON COLUMN match_team_stats.goals_against IS 'Goals conceded (opponent goals)';
COMMENT ON COLUMN match_team_stats.result IS 'Match result from team perspective: W(in), L(oss), D(raw)';
COMMENT ON COLUMN match_team_stats.possession_pct IS 'Ball possession percentage (0-100)';
COMMENT ON COLUMN match_team_stats.passing_acc_pct IS 'Pass completion percentage (0-100)';
COMMENT ON COLUMN match_team_stats.sot_pct IS 'Shots on target percentage (0-100)';
COMMENT ON COLUMN match_team_stats.xg IS 'Expected goals statistical metric';
COMMENT ON COLUMN match_team_stats.saves_pct IS 'Save percentage for goalkeeper (0-100)';
COMMENT ON COLUMN match_team_stats.fbref_match_team_id IS 'Original FBref identifier preserved for reference';

-- =====================================================
-- PHASE 5: CREATE SUPPORTING OBJECTS
-- =====================================================

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER update_match_team_stats_updated_at 
    BEFORE UPDATE ON match_team_stats 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Materialized View for Performance
DROP MATERIALIZED VIEW IF EXISTS team_season_performance CASCADE;

CREATE MATERIALIZED VIEW team_season_performance AS
SELECT 
    ts.id as team_season_id,
    ts.team_id,
    ts.team_name_season_1 as team_name,
    mts.season_id,
    COUNT(*) as total_matches,
    SUM(CASE WHEN is_home THEN 1 ELSE 0 END) as home_matches,
    SUM(CASE WHEN NOT is_home THEN 1 ELSE 0 END) as away_matches,
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
    SUM(CASE WHEN result = 'W' THEN 3 WHEN result = 'D' THEN 1 ELSE 0 END) as points,
    SUM(CASE WHEN is_home AND result = 'W' THEN 1 ELSE 0 END) as home_wins,
    SUM(CASE WHEN is_home AND result = 'D' THEN 1 ELSE 0 END) as home_draws,
    SUM(CASE WHEN is_home AND result = 'L' THEN 1 ELSE 0 END) as home_losses,
    SUM(CASE WHEN NOT is_home AND result = 'W' THEN 1 ELSE 0 END) as away_wins,
    SUM(CASE WHEN NOT is_home AND result = 'D' THEN 1 ELSE 0 END) as away_draws,
    SUM(CASE WHEN NOT is_home AND result = 'L' THEN 1 ELSE 0 END) as away_losses,
    SUM(goals) as total_goals_for,
    SUM(goals_against) as total_goals_against,
    SUM(goals) - SUM(goals_against) as goal_difference,
    ROUND(AVG(goals), 2) as avg_goals_for,
    ROUND(AVG(goals_against), 2) as avg_goals_against,
    SUM(CASE WHEN goals_against = 0 THEN 1 ELSE 0 END) as clean_sheets,
    ROUND(AVG(possession_pct), 1) as avg_possession,
    ROUND(AVG(passing_acc_pct), 1) as avg_pass_accuracy,
    ROUND(AVG(xg), 2) as avg_xg,
    CURRENT_TIMESTAMP as last_refreshed
FROM match_team_stats mts
JOIN team_season ts ON mts.team_season_id = ts.id
GROUP BY ts.id, ts.team_id, ts.team_name_season_1, mts.season_id;

CREATE UNIQUE INDEX idx_team_season_performance_pk ON team_season_performance(team_season_id, season_id);
CREATE INDEX idx_team_season_performance_season ON team_season_performance(season_id);
CREATE INDEX idx_team_season_performance_points ON team_season_performance(points DESC);

-- Function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_team_performance()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY team_season_performance;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PHASE 6: VALIDATION
-- =====================================================

DO $$
DECLARE
    original_count INTEGER;
    migrated_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO original_count FROM match_team;
    SELECT COUNT(*) INTO migrated_count FROM match_team_stats;
    
    IF original_count != migrated_count THEN
        RAISE EXCEPTION 'Migration failed! Record count mismatch. Original: %, Migrated: %', 
            original_count, migrated_count;
    END IF;
    
    RAISE NOTICE 'Migration successful! % records migrated.', migrated_count;
END $$;

-- =====================================================
-- PHASE 7: BACKUP AND CLEANUP (Optional - Uncomment when ready)
-- =====================================================

-- Create backup before dropping
-- CREATE TABLE IF NOT EXISTS match_team_backup AS SELECT * FROM match_team;

-- Drop original table (uncomment when ready)
-- DROP TABLE match_team CASCADE;

COMMIT;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Summary
SELECT 
    'Migration Complete' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT match_id) as unique_matches,
    COUNT(DISTINCT team_season_id) as unique_teams
FROM match_team_stats;

-- Data quality check
SELECT 
    'Data Quality' as metric,
    ROUND(COUNT(opponent_team_season_id)::numeric / COUNT(*) * 100, 1) || '% with opponent' as value
FROM match_team_stats
UNION ALL
SELECT 
    'Home/Away Split',
    SUM(CASE WHEN is_home THEN 1 ELSE 0 END) || ' home / ' || 
    SUM(CASE WHEN NOT is_home THEN 1 ELSE 0 END) || ' away'
FROM match_team_stats;