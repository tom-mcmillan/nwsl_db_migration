-- =====================================================
-- MATCH_GOALKEEPER_PERFORMANCE TABLE MIGRATION
-- Purpose: Transform to UUID primary key and clean structure
-- Author: PostgreSQL Migration Specialist
-- Date: 2025-08-10
-- =====================================================

BEGIN;

-- =====================================================
-- PHASE 1: CREATE NEW TABLE STRUCTURE
-- =====================================================

DROP TABLE IF EXISTS match_goalkeeper_performance_new CASCADE;

CREATE TABLE match_goalkeeper_performance_new (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Core Relationships
    match_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    team_season_id UUID NOT NULL,
    
    -- Match Context (denormalized for performance)
    match_date DATE NOT NULL,
    season_id INTEGER,
    minutes_played INTEGER CHECK (minutes_played >= 0 AND minutes_played <= 120),
    
    -- Goalkeeping Performance
    shots_on_target_against INTEGER CHECK (shots_on_target_against >= 0) DEFAULT 0,
    goals_against INTEGER CHECK (goals_against >= 0) DEFAULT 0,
    saves INTEGER CHECK (saves >= 0) DEFAULT 0,
    save_percentage NUMERIC(5,2),
    post_shot_xg NUMERIC(5,3) CHECK (post_shot_xg >= 0),
    
    -- Distribution - Launched Passes
    launched_completed INTEGER CHECK (launched_completed >= 0) DEFAULT 0,
    launched_attempted INTEGER CHECK (launched_attempted >= 0) DEFAULT 0,
    launched_completion_pct NUMERIC(5,2) CHECK (launched_completion_pct >= 0 AND launched_completion_pct <= 100),
    
    -- Distribution - All Passes
    passes_attempted INTEGER CHECK (passes_attempted >= 0) DEFAULT 0,
    passes_throws INTEGER CHECK (passes_throws >= 0) DEFAULT 0,
    passes_launch_pct NUMERIC(5,2) CHECK (passes_launch_pct >= 0 AND passes_launch_pct <= 100),
    passes_avg_length NUMERIC(5,2) CHECK (passes_avg_length >= 0),
    
    -- Goal Kicks
    goal_kicks_attempted INTEGER CHECK (goal_kicks_attempted >= 0) DEFAULT 0,
    goal_kicks_launch_pct NUMERIC(5,2) CHECK (goal_kicks_launch_pct >= 0 AND goal_kicks_launch_pct <= 100),
    goal_kicks_avg_length NUMERIC(5,2) CHECK (goal_kicks_avg_length >= 0),
    
    -- Cross Stopping
    crosses_opposed INTEGER CHECK (crosses_opposed >= 0) DEFAULT 0,
    crosses_stopped INTEGER CHECK (crosses_stopped >= 0) DEFAULT 0,
    crosses_stopped_pct NUMERIC(5,2) CHECK (crosses_stopped_pct >= 0 AND crosses_stopped_pct <= 100),
    
    -- Sweeper Actions
    sweeper_actions INTEGER CHECK (sweeper_actions >= 0) DEFAULT 0,
    sweeper_avg_distance NUMERIC(5,2) CHECK (sweeper_avg_distance >= 0),
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add logical constraints
ALTER TABLE match_goalkeeper_performance_new 
    ADD CONSTRAINT check_saves_logical 
    CHECK (saves <= COALESCE(shots_on_target_against, saves, 0));

ALTER TABLE match_goalkeeper_performance_new 
    ADD CONSTRAINT check_launched_passes_logical 
    CHECK (launched_completed <= COALESCE(launched_attempted, launched_completed, 0));

ALTER TABLE match_goalkeeper_performance_new 
    ADD CONSTRAINT check_crosses_stopped_logical 
    CHECK (crosses_stopped <= COALESCE(crosses_opposed, crosses_stopped, 0));

-- =====================================================
-- PHASE 2: MIGRATE DATA WITH IMPROVEMENTS
-- =====================================================

INSERT INTO match_goalkeeper_performance_new (
    match_id,
    player_id,
    team_season_id,
    match_date,
    season_id,
    minutes_played,
    shots_on_target_against,
    goals_against,
    saves,
    save_percentage,
    post_shot_xg,
    launched_completed,
    launched_attempted,
    launched_completion_pct,
    passes_attempted,
    passes_throws,
    passes_launch_pct,
    passes_avg_length,
    goal_kicks_attempted,
    goal_kicks_launch_pct,
    goal_kicks_avg_length,
    crosses_opposed,
    crosses_stopped,
    crosses_stopped_pct,
    sweeper_actions,
    sweeper_avg_distance
)
SELECT 
    mgp.match_id,
    mgp.player_id,
    mgp.team_season_id,
    m.match_date,
    COALESCE(mgp.season_id::integer, EXTRACT(YEAR FROM m.match_date)::integer),
    COALESCE(mgp.minutes_played, 0),
    COALESCE(mgp.shots_on_target_against, 0),
    COALESCE(mgp.goals_against, 0),
    COALESCE(mgp.saves, 0),
    CASE WHEN mgp.save_percentage >= 0 AND mgp.save_percentage <= 100 THEN mgp.save_percentage ELSE NULL END,
    mgp.post_shot_xg,
    COALESCE(mgp.launched_cmp, 0),
    COALESCE(mgp.launched_att, 0),
    mgp.launched_cmp_pct,
    COALESCE(mgp.passes_att, 0),
    COALESCE(mgp.passes_thr, 0),
    mgp.passes_launch_pct,
    mgp.passes_avg_len,
    COALESCE(mgp.goal_kicks_att, 0),
    mgp.goal_kicks_launch_pct,
    mgp.goal_kicks_avg_len,
    COALESCE(mgp.crosses_opp, 0),
    COALESCE(mgp.crosses_stp, 0),
    mgp.crosses_stp_pct,
    COALESCE(mgp.sweeper_opa, 0),
    mgp.sweeper_avg_dist
FROM match_goalkeeper_performance mgp
JOIN match m ON mgp.match_id = m.match_id
WHERE mgp.player_id IS NOT NULL;

-- =====================================================
-- PHASE 3: REPLACE OLD TABLE
-- =====================================================

-- Create backup
CREATE TABLE match_goalkeeper_performance_backup AS SELECT * FROM match_goalkeeper_performance;

-- Drop old table
DROP TABLE match_goalkeeper_performance CASCADE;

-- Rename new table
ALTER TABLE match_goalkeeper_performance_new RENAME TO match_goalkeeper_performance;

-- =====================================================
-- PHASE 4: ADD FOREIGN KEYS AND INDEXES
-- =====================================================

-- Foreign Keys
ALTER TABLE match_goalkeeper_performance 
    ADD CONSTRAINT fk_match_goalkeeper_performance_match 
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE;

ALTER TABLE match_goalkeeper_performance 
    ADD CONSTRAINT fk_match_goalkeeper_performance_player 
    FOREIGN KEY (player_id) REFERENCES player(player_id) ON DELETE CASCADE;

ALTER TABLE match_goalkeeper_performance 
    ADD CONSTRAINT fk_match_goalkeeper_performance_team_season 
    FOREIGN KEY (team_season_id) REFERENCES team_season(id) ON DELETE CASCADE;

-- Performance Indexes
CREATE INDEX idx_match_goalkeeper_performance_player ON match_goalkeeper_performance(player_id);
CREATE INDEX idx_match_goalkeeper_performance_match ON match_goalkeeper_performance(match_id);
CREATE INDEX idx_match_goalkeeper_performance_team_season ON match_goalkeeper_performance(team_season_id);
CREATE INDEX idx_match_goalkeeper_performance_date ON match_goalkeeper_performance(match_date);
CREATE INDEX idx_match_goalkeeper_performance_season ON match_goalkeeper_performance(season_id);

-- Specialized indexes for goalkeeper analysis
CREATE INDEX idx_match_goalkeeper_performance_saves ON match_goalkeeper_performance(saves) WHERE saves > 0;
CREATE INDEX idx_match_goalkeeper_performance_clean_sheets ON match_goalkeeper_performance(goals_against) WHERE goals_against = 0;
CREATE INDEX idx_match_goalkeeper_performance_player_season ON match_goalkeeper_performance(player_id, season_id);

-- =====================================================
-- PHASE 5: ADD DOCUMENTATION
-- =====================================================

COMMENT ON TABLE match_goalkeeper_performance IS 'Goalkeeper performance statistics per match, optimized for goalkeeper analysis';
COMMENT ON COLUMN match_goalkeeper_performance.id IS 'Unique identifier (UUID) for each goalkeeper match performance record';
COMMENT ON COLUMN match_goalkeeper_performance.saves IS 'Total saves made by goalkeeper';
COMMENT ON COLUMN match_goalkeeper_performance.save_percentage IS 'Save percentage (0-100)';
COMMENT ON COLUMN match_goalkeeper_performance.post_shot_xg IS 'Post-shot expected goals faced';
COMMENT ON COLUMN match_goalkeeper_performance.launched_completed IS 'Completed launched passes (long distribution)';
COMMENT ON COLUMN match_goalkeeper_performance.sweeper_actions IS 'Number of times goalkeeper acted as sweeper outside penalty area';

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER update_match_goalkeeper_performance_updated_at 
    BEFORE UPDATE ON match_goalkeeper_performance 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- PHASE 6: VALIDATION
-- =====================================================

DO $$
DECLARE
    original_count INTEGER;
    migrated_count INTEGER;
    valid_original_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO original_count FROM match_goalkeeper_performance_backup;
    SELECT COUNT(*) INTO valid_original_count FROM match_goalkeeper_performance_backup WHERE player_id IS NOT NULL;
    SELECT COUNT(*) INTO migrated_count FROM match_goalkeeper_performance;
    
    RAISE NOTICE 'Original records: %, Valid records (with player_id): %, Migrated: %', 
        original_count, valid_original_count, migrated_count;
    
    IF valid_original_count != migrated_count THEN
        RAISE EXCEPTION 'Migration failed! Valid record count mismatch. Valid Original: %, Migrated: %', 
            valid_original_count, migrated_count;
    END IF;
    
    RAISE NOTICE 'Migration successful! % valid records migrated (excluded % records with NULL player_id).', 
        migrated_count, original_count - valid_original_count;
END $$;

COMMIT;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Summary
SELECT 
    'Migration Complete' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT match_id) as unique_matches,
    COUNT(DISTINCT player_id) as unique_goalkeepers
FROM match_goalkeeper_performance;

-- Coverage verification
SELECT 
    'Coverage Analysis' as analysis,
    (SELECT COUNT(*) FROM match) as total_matches,
    COUNT(DISTINCT match_id) as matches_with_gk_data,
    ROUND(COUNT(DISTINCT match_id)::numeric / (SELECT COUNT(*) FROM match) * 100, 2) as coverage_pct
FROM match_goalkeeper_performance;

-- Clean sheets analysis
SELECT 
    'Clean Sheets' as metric,
    COUNT(CASE WHEN goals_against = 0 AND minutes_played >= 90 THEN 1 END) as clean_sheets,
    COUNT(CASE WHEN minutes_played >= 90 THEN 1 END) as full_games,
    ROUND(COUNT(CASE WHEN goals_against = 0 AND minutes_played >= 90 THEN 1 END)::numeric / 
          NULLIF(COUNT(CASE WHEN minutes_played >= 90 THEN 1 END), 0) * 100, 1) as clean_sheet_pct
FROM match_goalkeeper_performance;