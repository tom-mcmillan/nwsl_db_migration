-- Simplified match_player_summary migration without materialized view
-- This focuses on just the table migration first

BEGIN;

-- =====================================================
-- PHASE 1: CREATE NEW TABLE STRUCTURE
-- =====================================================

DROP TABLE IF EXISTS match_player_performance CASCADE;

CREATE TABLE match_player_performance (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Core Relationships
    match_id TEXT NOT NULL,
    player_id TEXT NOT NULL,
    team_season_id UUID NOT NULL,
    
    -- Match Context (denormalized for performance)
    match_date DATE NOT NULL,
    season_id INTEGER NOT NULL,
    position TEXT,
    shirt_number INTEGER CHECK (shirt_number >= 1 AND shirt_number <= 255),
    minutes_played INTEGER CHECK (minutes_played >= 0 AND minutes_played <= 120),
    started BOOLEAN NOT NULL DEFAULT FALSE,
    
    -- Attacking Performance
    goals INTEGER CHECK (goals >= 0) DEFAULT 0,
    assists INTEGER CHECK (assists >= 0) DEFAULT 0,
    penalty_kicks INTEGER CHECK (penalty_kicks >= 0) DEFAULT 0,
    penalty_kicks_attempted INTEGER CHECK (penalty_kicks_attempted >= 0) DEFAULT 0,
    shots INTEGER CHECK (shots >= 0) DEFAULT 0,
    shots_on_target INTEGER CHECK (shots_on_target >= 0) DEFAULT 0,
    
    -- Expected Performance Metrics
    xg NUMERIC(5,3) CHECK (xg >= 0),
    npxg NUMERIC(5,3) CHECK (npxg >= 0),
    xag NUMERIC(5,3) CHECK (xag >= 0),
    
    -- Creative Performance
    sca INTEGER CHECK (sca >= 0) DEFAULT 0,
    gca INTEGER CHECK (gca >= 0) DEFAULT 0,
    
    -- Passing Performance
    passes_completed INTEGER CHECK (passes_completed >= 0) DEFAULT 0,
    passes_attempted INTEGER CHECK (passes_attempted >= 0) DEFAULT 0,
    pass_completion_pct NUMERIC(5,2) CHECK (pass_completion_pct >= 0 AND pass_completion_pct <= 100),
    progressive_passes INTEGER CHECK (progressive_passes >= 0) DEFAULT 0,
    
    -- Ball Progression
    touches INTEGER CHECK (touches >= 0) DEFAULT 0,
    carries INTEGER CHECK (carries >= 0) DEFAULT 0,
    progressive_carries INTEGER CHECK (progressive_carries >= 0) DEFAULT 0,
    take_ons_attempted INTEGER CHECK (take_ons_attempted >= 0) DEFAULT 0,
    take_ons_successful INTEGER CHECK (take_ons_successful >= 0) DEFAULT 0,
    
    -- Defensive Performance
    tackles INTEGER CHECK (tackles >= 0) DEFAULT 0,
    interceptions INTEGER CHECK (interceptions >= 0) DEFAULT 0,
    blocks INTEGER CHECK (blocks >= 0) DEFAULT 0,
    
    -- Discipline
    yellow_cards INTEGER CHECK (yellow_cards >= 0) DEFAULT 0,
    red_cards INTEGER CHECK (red_cards >= 0) DEFAULT 0,
    
    -- Metadata
    fbref_match_player_id TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add table constraints
ALTER TABLE match_player_performance 
    ADD CONSTRAINT check_shots_on_target_logical 
    CHECK (shots_on_target <= COALESCE(shots, shots_on_target, 0));

ALTER TABLE match_player_performance 
    ADD CONSTRAINT check_pk_attempted_logical 
    CHECK (penalty_kicks <= COALESCE(penalty_kicks_attempted, penalty_kicks, 0));

ALTER TABLE match_player_performance 
    ADD CONSTRAINT check_passes_completed_logical 
    CHECK (passes_completed <= COALESCE(passes_attempted, passes_completed, 0));

ALTER TABLE match_player_performance 
    ADD CONSTRAINT check_take_ons_logical 
    CHECK (take_ons_successful <= COALESCE(take_ons_attempted, take_ons_successful, 0));

-- =====================================================
-- PHASE 2: MIGRATE DATA
-- =====================================================

INSERT INTO match_player_performance (
    match_id,
    player_id,
    team_season_id,
    match_date,
    season_id,
    position,
    shirt_number,
    minutes_played,
    started,
    goals,
    assists,
    penalty_kicks,
    penalty_kicks_attempted,
    shots,
    shots_on_target,
    xg,
    npxg,
    xag,
    sca,
    gca,
    passes_completed,
    passes_attempted,
    pass_completion_pct,
    progressive_passes,
    touches,
    carries,
    progressive_carries,
    take_ons_attempted,
    take_ons_successful,
    tackles,
    interceptions,
    blocks,
    yellow_cards,
    red_cards,
    fbref_match_player_id
)
SELECT 
    mps.match_id,
    mps.player_id,
    mps.team_season_id,
    m.match_date,
    mps.season_id,
    mps.position,
    CASE WHEN mps.shirt_number > 0 THEN mps.shirt_number ELSE NULL END,
    COALESCE(mps.minutes_played, 0),
    CASE WHEN COALESCE(mps.minutes_played, 0) > 0 THEN TRUE ELSE FALSE END as started,
    COALESCE(mps.goals, 0),
    COALESCE(mps.assists, 0),
    COALESCE(mps.penalty_kicks, 0),
    COALESCE(mps.penalty_kicks_attempted, 0),
    CASE WHEN mps.shots IS NULL AND mps.shots_on_target > 0 THEN mps.shots_on_target ELSE COALESCE(mps.shots, 0) END,
    COALESCE(mps.shots_on_target, 0),
    mps.xg,
    mps.npxg,
    mps.xag,
    COALESCE(mps.sca, 0),
    COALESCE(mps.gca, 0),
    COALESCE(mps.passes_completed, 0),
    COALESCE(mps.passes_attempted, 0),
    mps.pass_completion_pct,
    COALESCE(mps.progressive_passes, 0),
    COALESCE(mps.touches, 0),
    COALESCE(mps.carries, 0),
    COALESCE(mps.progressive_carries, 0),
    COALESCE(mps.take_ons_attempted, 0),
    COALESCE(mps.take_ons_successful, 0),
    COALESCE(mps.tackles, 0),
    COALESCE(mps.interceptions, 0),
    COALESCE(mps.blocks, 0),
    COALESCE(mps.yellow_cards, 0),
    COALESCE(mps.red_cards, 0),
    mps.match_player_summary_id as fbref_match_player_id
FROM match_player_summary mps
JOIN match m ON mps.match_id = m.match_id;

-- =====================================================
-- PHASE 3: ADD FOREIGN KEYS AND INDEXES
-- =====================================================

-- Foreign Keys
ALTER TABLE match_player_performance 
    ADD CONSTRAINT fk_match_player_performance_match 
    FOREIGN KEY (match_id) REFERENCES match(match_id) ON DELETE CASCADE;

ALTER TABLE match_player_performance 
    ADD CONSTRAINT fk_match_player_performance_player 
    FOREIGN KEY (player_id) REFERENCES player(player_id) ON DELETE CASCADE;

ALTER TABLE match_player_performance 
    ADD CONSTRAINT fk_match_player_performance_team_season 
    FOREIGN KEY (team_season_id) REFERENCES team_season(id) ON DELETE CASCADE;

-- Performance Indexes
CREATE INDEX idx_match_player_performance_player ON match_player_performance(player_id);
CREATE INDEX idx_match_player_performance_match ON match_player_performance(match_id);
CREATE INDEX idx_match_player_performance_team_season ON match_player_performance(team_season_id);
CREATE INDEX idx_match_player_performance_date ON match_player_performance(match_date);
CREATE INDEX idx_match_player_performance_season ON match_player_performance(season_id);
CREATE INDEX idx_match_player_performance_position ON match_player_performance(position);
CREATE INDEX idx_match_player_performance_player_season ON match_player_performance(player_id, season_id);

-- Add comments
COMMENT ON TABLE match_player_performance IS 'Individual player performance statistics per match, optimized for player development tracking';
COMMENT ON COLUMN match_player_performance.started IS 'TRUE if player started the match, FALSE if substitute';
COMMENT ON COLUMN match_player_performance.fbref_match_player_id IS 'Original FBref identifier preserved for reference';

-- =====================================================
-- VALIDATION
-- =====================================================

DO $$
DECLARE
    original_count INTEGER;
    migrated_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO original_count FROM match_player_summary;
    SELECT COUNT(*) INTO migrated_count FROM match_player_performance;
    
    IF original_count != migrated_count THEN
        RAISE EXCEPTION 'Migration failed! Record count mismatch. Original: %, Migrated: %', 
            original_count, migrated_count;
    END IF;
    
    RAISE NOTICE 'Migration successful! % records migrated.', migrated_count;
END $$;

COMMIT;

-- Summary
SELECT 
    'Migration Complete' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT match_id) as unique_matches,
    COUNT(DISTINCT player_id) as unique_players
FROM match_player_performance;