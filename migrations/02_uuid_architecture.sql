-- =====================================================
-- UUID ARCHITECTURE IMPLEMENTATION
-- Phase 2: Add UUID columns to all tables for performance
-- Maintains FBref IDs as natural keys
-- =====================================================

BEGIN;

-- =====================================================
-- 1. ADD UUID COLUMNS TO TABLES WITHOUT THEM
-- =====================================================

-- Tables that need UUID columns added
-- These tables currently only have text-based primary keys

-- match table (already has UUID columns for relationships, needs own UUID)
ALTER TABLE match 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid();
UPDATE match SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_uuid ON match(uuid);

-- match_player table
ALTER TABLE match_player 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid();
UPDATE match_player SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_player ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_player_uuid ON match_player(uuid);

-- match_lineup table
ALTER TABLE match_lineup 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid();
UPDATE match_lineup SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_lineup ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_lineup_uuid ON match_lineup(uuid);

-- match_shot table
ALTER TABLE match_shot 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid();
UPDATE match_shot SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_shot ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_shot_uuid ON match_shot(uuid);

-- match_goalkeeper_summary table
ALTER TABLE match_goalkeeper_summary 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid();
UPDATE match_goalkeeper_summary SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_goalkeeper_summary ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_goalkeeper_summary_uuid ON match_goalkeeper_summary(uuid);

-- match_venue_weather table
ALTER TABLE match_venue_weather 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS match_uuid uuid,
    ADD COLUMN IF NOT EXISTS venue_uuid uuid;
UPDATE match_venue_weather SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_venue_weather ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_venue_weather_uuid ON match_venue_weather(uuid);

-- match_type table
ALTER TABLE match_type 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid();
UPDATE match_type SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_type ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_type_uuid ON match_type(uuid);

-- match_subtype table
ALTER TABLE match_subtype 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS match_type_uuid uuid;
UPDATE match_subtype SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_subtype ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_subtype_uuid ON match_subtype(uuid);

-- shot_outcome table
ALTER TABLE shot_outcome 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid();
UPDATE shot_outcome SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE shot_outcome ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_shot_outcome_uuid ON shot_outcome(uuid);

-- player_season table
ALTER TABLE player_season 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS player_uuid uuid,
    ADD COLUMN IF NOT EXISTS season_uuid uuid;
UPDATE player_season SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE player_season ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_player_season_uuid ON player_season(uuid);

-- team_season table
ALTER TABLE team_season 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS team_uuid uuid,
    ADD COLUMN IF NOT EXISTS season_uuid uuid;
UPDATE team_season SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE team_season ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_team_season_uuid ON team_season(uuid);

-- team_record_regular_season table
ALTER TABLE team_record_regular_season 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS team_season_uuid uuid,
    ADD COLUMN IF NOT EXISTS season_uuid uuid;
UPDATE team_record_regular_season SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE team_record_regular_season ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_team_record_regular_season_uuid ON team_record_regular_season(uuid);

-- region table
ALTER TABLE region 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid();
UPDATE region SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE region ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_region_uuid ON region(uuid);

-- match_player detail tables
ALTER TABLE match_player_passing 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS match_player_uuid uuid;
UPDATE match_player_passing SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_player_passing ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_player_passing_uuid ON match_player_passing(uuid);

ALTER TABLE match_player_pass_types 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS match_player_uuid uuid;
UPDATE match_player_pass_types SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_player_pass_types ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_player_pass_types_uuid ON match_player_pass_types(uuid);

ALTER TABLE match_player_defensive_actions 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS match_player_uuid uuid;
UPDATE match_player_defensive_actions SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_player_defensive_actions ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_player_defensive_actions_uuid ON match_player_defensive_actions(uuid);

ALTER TABLE match_player_possession 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS match_player_uuid uuid;
UPDATE match_player_possession SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_player_possession ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_player_possession_uuid ON match_player_possession(uuid);

ALTER TABLE match_player_misc 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS match_player_uuid uuid;
UPDATE match_player_misc SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_player_misc ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_player_misc_uuid ON match_player_misc(uuid);

ALTER TABLE match_player_summary 
    ADD COLUMN IF NOT EXISTS uuid uuid DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS match_uuid uuid,
    ADD COLUMN IF NOT EXISTS match_player_uuid uuid,
    ADD COLUMN IF NOT EXISTS team_uuid uuid;
UPDATE match_player_summary SET uuid = gen_random_uuid() WHERE uuid IS NULL;
ALTER TABLE match_player_summary ALTER COLUMN uuid SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_player_summary_uuid ON match_player_summary(uuid);

-- =====================================================
-- 2. POPULATE UUID FOREIGN KEY REFERENCES
-- =====================================================

-- Update match table UUID references
UPDATE match m
SET 
    home_team_uuid = t.id,
    away_team_uuid = t2.id,
    season_uuid = s.id
FROM team t, team t2, season s
WHERE m.home_team_id = t.team_id
    AND m.away_team_id = t2.team_id
    AND m.season_id = s.season_id;

-- Update match_player_summary UUID references
UPDATE match_player_summary mps
SET 
    player_uuid = p.id,
    season_uuid = s.id,
    match_uuid = m.uuid,
    match_player_uuid = mp.uuid,
    team_uuid = t.id
FROM player p, season s, match m, match_player mp, team t
WHERE mps.player_id = p.player_id
    AND mps.season_id = s.season_id
    AND mps.match_id = m.match_id
    AND mps.match_player_id = mp.match_player_id
    AND mps.team_id = t.team_id;

-- Update match_lineup UUID references
UPDATE match_lineup ml
SET 
    player_uuid = p.id,
    team_uuid = t.id
FROM player p, team t
WHERE ml.player_id = p.player_id
    AND ml.team_id = t.team_id;

-- Update match_goalkeeper_summary UUID references
UPDATE match_goalkeeper_summary mgs
SET player_uuid = p.id
FROM player p
WHERE mgs.player_id = p.player_id;

-- Update match_shot UUID references
UPDATE match_shot ms
SET player_uuid = p.id
FROM player p
WHERE ms.player_id = p.player_id;

-- Update match_venue_weather UUID references
UPDATE match_venue_weather mvw
SET 
    match_uuid = m.uuid,
    venue_uuid = v.id
FROM match m, venue v
WHERE mvw.match_id = m.match_id
    AND mvw.venue_id = v.venue_id;

-- Update team_season UUID references
UPDATE team_season ts
SET 
    team_uuid = t.id,
    season_uuid = s.id
FROM team t, season s
WHERE ts.team_id = t.team_id
    AND ts.season_id = s.season_id;

-- Update player_season UUID references
UPDATE player_season ps
SET 
    player_uuid = p.id,
    season_uuid = s.id
FROM player p, season s
WHERE ps.player_id = p.player_id
    AND ps.season_id = s.season_id;

-- Update match_player detail tables UUID references
UPDATE match_player_passing mpp
SET match_player_uuid = mp.uuid
FROM match_player mp
WHERE mpp.match_player_id = mp.match_player_id;

UPDATE match_player_pass_types mppt
SET match_player_uuid = mp.uuid
FROM match_player mp
WHERE mppt.match_player_id = mp.match_player_id;

UPDATE match_player_defensive_actions mpda
SET match_player_uuid = mp.uuid
FROM match_player mp
WHERE mpda.match_player_id = mp.match_player_id;

UPDATE match_player_possession mpp
SET match_player_uuid = mp.uuid
FROM match_player mp
WHERE mpp.match_player_id = mp.match_player_id;

UPDATE match_player_misc mpm
SET match_player_uuid = mp.uuid
FROM match_player mp
WHERE mpm.match_player_id = mp.match_player_id;

-- Update match_subtype UUID references
UPDATE match_subtype ms
SET match_type_uuid = mt.uuid
FROM match_type mt
WHERE ms.match_type_id = mt.match_type_id;

-- Update team_record_regular_season UUID references
UPDATE team_record_regular_season trrs
SET 
    team_season_uuid = ts.uuid,
    season_uuid = s.id
FROM team_season ts, season s
WHERE trrs.team_season_id = ts.team_season_id
    AND trrs.season_id = s.season_id;

-- =====================================================
-- 3. CREATE UUID-BASED FOREIGN KEY CONSTRAINTS
-- =====================================================

-- Add UUID-based foreign key constraints (in addition to text-based ones)
-- This provides flexibility to use either key type for joins

-- match table UUID foreign keys
ALTER TABLE match
    ADD CONSTRAINT IF NOT EXISTS match_home_team_uuid_fkey 
        FOREIGN KEY (home_team_uuid) REFERENCES team(id),
    ADD CONSTRAINT IF NOT EXISTS match_away_team_uuid_fkey 
        FOREIGN KEY (away_team_uuid) REFERENCES team(id),
    ADD CONSTRAINT IF NOT EXISTS match_season_uuid_fkey 
        FOREIGN KEY (season_uuid) REFERENCES season(id);

-- match_player_summary UUID foreign keys
ALTER TABLE match_player_summary
    ADD CONSTRAINT IF NOT EXISTS mps_player_uuid_fkey 
        FOREIGN KEY (player_uuid) REFERENCES player(id),
    ADD CONSTRAINT IF NOT EXISTS mps_season_uuid_fkey 
        FOREIGN KEY (season_uuid) REFERENCES season(id),
    ADD CONSTRAINT IF NOT EXISTS mps_match_uuid_fkey 
        FOREIGN KEY (match_uuid) REFERENCES match(uuid),
    ADD CONSTRAINT IF NOT EXISTS mps_match_player_uuid_fkey 
        FOREIGN KEY (match_player_uuid) REFERENCES match_player(uuid),
    ADD CONSTRAINT IF NOT EXISTS mps_team_uuid_fkey 
        FOREIGN KEY (team_uuid) REFERENCES team(id);

-- Continue with other tables...
-- (Similar pattern for all other UUID foreign keys)

-- =====================================================
-- 4. CREATE PERFORMANCE INDEXES ON UUID COLUMNS
-- =====================================================

-- Indexes on UUID foreign key columns for join performance
CREATE INDEX IF NOT EXISTS idx_match_home_team_uuid ON match(home_team_uuid);
CREATE INDEX IF NOT EXISTS idx_match_away_team_uuid ON match(away_team_uuid);
CREATE INDEX IF NOT EXISTS idx_match_season_uuid ON match(season_uuid);

CREATE INDEX IF NOT EXISTS idx_mps_player_uuid ON match_player_summary(player_uuid);
CREATE INDEX IF NOT EXISTS idx_mps_season_uuid ON match_player_summary(season_uuid);
CREATE INDEX IF NOT EXISTS idx_mps_match_uuid ON match_player_summary(match_uuid);
CREATE INDEX IF NOT EXISTS idx_mps_match_player_uuid ON match_player_summary(match_player_uuid);

-- Continue for all UUID foreign key columns...

COMMIT;

-- =====================================================
-- VERIFICATION
-- =====================================================

-- Count UUID columns added
SELECT table_name, COUNT(*) as uuid_columns
FROM information_schema.columns
WHERE table_schema = 'public'
    AND data_type = 'uuid'
GROUP BY table_name
ORDER BY table_name;

-- Verify UUID population
SELECT 
    'match' as table_name, 
    COUNT(*) as total_rows,
    COUNT(uuid) as rows_with_uuid
FROM match
UNION ALL
SELECT 'match_player', COUNT(*), COUNT(uuid) FROM match_player
UNION ALL
SELECT 'match_lineup', COUNT(*), COUNT(uuid) FROM match_lineup
UNION ALL
SELECT 'match_shot', COUNT(*), COUNT(uuid) FROM match_shot;