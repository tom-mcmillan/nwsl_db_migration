-- =====================================================
-- ROLLBACK SCRIPT FOR KEY STRUCTURE CHANGES
-- Use only if needed to revert optimizations
-- =====================================================

BEGIN;

-- =====================================================
-- ROLLBACK PHASE 2: UUID ARCHITECTURE
-- =====================================================

-- Drop UUID-based foreign key constraints
ALTER TABLE match
    DROP CONSTRAINT IF EXISTS match_home_team_uuid_fkey,
    DROP CONSTRAINT IF EXISTS match_away_team_uuid_fkey,
    DROP CONSTRAINT IF EXISTS match_season_uuid_fkey;

ALTER TABLE match_player_summary
    DROP CONSTRAINT IF EXISTS mps_player_uuid_fkey,
    DROP CONSTRAINT IF EXISTS mps_season_uuid_fkey,
    DROP CONSTRAINT IF EXISTS mps_match_uuid_fkey,
    DROP CONSTRAINT IF EXISTS mps_match_player_uuid_fkey,
    DROP CONSTRAINT IF EXISTS mps_team_uuid_fkey;

-- Drop UUID indexes
DROP INDEX IF EXISTS idx_match_uuid;
DROP INDEX IF EXISTS idx_match_player_uuid;
DROP INDEX IF EXISTS idx_match_lineup_uuid;
DROP INDEX IF EXISTS idx_match_shot_uuid;
DROP INDEX IF EXISTS idx_match_goalkeeper_summary_uuid;
DROP INDEX IF EXISTS idx_match_venue_weather_uuid;
DROP INDEX IF EXISTS idx_match_type_uuid;
DROP INDEX IF EXISTS idx_match_subtype_uuid;
DROP INDEX IF EXISTS idx_shot_outcome_uuid;
DROP INDEX IF EXISTS idx_player_season_uuid;
DROP INDEX IF EXISTS idx_team_season_uuid;
DROP INDEX IF EXISTS idx_team_record_regular_season_uuid;
DROP INDEX IF EXISTS idx_region_uuid;
DROP INDEX IF EXISTS idx_match_player_passing_uuid;
DROP INDEX IF EXISTS idx_match_player_pass_types_uuid;
DROP INDEX IF EXISTS idx_match_player_defensive_actions_uuid;
DROP INDEX IF EXISTS idx_match_player_possession_uuid;
DROP INDEX IF EXISTS idx_match_player_misc_uuid;
DROP INDEX IF EXISTS idx_match_player_summary_uuid;

-- Drop UUID foreign key indexes
DROP INDEX IF EXISTS idx_match_home_team_uuid;
DROP INDEX IF EXISTS idx_match_away_team_uuid;
DROP INDEX IF EXISTS idx_match_season_uuid;
DROP INDEX IF EXISTS idx_mps_player_uuid;
DROP INDEX IF EXISTS idx_mps_season_uuid;
DROP INDEX IF EXISTS idx_mps_match_uuid;
DROP INDEX IF EXISTS idx_mps_match_player_uuid;

-- Remove UUID columns from tables (CAUTION: Data loss)
ALTER TABLE match DROP COLUMN IF EXISTS uuid;
ALTER TABLE match_player DROP COLUMN IF EXISTS uuid;
ALTER TABLE match_lineup DROP COLUMN IF EXISTS uuid;
ALTER TABLE match_shot DROP COLUMN IF EXISTS uuid;
ALTER TABLE match_goalkeeper_summary DROP COLUMN IF EXISTS uuid;
ALTER TABLE match_venue_weather 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS match_uuid,
    DROP COLUMN IF EXISTS venue_uuid;
ALTER TABLE match_type DROP COLUMN IF EXISTS uuid;
ALTER TABLE match_subtype 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS match_type_uuid;
ALTER TABLE shot_outcome DROP COLUMN IF EXISTS uuid;
ALTER TABLE player_season 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS player_uuid,
    DROP COLUMN IF EXISTS season_uuid;
ALTER TABLE team_season 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS team_uuid,
    DROP COLUMN IF EXISTS season_uuid;
ALTER TABLE team_record_regular_season 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS team_season_uuid,
    DROP COLUMN IF EXISTS season_uuid;
ALTER TABLE region DROP COLUMN IF EXISTS uuid;
ALTER TABLE match_player_passing 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS match_player_uuid;
ALTER TABLE match_player_pass_types 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS match_player_uuid;
ALTER TABLE match_player_defensive_actions 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS match_player_uuid;
ALTER TABLE match_player_possession 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS match_player_uuid;
ALTER TABLE match_player_misc 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS match_player_uuid;
ALTER TABLE match_player_summary 
    DROP COLUMN IF EXISTS uuid,
    DROP COLUMN IF EXISTS match_uuid,
    DROP COLUMN IF EXISTS match_player_uuid,
    DROP COLUMN IF EXISTS team_uuid;

-- =====================================================
-- ROLLBACK PHASE 1: CRITICAL FIXES
-- =====================================================

-- Remove composite indexes
DROP INDEX IF EXISTS idx_match_season_date;
DROP INDEX IF EXISTS idx_match_player_summary_season_player;
DROP INDEX IF EXISTS idx_match_player_match_player;

-- Remove check constraints
ALTER TABLE match DROP CONSTRAINT IF EXISTS match_id_format_check;
ALTER TABLE player DROP CONSTRAINT IF EXISTS player_id_format_check;
ALTER TABLE team DROP CONSTRAINT IF EXISTS team_id_format_check;

-- Remove UUID generation defaults
ALTER TABLE nation ALTER COLUMN id DROP DEFAULT;
ALTER TABLE player ALTER COLUMN id DROP DEFAULT;
ALTER TABLE season ALTER COLUMN id DROP DEFAULT;
ALTER TABLE team ALTER COLUMN id DROP DEFAULT;
ALTER TABLE venue ALTER COLUMN id DROP DEFAULT;

-- Remove indexes added for foreign keys
DROP INDEX IF EXISTS idx_match_season_id;
DROP INDEX IF EXISTS idx_player_nation_id;
DROP INDEX IF EXISTS idx_match_team_match_id;
DROP INDEX IF EXISTS idx_match_team_team_id;
DROP INDEX IF EXISTS idx_match_team_team_season_id;

-- Remove match_team foreign key constraints
ALTER TABLE match_team
    DROP CONSTRAINT IF EXISTS match_team_match_id_fkey,
    DROP CONSTRAINT IF EXISTS match_team_team_id_fkey,
    DROP CONSTRAINT IF EXISTS match_team_team_season_id_fkey;

-- Remove match_team unique constraint
ALTER TABLE match_team 
    DROP CONSTRAINT IF EXISTS match_team_match_id_team_id_unique;

-- Remove match_team primary key and id column
ALTER TABLE match_team DROP CONSTRAINT IF EXISTS match_team_pkey;
ALTER TABLE match_team DROP COLUMN IF EXISTS id;

COMMIT;

-- =====================================================
-- VERIFICATION
-- =====================================================

-- Check that rollback was successful
SELECT 
    'Tables with UUID columns' as check_type,
    COUNT(DISTINCT table_name) as count
FROM information_schema.columns
WHERE table_schema = 'public'
    AND column_name = 'uuid'
UNION ALL
SELECT 
    'UUID generation defaults',
    COUNT(*)
FROM information_schema.columns
WHERE table_schema = 'public'
    AND data_type = 'uuid'
    AND column_default LIKE '%gen_random_uuid()%'
UNION ALL
SELECT 
    'Total indexes',
    COUNT(*)
FROM pg_indexes
WHERE schemaname = 'public';

-- Show match_team structure after rollback
\d match_team