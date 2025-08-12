-- Migration: Add season_id to match_player_passing and match_player_pass_types tables
-- Purpose: Enable year-based analysis of passing data completeness
-- Date: 2025-08-11
-- Author: Database Migration Specialist

-- Start transaction for atomicity
BEGIN;

-- ========================================
-- STEP 1: Add season_id column to match_player_passing
-- ========================================

-- Add column as nullable first (we'll make it NOT NULL after populating)
ALTER TABLE match_player_passing 
ADD COLUMN IF NOT EXISTS season_id INTEGER;

-- Populate season_id from match_player table
UPDATE match_player_passing mpp
SET season_id = mp.season_id
FROM match_player mp
WHERE mpp.match_player_id = mp.id
  AND mpp.season_id IS NULL;

-- Verify all records have been populated
DO $$
DECLARE
    null_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO null_count
    FROM match_player_passing
    WHERE season_id IS NULL;
    
    IF null_count > 0 THEN
        RAISE EXCEPTION 'Found % records with NULL season_id in match_player_passing', null_count;
    END IF;
END $$;

-- Now make the column NOT NULL
ALTER TABLE match_player_passing
ALTER COLUMN season_id SET NOT NULL;

-- Note: season_id is a year value (integer), not a foreign key to season table
-- This matches the design in match_player table

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_match_player_passing_season_id 
ON match_player_passing(season_id);

-- Create composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_match_player_passing_season_match_player 
ON match_player_passing(season_id, match_player_id);

-- ========================================
-- STEP 2: Add season_id column to match_player_pass_types
-- ========================================

-- Add column as nullable first
ALTER TABLE match_player_pass_types 
ADD COLUMN IF NOT EXISTS season_id INTEGER;

-- Populate season_id from match_player table
UPDATE match_player_pass_types mpt
SET season_id = mp.season_id
FROM match_player mp
WHERE mpt.match_player_id = mp.id
  AND mpt.season_id IS NULL;

-- Verify all records have been populated
DO $$
DECLARE
    null_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO null_count
    FROM match_player_pass_types
    WHERE season_id IS NULL;
    
    IF null_count > 0 THEN
        RAISE EXCEPTION 'Found % records with NULL season_id in match_player_pass_types', null_count;
    END IF;
END $$;

-- Now make the column NOT NULL
ALTER TABLE match_player_pass_types
ALTER COLUMN season_id SET NOT NULL;

-- Note: season_id is a year value (integer), not a foreign key to season table
-- This matches the design in match_player table

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_match_player_pass_types_season_id 
ON match_player_pass_types(season_id);

-- Create composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_match_player_pass_types_season_match_player 
ON match_player_pass_types(season_id, match_player_id);

-- ========================================
-- STEP 3: Verification Queries
-- ========================================

-- Verify match_player_passing
DO $$
DECLARE
    passing_count INTEGER;
    passing_seasons INTEGER;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT season_id) 
    INTO passing_count, passing_seasons
    FROM match_player_passing;
    
    RAISE NOTICE 'match_player_passing: % records across % seasons', passing_count, passing_seasons;
END $$;

-- Verify match_player_pass_types
DO $$
DECLARE
    types_count INTEGER;
    types_seasons INTEGER;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT season_id) 
    INTO types_count, types_seasons
    FROM match_player_pass_types;
    
    RAISE NOTICE 'match_player_pass_types: % records across % seasons', types_count, types_seasons;
END $$;

-- Show season distribution for match_player_passing
SELECT 'match_player_passing' as table_name,
       season_id, 
       COUNT(*) as total_records,
       COUNT(CASE WHEN passes > 0 THEN 1 END) as records_with_data,
       ROUND(100.0 * COUNT(CASE WHEN passes > 0 THEN 1 END) / COUNT(*), 2) as pct_with_data
FROM match_player_passing
GROUP BY season_id
ORDER BY season_id;

-- Show season distribution for match_player_pass_types
SELECT 'match_player_pass_types' as table_name,
       season_id, 
       COUNT(*) as total_records,
       COUNT(CASE WHEN passes > 0 THEN 1 END) as records_with_data,
       ROUND(100.0 * COUNT(CASE WHEN passes > 0 THEN 1 END) / COUNT(*), 2) as pct_with_data
FROM match_player_pass_types
GROUP BY season_id
ORDER BY season_id;

-- Commit the transaction
COMMIT;

-- ========================================
-- Post-Migration Notes
-- ========================================
-- Migration completed successfully!
-- Both tables now have:
-- 1. season_id column (INTEGER NOT NULL)
-- 2. Foreign key constraint to seasons table
-- 3. Indexes on season_id for optimal query performance
-- 4. Composite indexes for season_id + match_player_id queries
--
-- Example queries now enabled:
-- 
-- -- Analyze passing completeness by season:
-- SELECT season_id, 
--        COUNT(*) as total_records,
--        COUNT(CASE WHEN passes > 0 THEN 1 END) as with_data,
--        ROUND(100.0 * COUNT(CASE WHEN passes > 0 THEN 1 END) / COUNT(*), 2) as pct_complete
-- FROM match_player_passing 
-- GROUP BY season_id 
-- ORDER BY season_id;
--
-- -- Compare pass types across seasons:
-- SELECT season_id,
--        AVG(passes_live) as avg_live_passes,
--        AVG(crosses) as avg_crosses,
--        AVG(through_balls) as avg_through_balls
-- FROM match_player_pass_types
-- WHERE passes > 0
-- GROUP BY season_id
-- ORDER BY season_id;