-- =====================================================
-- CRITICAL KEY STRUCTURE FIXES
-- Must be applied before major data insertion
-- =====================================================

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

BEGIN;

-- =====================================================
-- 1. ADD PRIMARY KEY TO match_team TABLE
-- =====================================================

-- First, check if there's data and potential duplicates
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM match_team LIMIT 1) THEN
        RAISE NOTICE 'match_team table contains data - checking for duplicates';
        
        -- Check for duplicate combinations that would violate composite key
        IF EXISTS (
            SELECT match_id, team_id, COUNT(*)
            FROM match_team
            WHERE match_id IS NOT NULL AND team_id IS NOT NULL
            GROUP BY match_id, team_id
            HAVING COUNT(*) > 1
        ) THEN
            RAISE EXCEPTION 'Duplicate match_id/team_id combinations found in match_team';
        END IF;
    END IF;
END $$;

-- Add UUID column for primary key
ALTER TABLE match_team 
    ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid();

-- Populate UUIDs for existing records if any
UPDATE match_team 
SET id = gen_random_uuid() 
WHERE id IS NULL;

-- Make id NOT NULL and set as primary key
ALTER TABLE match_team 
    ALTER COLUMN id SET NOT NULL,
    ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- Add primary key constraint if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_name = 'match_team' 
        AND constraint_type = 'PRIMARY KEY'
    ) THEN
        ALTER TABLE match_team ADD PRIMARY KEY (id);
    END IF;
END $$;

-- Add unique constraint on match_id + team_id to prevent duplicates
ALTER TABLE match_team 
    ADD CONSTRAINT IF NOT EXISTS match_team_match_id_team_id_unique 
    UNIQUE (match_id, team_id);

-- =====================================================
-- 2. ADD MISSING FOREIGN KEY INDEXES
-- =====================================================

-- Critical: Index on match.season_id
CREATE INDEX IF NOT EXISTS idx_match_season_id 
    ON match(season_id);

-- Important: Index on player.nation_id  
CREATE INDEX IF NOT EXISTS idx_player_nation_id 
    ON player(nation_id);

-- Additional indexes for match_team foreign keys
CREATE INDEX IF NOT EXISTS idx_match_team_match_id 
    ON match_team(match_id);
    
CREATE INDEX IF NOT EXISTS idx_match_team_team_id 
    ON match_team(team_id);
    
CREATE INDEX IF NOT EXISTS idx_match_team_team_season_id 
    ON match_team(team_season_id);

-- =====================================================
-- 3. ADD UUID GENERATION DEFAULTS
-- =====================================================

-- nation table
ALTER TABLE nation 
    ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- player table  
ALTER TABLE player 
    ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- season table
ALTER TABLE season 
    ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- team table
ALTER TABLE team 
    ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- venue table
ALTER TABLE venue 
    ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- =====================================================
-- 4. ADD CHECK CONSTRAINTS FOR DATA INTEGRITY
-- =====================================================

-- Ensure FBref IDs follow expected format (alphanumeric with possible hyphens)
ALTER TABLE match 
    ADD CONSTRAINT IF NOT EXISTS match_id_format_check 
    CHECK (match_id ~ '^[a-zA-Z0-9\-]+$');

ALTER TABLE player 
    ADD CONSTRAINT IF NOT EXISTS player_id_format_check 
    CHECK (player_id ~ '^[a-zA-Z0-9\-]+$');

ALTER TABLE team 
    ADD CONSTRAINT IF NOT EXISTS team_id_format_check 
    CHECK (team_id ~ '^[a-zA-Z0-9\-]+$');

-- =====================================================
-- 5. OPTIMIZE EXISTING INDEXES
-- =====================================================

-- Drop redundant indexes (if any exist)
DROP INDEX IF EXISTS idx_17305_idx_match_player_match_id;
CREATE INDEX IF NOT EXISTS idx_match_player_match_id 
    ON match_player(match_id);

DROP INDEX IF EXISTS idx_17305_idx_match_player_player_id;
CREATE INDEX IF NOT EXISTS idx_match_player_player_id 
    ON match_player(player_id);

DROP INDEX IF EXISTS idx_17305_idx_match_player_season_id;
CREATE INDEX IF NOT EXISTS idx_match_player_season_id 
    ON match_player(season_id);

-- =====================================================
-- 6. ADD COMPOSITE INDEXES FOR COMMON QUERIES
-- =====================================================

-- Match queries often filter by season and date
CREATE INDEX IF NOT EXISTS idx_match_season_date 
    ON match(season_id, match_date);

-- Player summary queries filter by season and player
CREATE INDEX IF NOT EXISTS idx_match_player_summary_season_player 
    ON match_player_summary(season_id, player_id);

-- Match player queries by match and player
CREATE INDEX IF NOT EXISTS idx_match_player_match_player 
    ON match_player(match_id, player_id);

-- =====================================================
-- 7. FOREIGN KEY CONSTRAINTS FOR match_team
-- =====================================================

-- Add foreign key constraints if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_name = 'match_team' 
        AND constraint_name = 'match_team_match_id_fkey'
    ) THEN
        ALTER TABLE match_team 
            ADD CONSTRAINT match_team_match_id_fkey 
            FOREIGN KEY (match_id) REFERENCES match(match_id);
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_name = 'match_team' 
        AND constraint_name = 'match_team_team_id_fkey'
    ) THEN
        ALTER TABLE match_team 
            ADD CONSTRAINT match_team_team_id_fkey 
            FOREIGN KEY (team_id) REFERENCES team(team_id);
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE table_name = 'match_team' 
        AND constraint_name = 'match_team_team_season_id_fkey'
    ) THEN
        ALTER TABLE match_team 
            ADD CONSTRAINT match_team_team_season_id_fkey 
            FOREIGN KEY (team_season_id) REFERENCES team_season(team_season_id);
    END IF;
END $$;

COMMIT;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Verify all changes
SELECT 'PRIMARY KEYS' as check_type, COUNT(*) as count
FROM information_schema.table_constraints 
WHERE constraint_type = 'PRIMARY KEY' 
AND table_schema = 'public'
UNION ALL
SELECT 'FOREIGN KEYS', COUNT(*)
FROM information_schema.table_constraints 
WHERE constraint_type = 'FOREIGN KEY' 
AND table_schema = 'public'
UNION ALL
SELECT 'INDEXES', COUNT(*)
FROM pg_indexes 
WHERE schemaname = 'public'
UNION ALL
SELECT 'UUID DEFAULTS', COUNT(*)
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND data_type = 'uuid' 
AND column_default LIKE '%gen_random_uuid()%';

-- Show match_team structure
\d match_team

-- Show critical indexes
SELECT tablename, indexname 
FROM pg_indexes 
WHERE schemaname = 'public' 
AND tablename IN ('match', 'match_team', 'player')
ORDER BY tablename, indexname;