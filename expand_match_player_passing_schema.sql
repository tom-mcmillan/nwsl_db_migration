-- =====================================================
-- Migration: Expand match_player_passing Table
-- Purpose: Capture all FBref passing statistics
-- Author: PostgreSQL Migration Specialist
-- Date: 2025-08-11
-- =====================================================

-- Start transaction for atomic migration
BEGIN;

-- Add migration tracking comment
COMMENT ON TABLE match_player_passing IS 
'Comprehensive passing statistics from FBref match data. Expanded schema to capture 28+ passing metrics.';

-- =====================================================
-- STEP 1: Add Basic Passing Columns
-- =====================================================

-- Rename existing columns for consistency with FBref naming
ALTER TABLE match_player_passing 
    RENAME COLUMN total_completed TO passes_completed;

ALTER TABLE match_player_passing 
    RENAME COLUMN total_attempted TO passes;

-- Add missing basic columns
ALTER TABLE match_player_passing 
    ADD COLUMN IF NOT EXISTS passes_pct DECIMAL(5,2)
    CONSTRAINT chk_passes_pct CHECK (passes_pct >= 0 AND passes_pct <= 100);

-- =====================================================
-- STEP 2: Add Distance-based Passing Columns
-- =====================================================

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_total_distance INTEGER
    CONSTRAINT chk_passes_total_distance CHECK (passes_total_distance >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_progressive_distance INTEGER
    CONSTRAINT chk_passes_progressive_distance CHECK (passes_progressive_distance >= 0);

-- =====================================================
-- STEP 3: Add Short Pass Columns (5-15 yards)
-- =====================================================

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_completed_short INTEGER
    CONSTRAINT chk_passes_completed_short CHECK (passes_completed_short >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_short INTEGER
    CONSTRAINT chk_passes_short CHECK (passes_short >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_pct_short DECIMAL(5,2)
    CONSTRAINT chk_passes_pct_short CHECK (passes_pct_short >= 0 AND passes_pct_short <= 100);

-- =====================================================
-- STEP 4: Add Medium Pass Columns (15-30 yards)
-- =====================================================

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_completed_medium INTEGER
    CONSTRAINT chk_passes_completed_medium CHECK (passes_completed_medium >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_medium INTEGER
    CONSTRAINT chk_passes_medium CHECK (passes_medium >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_pct_medium DECIMAL(5,2)
    CONSTRAINT chk_passes_pct_medium CHECK (passes_pct_medium >= 0 AND passes_pct_medium <= 100);

-- =====================================================
-- STEP 5: Add Long Pass Columns (30+ yards)
-- =====================================================

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_completed_long INTEGER
    CONSTRAINT chk_passes_completed_long CHECK (passes_completed_long >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_long INTEGER
    CONSTRAINT chk_passes_long CHECK (passes_long >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_pct_long DECIMAL(5,2)
    CONSTRAINT chk_passes_pct_long CHECK (passes_pct_long >= 0 AND passes_pct_long <= 100);

-- =====================================================
-- STEP 6: Add Advanced Passing Metrics
-- =====================================================

-- Expected goals metrics
ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS xg_assist DECIMAL(5,3)
    CONSTRAINT chk_xg_assist CHECK (xg_assist >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS pass_xa DECIMAL(5,3)
    CONSTRAINT chk_pass_xa CHECK (pass_xa >= 0);

-- Key pass metrics
ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS key_passes INTEGER
    CONSTRAINT chk_key_passes CHECK (key_passes >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS assisted_shots INTEGER
    CONSTRAINT chk_assisted_shots CHECK (assisted_shots >= 0);

-- =====================================================
-- STEP 7: Add Positioning/Zone Passing Columns
-- =====================================================

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_into_final_third INTEGER
    CONSTRAINT chk_passes_into_final_third CHECK (passes_into_final_third >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_into_penalty_area INTEGER
    CONSTRAINT chk_passes_into_penalty_area CHECK (passes_into_penalty_area >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS crosses_into_penalty_area INTEGER
    CONSTRAINT chk_crosses_into_penalty_area CHECK (crosses_into_penalty_area >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS progressive_passes INTEGER
    CONSTRAINT chk_progressive_passes CHECK (progressive_passes >= 0);

-- =====================================================
-- STEP 8: Add Additional FBref Metrics
-- =====================================================

-- Pass types
ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_live INTEGER
    CONSTRAINT chk_passes_live CHECK (passes_live >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_dead INTEGER
    CONSTRAINT chk_passes_dead CHECK (passes_dead >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_free_kicks INTEGER
    CONSTRAINT chk_passes_free_kicks CHECK (passes_free_kicks >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS through_balls INTEGER
    CONSTRAINT chk_through_balls CHECK (through_balls >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_switches INTEGER
    CONSTRAINT chk_passes_switches CHECK (passes_switches >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS crosses INTEGER
    CONSTRAINT chk_crosses CHECK (crosses >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS throw_ins INTEGER
    CONSTRAINT chk_throw_ins CHECK (throw_ins >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS corner_kicks INTEGER
    CONSTRAINT chk_corner_kicks CHECK (corner_kicks >= 0);

-- Outcome metrics
ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_offsides INTEGER
    CONSTRAINT chk_passes_offsides CHECK (passes_offsides >= 0);

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS passes_blocked INTEGER
    CONSTRAINT chk_passes_blocked CHECK (passes_blocked >= 0);

-- =====================================================
-- STEP 9: Add Metadata Column
-- =====================================================

ALTER TABLE match_player_passing
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW();

-- =====================================================
-- STEP 10: Create Optimized Indexes
-- =====================================================

-- Drop existing redundant index if exists
DROP INDEX IF EXISTS idx_match_player_passing_mp;

-- Create composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_mpp_match_player_assists 
    ON match_player_passing(match_player_id, assists) 
    WHERE assists > 0;

CREATE INDEX IF NOT EXISTS idx_mpp_xg_metrics 
    ON match_player_passing(match_player_id, xg_assist, pass_xa) 
    WHERE xg_assist IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mpp_progressive 
    ON match_player_passing(match_player_id, progressive_passes, passes_progressive_distance) 
    WHERE progressive_passes > 0;

CREATE INDEX IF NOT EXISTS idx_mpp_key_passes 
    ON match_player_passing(match_player_id, key_passes, assisted_shots) 
    WHERE key_passes > 0;

-- =====================================================
-- STEP 11: Add Table Constraints
-- =====================================================

-- Ensure data consistency with check constraints
ALTER TABLE match_player_passing
    ADD CONSTRAINT chk_pass_completion_consistency 
    CHECK (
        (passes IS NULL AND passes_completed IS NULL) OR 
        (passes >= passes_completed AND passes_completed >= 0)
    );

ALTER TABLE match_player_passing
    ADD CONSTRAINT chk_short_pass_consistency 
    CHECK (
        (passes_short IS NULL AND passes_completed_short IS NULL) OR 
        (passes_short >= passes_completed_short AND passes_completed_short >= 0)
    );

ALTER TABLE match_player_passing
    ADD CONSTRAINT chk_medium_pass_consistency 
    CHECK (
        (passes_medium IS NULL AND passes_completed_medium IS NULL) OR 
        (passes_medium >= passes_completed_medium AND passes_completed_medium >= 0)
    );

ALTER TABLE match_player_passing
    ADD CONSTRAINT chk_long_pass_consistency 
    CHECK (
        (passes_long IS NULL AND passes_completed_long IS NULL) OR 
        (passes_long >= passes_completed_long AND passes_completed_long >= 0)
    );

-- =====================================================
-- STEP 12: Add Column Comments for Documentation
-- =====================================================

COMMENT ON COLUMN match_player_passing.passes_completed IS 'Total number of successful passes';
COMMENT ON COLUMN match_player_passing.passes IS 'Total number of pass attempts';
COMMENT ON COLUMN match_player_passing.passes_pct IS 'Pass completion percentage';
COMMENT ON COLUMN match_player_passing.assists IS 'Number of assists (passes leading directly to goals)';

COMMENT ON COLUMN match_player_passing.passes_total_distance IS 'Total distance of all passes in yards';
COMMENT ON COLUMN match_player_passing.passes_progressive_distance IS 'Total progressive distance of passes in yards';

COMMENT ON COLUMN match_player_passing.passes_completed_short IS 'Completed passes 5-15 yards';
COMMENT ON COLUMN match_player_passing.passes_short IS 'Attempted passes 5-15 yards';
COMMENT ON COLUMN match_player_passing.passes_pct_short IS 'Short pass completion percentage';

COMMENT ON COLUMN match_player_passing.passes_completed_medium IS 'Completed passes 15-30 yards';
COMMENT ON COLUMN match_player_passing.passes_medium IS 'Attempted passes 15-30 yards';
COMMENT ON COLUMN match_player_passing.passes_pct_medium IS 'Medium pass completion percentage';

COMMENT ON COLUMN match_player_passing.passes_completed_long IS 'Completed passes 30+ yards';
COMMENT ON COLUMN match_player_passing.passes_long IS 'Attempted passes 30+ yards';
COMMENT ON COLUMN match_player_passing.passes_pct_long IS 'Long pass completion percentage';

COMMENT ON COLUMN match_player_passing.xg_assist IS 'Expected goals value of assists';
COMMENT ON COLUMN match_player_passing.pass_xa IS 'Expected assists from passes';
COMMENT ON COLUMN match_player_passing.key_passes IS 'Passes leading to shots';
COMMENT ON COLUMN match_player_passing.assisted_shots IS 'Number of shots created for teammates';

COMMENT ON COLUMN match_player_passing.passes_into_final_third IS 'Successful passes into attacking third';
COMMENT ON COLUMN match_player_passing.passes_into_penalty_area IS 'Successful passes into penalty box';
COMMENT ON COLUMN match_player_passing.crosses_into_penalty_area IS 'Crosses into penalty box';
COMMENT ON COLUMN match_player_passing.progressive_passes IS 'Passes moving ball significantly toward goal';

COMMENT ON COLUMN match_player_passing.passes_live IS 'Passes during live play';
COMMENT ON COLUMN match_player_passing.passes_dead IS 'Passes from dead ball situations';
COMMENT ON COLUMN match_player_passing.passes_free_kicks IS 'Passes from free kicks';
COMMENT ON COLUMN match_player_passing.through_balls IS 'Through ball attempts';
COMMENT ON COLUMN match_player_passing.passes_switches IS 'Switches of play';
COMMENT ON COLUMN match_player_passing.crosses IS 'Total crossing attempts';
COMMENT ON COLUMN match_player_passing.throw_ins IS 'Throw-ins taken';
COMMENT ON COLUMN match_player_passing.corner_kicks IS 'Corner kicks taken';

COMMENT ON COLUMN match_player_passing.passes_offsides IS 'Passes resulting in offside';
COMMENT ON COLUMN match_player_passing.passes_blocked IS 'Passes that were blocked';

-- =====================================================
-- STEP 13: Create Trigger for Updated Timestamp
-- =====================================================

-- Create function to update timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS update_match_player_passing_updated_at ON match_player_passing;
CREATE TRIGGER update_match_player_passing_updated_at
    BEFORE UPDATE ON match_player_passing
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- STEP 14: Verify Migration Success
-- =====================================================

-- Create verification query
DO $$
DECLARE
    col_count INTEGER;
    expected_cols INTEGER := 41; -- Total expected columns after migration
BEGIN
    SELECT COUNT(*) INTO col_count
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND table_name = 'match_player_passing';
    
    IF col_count < expected_cols THEN
        RAISE WARNING 'Column count is %, expected at least %', col_count, expected_cols;
    ELSE
        RAISE NOTICE 'Migration successful: % columns in match_player_passing', col_count;
    END IF;
END $$;

-- Commit the transaction
COMMIT;

-- =====================================================
-- Post-Migration Verification Queries
-- =====================================================

-- Show the new table structure
\d match_player_passing

-- Show column statistics
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public' 
AND table_name = 'match_player_passing'
ORDER BY ordinal_position;

-- Show all constraints
SELECT 
    con.conname AS constraint_name,
    con.contype AS constraint_type,
    pg_get_constraintdef(con.oid) AS definition
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
WHERE rel.relname = 'match_player_passing'
ORDER BY con.conname;

-- Show all indexes
SELECT 
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'match_player_passing'
ORDER BY indexname;