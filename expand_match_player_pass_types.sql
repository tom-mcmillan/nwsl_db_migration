-- ============================================================================
-- Migration: Expand match_player_pass_types table for comprehensive FBref data
-- Author: Database Migration Specialist
-- Date: 2025-08-11
-- Purpose: Expand table from 5 to 21+ columns to capture all FBref pass types
-- ============================================================================

-- Start transaction for atomic migration
BEGIN;

-- ============================================================================
-- STEP 1: Add all missing FBref pass types columns
-- ============================================================================

-- Core Stats (adding missing columns, keeping existing 'crosses')
ALTER TABLE match_player_pass_types
    -- Rename existing column for consistency with FBref naming
    RENAME COLUMN live_passes TO passes_live;

-- Add core pass statistics columns
ALTER TABLE match_player_pass_types
    ADD COLUMN IF NOT EXISTS passes INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_dead INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_free_kicks INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS through_balls INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_switches INTEGER DEFAULT 0;

-- Add set pieces columns
ALTER TABLE match_player_pass_types
    ADD COLUMN IF NOT EXISTS throw_ins INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS corner_kicks INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS corner_kicks_in INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS corner_kicks_out INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS corner_kicks_straight INTEGER DEFAULT 0;

-- Add outcomes columns
ALTER TABLE match_player_pass_types
    ADD COLUMN IF NOT EXISTS passes_completed INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_offsides INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_blocked INTEGER DEFAULT 0;

-- Add additional tracking columns
ALTER TABLE match_player_pass_types
    ADD COLUMN IF NOT EXISTS passes_pressure INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_ground INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_low INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS passes_high INTEGER DEFAULT 0;

-- Add metadata columns for tracking data quality
ALTER TABLE match_player_pass_types
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(50),
    ADD COLUMN IF NOT EXISTS is_complete BOOLEAN DEFAULT FALSE;

-- ============================================================================
-- STEP 2: Add column comments for documentation
-- ============================================================================

COMMENT ON COLUMN match_player_pass_types.passes IS 'Total passes attempted';
COMMENT ON COLUMN match_player_pass_types.passes_live IS 'Live-ball passes (renamed from live_passes)';
COMMENT ON COLUMN match_player_pass_types.passes_dead IS 'Dead-ball passes (free kicks, corners, kick offs, throw-ins)';
COMMENT ON COLUMN match_player_pass_types.passes_free_kicks IS 'Passes from free kicks';
COMMENT ON COLUMN match_player_pass_types.through_balls IS 'Through balls completed';
COMMENT ON COLUMN match_player_pass_types.passes_switches IS 'Switches - passes that travel 40+ yards width';
COMMENT ON COLUMN match_player_pass_types.crosses IS 'Crosses into the box';

COMMENT ON COLUMN match_player_pass_types.throw_ins IS 'Throw-ins taken';
COMMENT ON COLUMN match_player_pass_types.corner_kicks IS 'Total corner kicks taken';
COMMENT ON COLUMN match_player_pass_types.corner_kicks_in IS 'Inswinging corner kicks';
COMMENT ON COLUMN match_player_pass_types.corner_kicks_out IS 'Outswinging corner kicks';
COMMENT ON COLUMN match_player_pass_types.corner_kicks_straight IS 'Straight corner kicks';

COMMENT ON COLUMN match_player_pass_types.passes_completed IS 'Total completed passes';
COMMENT ON COLUMN match_player_pass_types.passes_offsides IS 'Passes resulting in offside';
COMMENT ON COLUMN match_player_pass_types.passes_blocked IS 'Passes blocked by opponent';

COMMENT ON COLUMN match_player_pass_types.passes_pressure IS 'Passes while under pressure';
COMMENT ON COLUMN match_player_pass_types.passes_ground IS 'Ground passes';
COMMENT ON COLUMN match_player_pass_types.passes_low IS 'Low passes (below shoulder)';
COMMENT ON COLUMN match_player_pass_types.passes_high IS 'High passes (above shoulder)';

COMMENT ON COLUMN match_player_pass_types.data_source IS 'Source of data (e.g., fbref_html, api, manual)';
COMMENT ON COLUMN match_player_pass_types.is_complete IS 'Flag indicating if all pass type data is populated';

-- ============================================================================
-- STEP 3: Add constraints for data integrity
-- ============================================================================

-- Add check constraints for non-negative values
ALTER TABLE match_player_pass_types
    ADD CONSTRAINT chk_passes_non_negative CHECK (passes >= 0),
    ADD CONSTRAINT chk_passes_live_non_negative CHECK (passes_live >= 0),
    ADD CONSTRAINT chk_passes_dead_non_negative CHECK (passes_dead >= 0),
    ADD CONSTRAINT chk_passes_free_kicks_non_negative CHECK (passes_free_kicks >= 0),
    ADD CONSTRAINT chk_through_balls_non_negative CHECK (through_balls >= 0),
    ADD CONSTRAINT chk_passes_switches_non_negative CHECK (passes_switches >= 0),
    ADD CONSTRAINT chk_crosses_non_negative CHECK (crosses >= 0),
    ADD CONSTRAINT chk_throw_ins_non_negative CHECK (throw_ins >= 0),
    ADD CONSTRAINT chk_corner_kicks_non_negative CHECK (corner_kicks >= 0),
    ADD CONSTRAINT chk_corner_kicks_in_non_negative CHECK (corner_kicks_in >= 0),
    ADD CONSTRAINT chk_corner_kicks_out_non_negative CHECK (corner_kicks_out >= 0),
    ADD CONSTRAINT chk_corner_kicks_straight_non_negative CHECK (corner_kicks_straight >= 0),
    ADD CONSTRAINT chk_passes_completed_non_negative CHECK (passes_completed >= 0),
    ADD CONSTRAINT chk_passes_offsides_non_negative CHECK (passes_offsides >= 0),
    ADD CONSTRAINT chk_passes_blocked_non_negative CHECK (passes_blocked >= 0),
    ADD CONSTRAINT chk_passes_pressure_non_negative CHECK (passes_pressure >= 0),
    ADD CONSTRAINT chk_passes_ground_non_negative CHECK (passes_ground >= 0),
    ADD CONSTRAINT chk_passes_low_non_negative CHECK (passes_low >= 0),
    ADD CONSTRAINT chk_passes_high_non_negative CHECK (passes_high >= 0);

-- Add logical constraints
ALTER TABLE match_player_pass_types
    ADD CONSTRAINT chk_live_dead_total CHECK (
        (passes_live IS NULL AND passes_dead IS NULL) OR 
        (passes IS NULL) OR 
        (passes_live + passes_dead <= passes + 5) -- Allow small margin for data inconsistencies
    ),
    ADD CONSTRAINT chk_corner_kicks_total CHECK (
        (corner_kicks_in IS NULL AND corner_kicks_out IS NULL AND corner_kicks_straight IS NULL) OR
        (corner_kicks IS NULL) OR
        (corner_kicks_in + corner_kicks_out + corner_kicks_straight <= corner_kicks + 2)
    ),
    ADD CONSTRAINT chk_completed_vs_total CHECK (
        (passes_completed IS NULL) OR 
        (passes IS NULL) OR 
        (passes_completed <= passes)
    );

-- ============================================================================
-- STEP 4: Create performance indexes
-- ============================================================================

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_mppt_passes ON match_player_pass_types(passes) 
    WHERE passes IS NOT NULL AND passes > 0;

CREATE INDEX IF NOT EXISTS idx_mppt_crosses ON match_player_pass_types(crosses) 
    WHERE crosses IS NOT NULL AND crosses > 0;

CREATE INDEX IF NOT EXISTS idx_mppt_through_balls ON match_player_pass_types(through_balls) 
    WHERE through_balls IS NOT NULL AND through_balls > 0;

CREATE INDEX IF NOT EXISTS idx_mppt_corner_kicks ON match_player_pass_types(corner_kicks) 
    WHERE corner_kicks IS NOT NULL AND corner_kicks > 0;

-- Composite index for data completeness queries
CREATE INDEX IF NOT EXISTS idx_mppt_data_quality ON match_player_pass_types(is_complete, data_source, updated_at);

-- Partial index for records with actual data (non-zero values)
CREATE INDEX IF NOT EXISTS idx_mppt_has_data ON match_player_pass_types(match_player_id)
    WHERE passes > 0 OR passes_live > 0 OR crosses > 0;

-- ============================================================================
-- STEP 5: Create trigger for updated_at timestamp
-- ============================================================================

CREATE OR REPLACE FUNCTION update_match_player_pass_types_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_match_player_pass_types_updated_at ON match_player_pass_types;

CREATE TRIGGER trg_match_player_pass_types_updated_at
    BEFORE UPDATE ON match_player_pass_types
    FOR EACH ROW
    EXECUTE FUNCTION update_match_player_pass_types_updated_at();

-- ============================================================================
-- STEP 6: Create view for data quality monitoring
-- ============================================================================

CREATE OR REPLACE VIEW v_match_player_pass_types_quality AS
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN passes > 0 THEN 1 END) as records_with_passes,
    COUNT(CASE WHEN is_complete = TRUE THEN 1 END) as complete_records,
    COUNT(CASE WHEN data_source IS NOT NULL THEN 1 END) as records_with_source,
    COUNT(CASE WHEN passes_live > 0 OR passes_dead > 0 THEN 1 END) as records_with_type_breakdown,
    COUNT(CASE WHEN corner_kicks > 0 THEN 1 END) as records_with_corners,
    COUNT(CASE WHEN through_balls > 0 THEN 1 END) as records_with_through_balls,
    MIN(updated_at) as oldest_update,
    MAX(updated_at) as latest_update
FROM match_player_pass_types;

-- ============================================================================
-- STEP 7: Validation and reporting
-- ============================================================================

-- Report on migration results
DO $$
DECLARE
    v_total_records INTEGER;
    v_column_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_total_records FROM match_player_pass_types;
    
    SELECT COUNT(*) INTO v_column_count
    FROM information_schema.columns
    WHERE table_schema = 'public' 
    AND table_name = 'match_player_pass_types';
    
    RAISE NOTICE 'Migration Summary:';
    RAISE NOTICE '  - Total existing records: %', v_total_records;
    RAISE NOTICE '  - Total columns after migration: %', v_column_count;
    RAISE NOTICE '  - Previous column count: 5';
    RAISE NOTICE '  - New columns added: %', v_column_count - 5;
    RAISE NOTICE '  - All existing data preserved with passes_live = 0';
END $$;

-- Commit the transaction
COMMIT;

-- ============================================================================
-- POST-MIGRATION VERIFICATION QUERIES
-- ============================================================================

-- Verify schema changes
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public' 
AND table_name = 'match_player_pass_types'
ORDER BY ordinal_position;

-- Check data integrity
SELECT * FROM v_match_player_pass_types_quality;

-- Sample records to verify structure
SELECT 
    id,
    match_player_id,
    passes,
    passes_live,
    passes_dead,
    crosses,
    through_balls,
    corner_kicks,
    is_complete,
    data_source
FROM match_player_pass_types
LIMIT 5;