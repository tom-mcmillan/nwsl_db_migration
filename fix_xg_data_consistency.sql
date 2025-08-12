-- ========================================================
-- FIX DATA CONSISTENCY BETWEEN MATCH, MATCH_TEAM_PERFORMANCE, AND MATCH_SHOT TABLES
-- ========================================================
-- Purpose: Fix xG inconsistencies and ensure data integrity across related tables
-- Author: Database Migration Specialist
-- Date: 2025-08-12

-- Start transaction for atomicity
BEGIN;

-- ========================================================
-- STEP 1: ANALYZE CURRENT DATA INCONSISTENCIES
-- ========================================================

-- Create temporary table to store xG calculations from match_shot
DROP TABLE IF EXISTS temp_xg_aggregates;
CREATE TEMP TABLE temp_xg_aggregates AS
WITH shot_aggregates AS (
    SELECT 
        ms.match_id,
        ms.team_name,
        COUNT(*) as shot_count,
        SUM(ms.xg)::numeric(4,2) as total_xg,
        COUNT(CASE WHEN ms.outcome = 'Goal' THEN 1 END) as goals_from_shots
    FROM match_shot ms
    WHERE ms.xg IS NOT NULL
    GROUP BY ms.match_id, ms.team_name
),
match_teams AS (
    SELECT 
        m.match_id,
        hts.team_name_season_1 as home_team_name,
        ats.team_name_season_1 as away_team_name,
        m.home_goals,
        m.away_goals,
        m.xg_home,
        m.xg_away,
        m.home_team_season_id,
        m.away_team_season_id
    FROM match m
    LEFT JOIN team_season hts ON m.home_team_season_id = hts.id
    LEFT JOIN team_season ats ON m.away_team_season_id = ats.id
)
SELECT 
    mt.match_id,
    mt.home_team_name,
    mt.away_team_name,
    mt.home_team_season_id,
    mt.away_team_season_id,
    COALESCE(home_shots.total_xg, 0) as home_xg_from_shots,
    COALESCE(away_shots.total_xg, 0) as away_xg_from_shots,
    mt.xg_home as current_match_xg_home,
    mt.xg_away as current_match_xg_away,
    mt.home_goals,
    mt.away_goals,
    COALESCE(home_shots.goals_from_shots, 0) as home_goals_from_shots,
    COALESCE(away_shots.goals_from_shots, 0) as away_goals_from_shots
FROM match_teams mt
LEFT JOIN shot_aggregates home_shots 
    ON mt.match_id = home_shots.match_id 
    AND mt.home_team_name = home_shots.team_name
LEFT JOIN shot_aggregates away_shots 
    ON mt.match_id = away_shots.match_id 
    AND mt.away_team_name = away_shots.team_name
WHERE home_shots.total_xg IS NOT NULL OR away_shots.total_xg IS NOT NULL;

-- Display data inconsistencies before fix
SELECT 
    COUNT(*) as total_matches_with_shots,
    COUNT(CASE WHEN home_xg_from_shots != current_match_xg_home THEN 1 END) as home_xg_mismatches,
    COUNT(CASE WHEN away_xg_from_shots != current_match_xg_away THEN 1 END) as away_xg_mismatches
FROM temp_xg_aggregates;

-- ========================================================
-- STEP 2: UPDATE MATCH_TEAM_PERFORMANCE XG VALUES
-- ========================================================

-- Update home team xG in match_team_performance
UPDATE match_team_performance mtp
SET 
    xg = agg.home_xg_from_shots,
    updated_at = CURRENT_TIMESTAMP
FROM temp_xg_aggregates agg
WHERE mtp.match_id = agg.match_id
    AND mtp.team_season_id = agg.home_team_season_id
    AND mtp.is_home = true
    AND (mtp.xg IS NULL OR mtp.xg != agg.home_xg_from_shots);

-- Update away team xG in match_team_performance
UPDATE match_team_performance mtp
SET 
    xg = agg.away_xg_from_shots,
    updated_at = CURRENT_TIMESTAMP
FROM temp_xg_aggregates agg
WHERE mtp.match_id = agg.match_id
    AND mtp.team_season_id = agg.away_team_season_id
    AND mtp.is_home = false
    AND (mtp.xg IS NULL OR mtp.xg != agg.away_xg_from_shots);

-- Alternative update method using team name matching for any missing records
UPDATE match_team_performance mtp
SET 
    xg = xbt.total_xg,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT 
        ms.match_id,
        ms.team_name,
        SUM(ms.xg)::numeric(4,2) as total_xg
    FROM match_shot ms
    WHERE ms.xg IS NOT NULL
    GROUP BY ms.match_id, ms.team_name
) xbt,
team_season ts
WHERE mtp.match_id = xbt.match_id
    AND mtp.team_season_id = ts.id
    AND (ts.team_name_season_1 = xbt.team_name OR ts.team_name_season_2 = xbt.team_name)
    AND mtp.xg IS NULL;

-- ========================================================
-- STEP 3: UPDATE MATCH TABLE XG VALUES
-- ========================================================

-- Update match table xG values from aggregated shot data
UPDATE match m
SET 
    xg_home = agg.home_xg_from_shots,
    xg_away = agg.away_xg_from_shots
FROM temp_xg_aggregates agg
WHERE m.match_id = agg.match_id
    AND (m.xg_home != agg.home_xg_from_shots OR m.xg_away != agg.away_xg_from_shots);

-- ========================================================
-- STEP 4: VALIDATE GOAL CONSISTENCY
-- ========================================================

-- Check for goal inconsistencies between match and match_team_performance
DROP TABLE IF EXISTS temp_goal_validation;
CREATE TEMP TABLE temp_goal_validation AS
SELECT 
    m.match_id,
    m.home_goals as match_home_goals,
    m.away_goals as match_away_goals,
    home_mtp.goals as mtp_home_goals,
    away_mtp.goals as mtp_away_goals,
    CASE 
        WHEN m.home_goals != home_mtp.goals THEN 'HOME_MISMATCH'
        WHEN m.away_goals != away_mtp.goals THEN 'AWAY_MISMATCH'
        ELSE 'OK'
    END as validation_status
FROM match m
LEFT JOIN match_team_performance home_mtp 
    ON m.match_id = home_mtp.match_id 
    AND home_mtp.is_home = true
LEFT JOIN match_team_performance away_mtp 
    ON m.match_id = away_mtp.match_id 
    AND away_mtp.is_home = false
WHERE home_mtp.match_id IS NOT NULL AND away_mtp.match_id IS NOT NULL;

-- Fix goal inconsistencies (prioritize match table as source of truth)
UPDATE match_team_performance mtp
SET 
    goals = m.home_goals,
    updated_at = CURRENT_TIMESTAMP
FROM match m
WHERE mtp.match_id = m.match_id
    AND mtp.is_home = true
    AND mtp.goals != m.home_goals;

UPDATE match_team_performance mtp
SET 
    goals = m.away_goals,
    updated_at = CURRENT_TIMESTAMP
FROM match m
WHERE mtp.match_id = m.match_id
    AND mtp.is_home = false
    AND mtp.goals != m.away_goals;

-- Update goals_against field
UPDATE match_team_performance mtp
SET 
    goals_against = m.away_goals,
    updated_at = CURRENT_TIMESTAMP
FROM match m
WHERE mtp.match_id = m.match_id
    AND mtp.is_home = true
    AND (mtp.goals_against IS NULL OR mtp.goals_against != m.away_goals);

UPDATE match_team_performance mtp
SET 
    goals_against = m.home_goals,
    updated_at = CURRENT_TIMESTAMP
FROM match m
WHERE mtp.match_id = m.match_id
    AND mtp.is_home = false
    AND (mtp.goals_against IS NULL OR mtp.goals_against != m.home_goals);

-- ========================================================
-- STEP 5: ENSURE 2 TEAM PERFORMANCE RECORDS PER MATCH
-- ========================================================

-- Identify matches with missing team performance records
DROP TABLE IF EXISTS temp_missing_team_records;
CREATE TEMP TABLE temp_missing_team_records AS
SELECT 
    m.match_id,
    m.home_team_season_id,
    m.away_team_season_id,
    COUNT(mtp.id) as team_record_count
FROM match m
LEFT JOIN match_team_performance mtp ON m.match_id = mtp.match_id
WHERE m.home_team_season_id IS NOT NULL 
    AND m.away_team_season_id IS NOT NULL
GROUP BY m.match_id, m.home_team_season_id, m.away_team_season_id
HAVING COUNT(mtp.id) != 2;

-- Display summary of missing records
SELECT 
    COUNT(*) as matches_with_incomplete_records,
    COUNT(CASE WHEN team_record_count = 0 THEN 1 END) as matches_with_no_records,
    COUNT(CASE WHEN team_record_count = 1 THEN 1 END) as matches_with_one_record
FROM temp_missing_team_records;

-- ========================================================
-- STEP 6: CREATE DATA VALIDATION VIEWS
-- ========================================================

-- Drop existing validation views if they exist
DROP VIEW IF EXISTS v_xg_consistency_check CASCADE;
DROP VIEW IF EXISTS v_goal_consistency_check CASCADE;
DROP VIEW IF EXISTS v_match_team_record_check CASCADE;

-- Create view for xG consistency validation
CREATE VIEW v_xg_consistency_check AS
WITH shot_xg AS (
    SELECT 
        ms.match_id,
        ms.team_name,
        SUM(ms.xg)::numeric(4,2) as shot_xg
    FROM match_shot ms
    WHERE ms.xg IS NOT NULL
    GROUP BY ms.match_id, ms.team_name
),
match_xg AS (
    SELECT 
        m.match_id,
        hts.team_name_season_1 as home_team_name,
        ats.team_name_season_1 as away_team_name,
        m.xg_home,
        m.xg_away
    FROM match m
    LEFT JOIN team_season hts ON m.home_team_season_id = hts.id
    LEFT JOIN team_season ats ON m.away_team_season_id = ats.id
),
team_perf_xg AS (
    SELECT 
        mtp.match_id,
        ts.team_name_season_1 as team_name,
        mtp.is_home,
        mtp.xg as team_perf_xg
    FROM match_team_performance mtp
    JOIN team_season ts ON mtp.team_season_id = ts.id
)
SELECT 
    mx.match_id,
    mx.home_team_name,
    mx.away_team_name,
    mx.xg_home as match_xg_home,
    home_shot.shot_xg as shot_xg_home,
    home_perf.team_perf_xg as team_perf_xg_home,
    mx.xg_away as match_xg_away,
    away_shot.shot_xg as shot_xg_away,
    away_perf.team_perf_xg as team_perf_xg_away,
    CASE 
        WHEN mx.xg_home = home_shot.shot_xg 
            AND mx.xg_home = home_perf.team_perf_xg 
            AND mx.xg_away = away_shot.shot_xg 
            AND mx.xg_away = away_perf.team_perf_xg 
        THEN 'CONSISTENT'
        ELSE 'INCONSISTENT'
    END as consistency_status
FROM match_xg mx
LEFT JOIN shot_xg home_shot 
    ON mx.match_id = home_shot.match_id 
    AND mx.home_team_name = home_shot.team_name
LEFT JOIN shot_xg away_shot 
    ON mx.match_id = away_shot.match_id 
    AND mx.away_team_name = away_shot.team_name
LEFT JOIN team_perf_xg home_perf 
    ON mx.match_id = home_perf.match_id 
    AND home_perf.is_home = true
LEFT JOIN team_perf_xg away_perf 
    ON mx.match_id = away_perf.match_id 
    AND away_perf.is_home = false
WHERE home_shot.shot_xg IS NOT NULL OR away_shot.shot_xg IS NOT NULL;

-- Create view for goal consistency validation
CREATE VIEW v_goal_consistency_check AS
SELECT 
    m.match_id,
    m.home_goals as match_home_goals,
    m.away_goals as match_away_goals,
    home_mtp.goals as team_perf_home_goals,
    away_mtp.goals as team_perf_away_goals,
    home_mtp.goals_against as team_perf_home_goals_against,
    away_mtp.goals_against as team_perf_away_goals_against,
    CASE 
        WHEN m.home_goals = home_mtp.goals 
            AND m.away_goals = away_mtp.goals
            AND m.home_goals = away_mtp.goals_against
            AND m.away_goals = home_mtp.goals_against
        THEN 'CONSISTENT'
        ELSE 'INCONSISTENT'
    END as consistency_status
FROM match m
LEFT JOIN match_team_performance home_mtp 
    ON m.match_id = home_mtp.match_id 
    AND home_mtp.is_home = true
LEFT JOIN match_team_performance away_mtp 
    ON m.match_id = away_mtp.match_id 
    AND away_mtp.is_home = false
WHERE home_mtp.match_id IS NOT NULL OR away_mtp.match_id IS NOT NULL;

-- Create view for match team record completeness check
CREATE VIEW v_match_team_record_check AS
SELECT 
    m.match_id,
    m.match_date,
    COUNT(mtp.id) as team_performance_records,
    CASE 
        WHEN COUNT(mtp.id) = 2 THEN 'COMPLETE'
        WHEN COUNT(mtp.id) = 1 THEN 'INCOMPLETE_ONE_RECORD'
        WHEN COUNT(mtp.id) = 0 THEN 'MISSING_BOTH_RECORDS'
        ELSE 'ERROR_TOO_MANY_RECORDS'
    END as record_status
FROM match m
LEFT JOIN match_team_performance mtp ON m.match_id = mtp.match_id
GROUP BY m.match_id, m.match_date;

-- ========================================================
-- STEP 7: ADD CONSTRAINTS FOR FUTURE DATA INTEGRITY
-- ========================================================

-- Add check constraint to ensure exactly 2 team performance records per match
-- (This is complex to enforce at DB level, so we'll create a trigger instead)

-- Create function to validate team performance record count
CREATE OR REPLACE FUNCTION check_match_team_performance_count()
RETURNS TRIGGER AS $$
DECLARE
    record_count INTEGER;
BEGIN
    -- Count existing records for this match
    SELECT COUNT(*) INTO record_count
    FROM match_team_performance
    WHERE match_id = NEW.match_id;
    
    -- If we're updating, don't count the current record
    IF TG_OP = 'UPDATE' THEN
        record_count := record_count;
    -- If we're inserting, check if we'd exceed 2 records
    ELSIF TG_OP = 'INSERT' THEN
        IF record_count >= 2 THEN
            RAISE EXCEPTION 'Match % already has 2 team performance records', NEW.match_id;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for match_team_performance
DROP TRIGGER IF EXISTS ensure_two_team_records ON match_team_performance;
CREATE TRIGGER ensure_two_team_records
    BEFORE INSERT OR UPDATE ON match_team_performance
    FOR EACH ROW
    EXECUTE FUNCTION check_match_team_performance_count();

-- Create function to validate xG consistency on updates
CREATE OR REPLACE FUNCTION validate_xg_consistency()
RETURNS TRIGGER AS $$
DECLARE
    shot_xg NUMERIC(4,2);
BEGIN
    -- Only validate if xg is being set
    IF NEW.xg IS NOT NULL THEN
        -- Calculate xG from shots for this team/match
        SELECT SUM(ms.xg)::numeric(4,2) INTO shot_xg
        FROM match_shot ms
        JOIN team_season ts ON 
            (ts.team_name_season_1 = ms.team_name OR ts.team_name_season_2 = ms.team_name)
        WHERE ms.match_id = NEW.match_id
            AND ts.id = NEW.team_season_id
            AND ms.xg IS NOT NULL;
        
        -- If we have shot data, warn if xG doesn't match
        IF shot_xg IS NOT NULL AND shot_xg != NEW.xg THEN
            RAISE WARNING 'xG value % does not match aggregated shot xG % for match % team %', 
                NEW.xg, shot_xg, NEW.match_id, NEW.team_season_id;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for xG validation
DROP TRIGGER IF EXISTS validate_xg_on_update ON match_team_performance;
CREATE TRIGGER validate_xg_on_update
    BEFORE INSERT OR UPDATE OF xg ON match_team_performance
    FOR EACH ROW
    EXECUTE FUNCTION validate_xg_consistency();

-- ========================================================
-- STEP 8: FINAL VALIDATION REPORT
-- ========================================================

-- Generate comprehensive validation report
WITH validation_summary AS (
    SELECT 
        'xG Consistency' as check_type,
        COUNT(*) as total_records,
        COUNT(CASE WHEN consistency_status = 'CONSISTENT' THEN 1 END) as consistent_records,
        COUNT(CASE WHEN consistency_status = 'INCONSISTENT' THEN 1 END) as inconsistent_records
    FROM v_xg_consistency_check
    
    UNION ALL
    
    SELECT 
        'Goal Consistency' as check_type,
        COUNT(*) as total_records,
        COUNT(CASE WHEN consistency_status = 'CONSISTENT' THEN 1 END) as consistent_records,
        COUNT(CASE WHEN consistency_status = 'INCONSISTENT' THEN 1 END) as inconsistent_records
    FROM v_goal_consistency_check
    
    UNION ALL
    
    SELECT 
        'Team Record Completeness' as check_type,
        COUNT(*) as total_records,
        COUNT(CASE WHEN record_status = 'COMPLETE' THEN 1 END) as consistent_records,
        COUNT(CASE WHEN record_status != 'COMPLETE' THEN 1 END) as inconsistent_records
    FROM v_match_team_record_check
)
SELECT 
    check_type,
    total_records,
    consistent_records,
    inconsistent_records,
    ROUND(100.0 * consistent_records / NULLIF(total_records, 0), 2) as consistency_percentage
FROM validation_summary
ORDER BY check_type;

-- Show sample of remaining inconsistencies (if any)
SELECT 
    'xG Inconsistencies' as issue_type,
    match_id,
    home_team_name,
    away_team_name,
    'Match: ' || COALESCE(match_xg_home::text, 'NULL') || '/' || COALESCE(match_xg_away::text, 'NULL') ||
    ', Shots: ' || COALESCE(shot_xg_home::text, 'NULL') || '/' || COALESCE(shot_xg_away::text, 'NULL') ||
    ', Team Perf: ' || COALESCE(team_perf_xg_home::text, 'NULL') || '/' || COALESCE(team_perf_xg_away::text, 'NULL') as details
FROM v_xg_consistency_check
WHERE consistency_status = 'INCONSISTENT'
LIMIT 5;

-- Clean up temporary tables
DROP TABLE IF EXISTS temp_xg_aggregates;
DROP TABLE IF EXISTS temp_goal_validation;
DROP TABLE IF EXISTS temp_missing_team_records;

-- Commit transaction
COMMIT;

-- ========================================================
-- POST-MIGRATION VALIDATION QUERIES
-- ========================================================

-- Query 1: Check xG consistency for a specific match
/*
SELECT * FROM v_xg_consistency_check 
WHERE match_id = '07c68416';
*/

-- Query 2: Find all matches with inconsistent goals
/*
SELECT * FROM v_goal_consistency_check 
WHERE consistency_status = 'INCONSISTENT'
ORDER BY match_id;
*/

-- Query 3: Find matches missing team performance records
/*
SELECT * FROM v_match_team_record_check 
WHERE record_status != 'COMPLETE'
ORDER BY match_date DESC;
*/

-- Query 4: Verify xG totals by season
/*
SELECT 
    m.season_id,
    COUNT(DISTINCT m.match_id) as matches,
    ROUND(AVG(m.xg_home)::numeric, 2) as avg_xg_home,
    ROUND(AVG(m.xg_away)::numeric, 2) as avg_xg_away,
    ROUND(AVG(mtp.xg)::numeric, 2) as avg_team_perf_xg
FROM match m
LEFT JOIN match_team_performance mtp ON m.match_id = mtp.match_id
GROUP BY m.season_id
ORDER BY m.season_id;
*/

-- Query 5: Compare goals from shots vs recorded goals
/*
WITH shot_goals AS (
    SELECT 
        match_id,
        team_name,
        COUNT(CASE WHEN outcome = 'Goal' THEN 1 END) as goals_from_shots
    FROM match_shot
    GROUP BY match_id, team_name
)
SELECT 
    m.match_id,
    hts.team_name_season_1 as home_team_name,
    m.home_goals as recorded_goals,
    sg.goals_from_shots,
    CASE 
        WHEN m.home_goals = sg.goals_from_shots THEN 'MATCH'
        ELSE 'MISMATCH'
    END as validation
FROM match m
LEFT JOIN team_season hts ON m.home_team_season_id = hts.id
LEFT JOIN shot_goals sg 
    ON m.match_id = sg.match_id 
    AND hts.team_name_season_1 = sg.team_name
WHERE sg.goals_from_shots IS NOT NULL
    AND m.home_goals != sg.goals_from_shots
LIMIT 10;
*/