-- =====================================================
-- ANALYTICAL QUERY EXAMPLES
-- Demonstrating the optimized structure for team performance analysis
-- =====================================================

-- 1. Team Performance Summary by Season
SELECT 
    ts.team_name_season_1 as team_name,
    mts.season_id,
    COUNT(*) as matches_played,
    SUM(CASE WHEN is_home THEN 1 ELSE 0 END) as home_matches,
    SUM(CASE WHEN NOT is_home THEN 1 ELSE 0 END) as away_matches,
    ROUND(AVG(goals), 2) as avg_goals_for,
    ROUND(AVG(goals_against), 2) as avg_goals_against,
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
    SUM(CASE WHEN result = 'W' THEN 3 WHEN result = 'D' THEN 1 ELSE 0 END) as points
FROM match_team_stats mts
JOIN team_season ts ON mts.team_season_id = ts.id
WHERE mts.season_id = 2024
GROUP BY ts.team_name_season_1, mts.season_id
ORDER BY points DESC, avg_goals_for DESC
LIMIT 10;

-- 2. Home vs Away Performance Analysis
WITH performance_split AS (
    SELECT 
        ts.team_name_season_1 as team_name,
        mts.is_home,
        COUNT(*) as matches,
        ROUND(AVG(goals), 2) as avg_goals,
        ROUND(AVG(possession_pct), 1) as avg_possession,
        SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
        ROUND(SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1) as win_pct
    FROM match_team_stats mts
    JOIN team_season ts ON mts.team_season_id = ts.id
    WHERE mts.season_id = 2024
    GROUP BY ts.team_name_season_1, mts.is_home
)
SELECT 
    team_name,
    MAX(CASE WHEN is_home THEN matches END) as home_matches,
    MAX(CASE WHEN is_home THEN avg_goals END) as home_avg_goals,
    MAX(CASE WHEN is_home THEN win_pct END) as home_win_pct,
    MAX(CASE WHEN NOT is_home THEN matches END) as away_matches,
    MAX(CASE WHEN NOT is_home THEN avg_goals END) as away_avg_goals,
    MAX(CASE WHEN NOT is_home THEN win_pct END) as away_win_pct,
    ROUND(MAX(CASE WHEN is_home THEN win_pct END) - MAX(CASE WHEN NOT is_home THEN win_pct END), 1) as home_advantage
FROM performance_split
GROUP BY team_name
ORDER BY home_advantage DESC
LIMIT 10;

-- 3. Head-to-Head Records
WITH h2h AS (
    SELECT 
        ts1.team_name_season_1 as team,
        ts2.team_name_season_1 as opponent,
        mts.result,
        mts.goals,
        mts.goals_against,
        mts.match_date,
        mts.is_home
    FROM match_team_stats mts
    JOIN team_season ts1 ON mts.team_season_id = ts1.id
    JOIN team_season ts2 ON mts.opponent_team_season_id = ts2.id
    WHERE mts.season_id = 2024
)
SELECT 
    team,
    opponent,
    COUNT(*) as matches_played,
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
    SUM(goals) as goals_for,
    SUM(goals_against) as goals_against,
    SUM(goals) - SUM(goals_against) as goal_difference
FROM h2h
WHERE team = 'Orlando Pride'
GROUP BY team, opponent
ORDER BY wins DESC, goal_difference DESC;

-- 4. Best Possession Teams
SELECT 
    ts.team_name_season_1 as team_name,
    COUNT(*) FILTER (WHERE possession_pct IS NOT NULL) as matches_with_data,
    ROUND(AVG(possession_pct), 1) as avg_possession,
    ROUND(AVG(passing_acc_pct), 1) as avg_pass_accuracy,
    ROUND(AVG(touches), 0) as avg_touches,
    ROUND(AVG(CASE WHEN result = 'W' THEN possession_pct END), 1) as possession_when_winning,
    ROUND(AVG(CASE WHEN result = 'L' THEN possession_pct END), 1) as possession_when_losing
FROM match_team_stats mts
JOIN team_season ts ON mts.team_season_id = ts.id
WHERE mts.season_id >= 2023
    AND possession_pct IS NOT NULL
GROUP BY ts.team_name_season_1
HAVING COUNT(*) FILTER (WHERE possession_pct IS NOT NULL) >= 10
ORDER BY avg_possession DESC
LIMIT 10;

-- 5. Defensive Performance Metrics
SELECT 
    ts.team_name_season_1 as team_name,
    mts.season_id,
    COUNT(*) as matches,
    ROUND(AVG(goals_against), 2) as avg_goals_against,
    ROUND(AVG(saves_pct), 1) as avg_save_pct,
    ROUND(AVG(tackles), 1) as avg_tackles,
    ROUND(AVG(interceptions), 1) as avg_interceptions,
    ROUND(AVG(clearances), 1) as avg_clearances,
    SUM(CASE WHEN goals_against = 0 THEN 1 ELSE 0 END) as clean_sheets,
    ROUND(SUM(CASE WHEN goals_against = 0 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 1) as clean_sheet_pct
FROM match_team_stats mts
JOIN team_season ts ON mts.team_season_id = ts.id
WHERE mts.season_id = 2024
GROUP BY ts.team_name_season_1, mts.season_id
ORDER BY avg_goals_against ASC, clean_sheet_pct DESC
LIMIT 10;

-- 6. Match Type Performance
SELECT 
    match_type_name,
    match_subtype_name,
    COUNT(*) as matches,
    ROUND(AVG(goals), 2) as avg_goals,
    ROUND(AVG(possession_pct), 1) as avg_possession,
    SUM(CASE WHEN result = 'W' AND is_home THEN 1 ELSE 0 END) as home_wins,
    SUM(CASE WHEN result = 'W' AND NOT is_home THEN 1 ELSE 0 END) as away_wins,
    ROUND(SUM(CASE WHEN result = 'W' AND is_home THEN 1 ELSE 0 END)::numeric / 
          NULLIF(SUM(CASE WHEN is_home THEN 1 ELSE 0 END), 0) * 100, 1) as home_win_pct
FROM match_team_stats
WHERE season_id >= 2023
GROUP BY match_type_name, match_subtype_name
ORDER BY matches DESC;

-- 7. Recent Form (Last 5 matches per team)
WITH recent_matches AS (
    SELECT 
        ts.team_name_season_1 as team_name,
        mts.match_date,
        mts.result,
        mts.goals,
        mts.goals_against,
        mts.is_home,
        ROW_NUMBER() OVER (PARTITION BY mts.team_season_id ORDER BY mts.match_date DESC) as match_rank
    FROM match_team_stats mts
    JOIN team_season ts ON mts.team_season_id = ts.id
    WHERE mts.season_id = 2024
)
SELECT 
    team_name,
    STRING_AGG(result, '' ORDER BY match_date DESC) as last_5_results,
    SUM(CASE WHEN result = 'W' THEN 3 WHEN result = 'D' THEN 1 ELSE 0 END) as last_5_points,
    SUM(goals) as last_5_goals_for,
    SUM(goals_against) as last_5_goals_against
FROM recent_matches
WHERE match_rank <= 5
GROUP BY team_name
ORDER BY last_5_points DESC, (SUM(goals) - SUM(goals_against)) DESC;