# NWSL Database Table Role Definitions

## Core Principle: Purpose-Driven Database Design

Each table should have a **clear analytical purpose** that drives its structure and content.

## Table Roles Defined

### **match** Table - MATCH METADATA MASTER
- **Purpose**: Core match facts and context
- **Contents**: Date, venue, season, competition, weather
- **Users**: All analysts (foundational data)
- **Key Question**: *"When and where did this match happen?"*

### **match_team** Table - TEAM PERFORMANCE AGGREGATES  
- **Purpose**: Team-level tactical and collective performance analysis
- **Contents**: 
  - **KEEP**: Results, possession, passing accuracy, team actions
  - **REMOVE**: Redundant fields (match_date, season_id)
  - **ADD**: Home/away context, opponent linkage
- **Users**: Coaches, tactical analysts, team performance evaluators
- **Key Questions**: 
  - *"How did the team play tactically?"*
  - *"What's our home vs away form?"*
  - *"Which teams control possession best?"*

### **match_player_summary** Table - INDIVIDUAL CONTRIBUTIONS
- **Purpose**: Individual player performance and development tracking
- **Contents**:
  - **KEEP**: Goals, assists, xG, playing time, position-specific stats
  - **REMOVE**: Redundant player_name (use JOIN to player table)
  - **IMPROVE**: Calculate age from player.dob instead of storing
- **Users**: Player scouts, fantasy analysts, individual performance evaluators  
- **Key Questions**:
  - *"Who are the top performers in each position?"*
  - *"How is player X developing this season?"*
  - *"Which players create the most chances?"*

## Analytical Use Case Mapping

### Team Performance Analysis → **match_team**
```sql
-- Example: Team's possession-based performance
SELECT team_name, AVG(possession_pct), AVG(passing_acc_pct), COUNT(CASE WHEN result = 'W' THEN 1 END)
FROM match_team mt JOIN team_season ts ON mt.team_season_id = ts.id
GROUP BY team_name;
```

### Player Performance Analysis → **match_player_summary** 
```sql
-- Example: Top scorers with efficiency
SELECT player_name, SUM(goals), SUM(minutes)/90 as matches_90, 
       SUM(goals)/(SUM(minutes)/90) as goals_per_90
FROM match_player_summary mps JOIN player p ON mps.player_uuid = p.id
GROUP BY player_name HAVING SUM(minutes) > 900;
```

### Combined Analysis → **BOTH tables**
```sql  
-- Example: Team success with star players
SELECT team_name, AVG(CASE WHEN mt.result = 'W' THEN 1 ELSE 0 END) as win_rate,
       COUNT(CASE WHEN mps.goals >= 2 THEN 1 END) as multi_goal_games
FROM match_team mt 
JOIN match_player_summary mps ON mt.match_team_id = mps.match_team_id
JOIN team_season ts ON mt.team_season_id = ts.id
GROUP BY team_name;
```

## Migration Strategy Based on Purpose

### **match_team** Migration Priority:
1. **Add missing context** - home/away, opponent linkage (critical for team analysis)
2. **Remove redundancy** - denormalized fields available via JOINs
3. **Optimize structure** - UUID PKs, proper FKs, column ordering
4. **Preserve FBref lineage** - for future data updates

### **match_player_summary** Migration Priority:
1. **Fix data types** - age calculation, proper numeric types
2. **Remove redundancy** - player names, duplicated match context
3. **Optimize joins** - better FK relationships
4. **Performance tuning** - indexes for common analytical queries

## Success Criteria

Each table should efficiently answer its core analytical questions:
- **match_team**: "How did the TEAM perform?" 
- **match_player_summary**: "How did individual PLAYERS contribute?"

The migration is successful when analysts can answer these questions with simple, performant queries that align with the table's defined purpose.