#!/usr/bin/env python3
"""
Extract missing goalkeeper data from FBref HTML files and insert into database.
Focuses on 96 missing records: 93 from 2015, 1 from 2014, 2 from 2021.
"""

import os
import re
import json
import psycopg2
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import uuid

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

HTML_DIR = '/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files'

def get_missing_matches():
    """Get list of matches missing goalkeeper data from database"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    query = """
        SELECT 
            m.match_id,
            s.season_year,
            m.match_date,
            m.home_team_season_id,
            m.away_team_season_id
        FROM match m
        LEFT JOIN season s ON m.season_uuid = s.id
        LEFT JOIN match_goalkeeper_performance mgp ON m.match_id = mgp.match_id
        WHERE mgp.match_id IS NULL
        ORDER BY s.season_year, m.match_date
    """
    
    cur.execute(query)
    missing_matches = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return missing_matches

def extract_goalkeeper_from_summary_table(table, team_id_from_table):
    """
    Extract goalkeeper data from a team summary table (2015 format).
    Returns list of goalkeeper data dictionaries.
    """
    goalkeepers = []
    
    try:
        # Convert table to DataFrame
        df = pd.read_html(StringIO(str(table)))[0]
        
        # Handle multi-level columns
        # Find position column
        pos_col = None
        player_col = None
        
        for col in df.columns:
            if 'Pos' in str(col):
                pos_col = col
            if 'Player' in str(col):
                player_col = col
        
        if not pos_col or not player_col:
            return goalkeepers
        
        # Filter for goalkeepers - handle different data types
        try:
            gk_mask = df[pos_col].str.contains('GK', na=False)
        except AttributeError:
            # If column is not string type, convert it first
            df[pos_col] = df[pos_col].astype(str)
            gk_mask = df[pos_col].str.contains('GK', na=False)
        gk_rows = df[gk_mask]
        
        if gk_rows.empty:
            return goalkeepers
        
        # Process each goalkeeper row
        for _, row in gk_rows.iterrows():
            gk_data = {
                'player_name': row[player_col],
                'team_fbref_id': team_id_from_table,
                'minutes_played': None,
                'performance_data': {}
            }
            
            # Extract minutes played
            for col in df.columns:
                if 'Min' in str(col):
                    try:
                        gk_data['minutes_played'] = int(row[col]) if pd.notna(row[col]) else None
                    except:
                        pass
            
            # Extract performance metrics
            for col in df.columns:
                col_str = str(col).lower()
                val = row[col]
                
                if pd.notna(val):
                    # Map common stats
                    if 'gls' in col_str or 'goals' in col_str:
                        gk_data['performance_data']['goals'] = val
                    elif 'ast' in col_str or 'assist' in col_str:
                        gk_data['performance_data']['assists'] = val
                    elif 'pk' in col_str and 'pkatt' not in col_str:
                        gk_data['performance_data']['pk_goals'] = val
                    elif 'pkatt' in col_str:
                        gk_data['performance_data']['pk_attempts'] = val
                    elif 'crdy' in col_str or 'yellow' in col_str:
                        gk_data['performance_data']['yellow_cards'] = val
                    elif 'crdr' in col_str or 'red' in col_str:
                        gk_data['performance_data']['red_cards'] = val
            
            # Extract player FBref ID from row attributes if available
            # The row element in BeautifulSoup contains data-append-csv with player ID
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                for html_row in rows:
                    # Check if this row matches our goalkeeper
                    player_cell = html_row.find('td', {'data-stat': 'player'}) or html_row.find('th', {'data-stat': 'player'})
                    if player_cell and player_cell.get_text(strip=True) == gk_data['player_name']:
                        # Get FBref player ID from data-append-csv
                        fbref_id = html_row.get('data-append-csv', '')
                        if fbref_id:
                            gk_data['player_fbref_id'] = fbref_id
                        
                        # Also try to get player link
                        player_link = player_cell.find('a')
                        if player_link:
                            href = player_link.get('href', '')
                            # Extract player ID from URL: /en/players/{id}/Player-Name
                            match = re.search(r'/players/([a-f0-9]+)/', href)
                            if match:
                                gk_data['player_fbref_id'] = match.group(1)
            
            goalkeepers.append(gk_data)
    
    except Exception as e:
        print(f"  Error extracting goalkeeper from summary table: {e}")
    
    return goalkeepers

def extract_goalkeeper_from_keeper_table(table, team_id_from_table):
    """
    Extract goalkeeper data from dedicated keeper stats table (newer format).
    Returns list of goalkeeper data dictionaries.
    """
    goalkeepers = []
    
    try:
        # Convert table to DataFrame
        df = pd.read_html(StringIO(str(table)))[0]
        
        # Process each row (each row is a goalkeeper)
        for idx, row in df.iterrows():
            gk_data = {
                'team_fbref_id': team_id_from_table,
                'performance_data': {}
            }
            
            # Extract player name
            for col in df.columns:
                if 'Player' in str(col):
                    gk_data['player_name'] = row[col]
                    break
            
            # Extract goalkeeper-specific stats
            for col in df.columns:
                col_str = str(col).lower()
                val = row[col]
                
                if pd.notna(val):
                    # Goalkeeper-specific metrics
                    if 'saves' in col_str or 'sv' in col_str:
                        gk_data['performance_data']['saves'] = val
                    elif 'ga' in col_str or 'goals_against' in col_str:
                        gk_data['performance_data']['goals_against'] = val
                    elif 'sota' in col_str or 'shots_on_target_against' in col_str:
                        gk_data['performance_data']['shots_on_target_against'] = val
                    elif 'save%' in col_str or 'sv%' in col_str:
                        gk_data['performance_data']['save_percentage'] = val
                    elif 'cs' in col_str or 'clean' in col_str:
                        gk_data['performance_data']['clean_sheets'] = val
                    elif 'psxg' in col_str:
                        gk_data['performance_data']['post_shot_xg'] = val
                    elif 'pksv' in col_str:
                        gk_data['performance_data']['pk_saves'] = val
                    elif 'min' in col_str:
                        try:
                            gk_data['minutes_played'] = int(val) if val else None
                        except:
                            pass
            
            # Extract FBref player ID from HTML
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                if idx < len(rows):
                    html_row = rows[idx]
                    fbref_id = html_row.get('data-append-csv', '')
                    if fbref_id:
                        gk_data['player_fbref_id'] = fbref_id
            
            if 'player_name' in gk_data:
                goalkeepers.append(gk_data)
    
    except Exception as e:
        print(f"  Error extracting from keeper table: {e}")
    
    return goalkeepers

def extract_goalkeepers_from_html(html_path, match_id):
    """
    Extract all goalkeeper data from an HTML file.
    Returns dictionary with home and away goalkeeper data.
    """
    print(f"\nProcessing {Path(html_path).name} for match {match_id}")
    
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all tables
    tables = soup.find_all('table')
    print(f"  Found {len(tables)} tables")
    
    result = {
        'match_id': match_id,
        'home_goalkeepers': [],
        'away_goalkeepers': [],
        'teams': {'home': None, 'away': None}
    }
    
    # Process each table
    for table in tables:
        table_id = table.get('id', '')
        
        # Check for goalkeeper-specific tables (newer format)
        if 'keeper_stats_' in table_id:
            team_id = table_id.replace('keeper_stats_', '')
            print(f"  Found keeper stats table for team {team_id}")
            gks = extract_goalkeeper_from_keeper_table(table, team_id)
            
            # Determine if home or away (we'll match later with database)
            if not result['teams']['home']:
                result['teams']['home'] = team_id
                result['home_goalkeepers'].extend(gks)
            else:
                result['teams']['away'] = team_id
                result['away_goalkeepers'].extend(gks)
        
        # Check for player summary tables (2015 format)
        elif 'stats_' in table_id and '_summary' in table_id:
            # Extract team ID from table ID: stats_{team_id}_summary
            match = re.search(r'stats_([a-f0-9]+)_summary', table_id)
            if match:
                team_id = match.group(1)
                print(f"  Found summary table for team {team_id}")
                gks = extract_goalkeeper_from_summary_table(table, team_id)
                
                if gks:
                    print(f"    Extracted {len(gks)} goalkeeper(s)")
                    # Determine if home or away
                    if not result['teams']['home']:
                        result['teams']['home'] = team_id
                        result['home_goalkeepers'].extend(gks)
                    else:
                        result['teams']['away'] = team_id
                        result['away_goalkeepers'].extend(gks)
    
    # Summary
    total_gks = len(result['home_goalkeepers']) + len(result['away_goalkeepers'])
    print(f"  Total goalkeepers extracted: {total_gks}")
    
    return result

def get_player_uuid(player_name, player_fbref_id=None):
    """
    Get or create player UUID from database.
    First tries to match by FBref ID, then by name.
    Note: In this database, player_id column contains the FBref hex ID
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    found_player_id = None  # This will be the FBref hex ID
    
    # First try to find by FBref ID if available
    if player_fbref_id:
        cur.execute("""
            SELECT player_id FROM player 
            WHERE player_id = %s
            LIMIT 1
        """, (player_fbref_id,))
        result = cur.fetchone()
        if result:
            found_player_id = result[0]
    
    # If not found by FBref ID, try by name
    if not found_player_id and player_name:
        # Clean name for matching
        clean_name = player_name.strip()
        
        # Try exact match first
        cur.execute("""
            SELECT player_id FROM player 
            WHERE player_name = %s
            LIMIT 1
        """, (clean_name,))
        result = cur.fetchone()
        
        if result:
            found_player_id = result[0]
        else:
            # Try case-insensitive match
            cur.execute("""
                SELECT player_id FROM player 
                WHERE LOWER(player_name) = LOWER(%s)
                LIMIT 1
            """, (clean_name,))
            result = cur.fetchone()
            if result:
                found_player_id = result[0]
    
    # If still not found, create new player with FBref ID as player_id
    if not found_player_id:
        # Use FBref ID if available, otherwise generate a hex-like ID
        if player_fbref_id:
            found_player_id = player_fbref_id
        else:
            # Generate a hex ID similar to FBref format
            import hashlib
            found_player_id = hashlib.md5(player_name.encode()).hexdigest()[:8]
        
        try:
            cur.execute("""
                INSERT INTO player (player_id, player_name)
                VALUES (%s, %s)
                ON CONFLICT (player_id) DO NOTHING
            """, (found_player_id, player_name))
            conn.commit()
            print(f"    Created new player: {player_name} ({found_player_id})")
        except Exception as e:
            conn.rollback()
            print(f"    Error creating player {player_name}: {e}")
    
    cur.close()
    conn.close()
    
    return found_player_id

def insert_goalkeeper_performance(match_id, team_season_id, gk_data):
    """
    Insert goalkeeper performance data into database.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Get player ID (FBref hex ID)
        player_id = get_player_uuid(
            gk_data.get('player_name'),
            gk_data.get('player_fbref_id')
        )
        
        if not player_id:
            print(f"    Could not get/create player ID for {gk_data.get('player_name')}")
            return False
        
        # Prepare performance data
        perf = gk_data.get('performance_data', {})
        
        # Generate unique match_goalkeeper_id
        import hashlib
        match_gk_id = hashlib.md5(f"{match_id}_{player_id}".encode()).hexdigest()[:8]
        
        # Insert goalkeeper performance
        insert_query = """
            INSERT INTO match_goalkeeper_performance (
                match_goalkeeper_id,
                match_id,
                player_id,
                team_season_id,
                minutes_played,
                goals_against,
                shots_on_target_against,
                saves,
                save_percentage,
                post_shot_xg,
                launched_cmp,
                launched_att,
                launched_cmp_pct,
                passes_att,
                passes_thr,
                passes_launch_pct,
                passes_avg_len,
                goal_kicks_att,
                goal_kicks_launch_pct,
                goal_kicks_avg_len,
                crosses_opp,
                crosses_stp,
                crosses_stp_pct,
                sweeper_opa,
                sweeper_avg_dist
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (match_goalkeeper_id) DO UPDATE SET
                minutes_played = EXCLUDED.minutes_played,
                goals_against = EXCLUDED.goals_against,
                saves = EXCLUDED.saves
        """
        
        # Convert percentage strings to floats if needed
        save_pct = perf.get('save_percentage')
        if isinstance(save_pct, str) and '%' in save_pct:
            save_pct = float(save_pct.replace('%', '')) / 100
        
        values = (
            match_gk_id,
            match_id,
            player_id,
            team_season_id,
            gk_data.get('minutes_played'),
            perf.get('goals_against'),
            perf.get('shots_on_target_against'),
            perf.get('saves'),
            save_pct,
            perf.get('post_shot_xg'),
            perf.get('launched_cmp'),
            perf.get('launched_att'),
            perf.get('launched_cmp_pct'),
            perf.get('passes_att'),
            perf.get('passes_thr'),
            perf.get('passes_launch_pct'),
            perf.get('passes_avg_len'),
            perf.get('goal_kicks_att'),
            perf.get('goal_kicks_launch_pct'),
            perf.get('goal_kicks_avg_len'),
            perf.get('crosses_opp'),
            perf.get('crosses_stp'),
            perf.get('crosses_stp_pct'),
            perf.get('sweeper_opa'),
            perf.get('sweeper_avg_dist')
        )
        
        cur.execute(insert_query, values)
        conn.commit()
        
        print(f"    Inserted goalkeeper performance for {gk_data.get('player_name')}")
        result = True
        
    except Exception as e:
        conn.rollback()
        print(f"    Error inserting goalkeeper performance: {e}")
        result = False
    finally:
        cur.close()
        conn.close()
    
    return result

def main():
    """Main extraction and insertion process"""
    
    print("=" * 80)
    print("GOALKEEPER DATA EXTRACTION AND INSERTION")
    print("=" * 80)
    
    # Get missing matches
    print("\nFetching matches missing goalkeeper data...")
    missing_matches = get_missing_matches()
    print(f"Found {len(missing_matches)} matches missing goalkeeper data")
    
    # Group by season
    by_season = {}
    for match in missing_matches:
        season = match[1]
        if season not in by_season:
            by_season[season] = []
        by_season[season].append(match)
    
    print("\nBreakdown by season:")
    for season in sorted(by_season.keys()):
        print(f"  {season}: {len(by_season[season])} matches")
    
    # Process each missing match
    successful = 0
    failed = 0
    
    for match in missing_matches:
        match_id = match[0]
        season_year = match[1]
        match_date = match[2]
        home_team_season_id = match[3]
        away_team_season_id = match[4]
        
        # Check if HTML file exists
        html_path = Path(HTML_DIR) / f"match_{match_id}.html"
        if not html_path.exists():
            print(f"\nSkipping {match_id} - HTML file not found")
            failed += 1
            continue
        
        # Extract goalkeeper data
        gk_data = extract_goalkeepers_from_html(html_path, match_id)
        
        # Insert home goalkeepers
        for gk in gk_data['home_goalkeepers']:
            if insert_goalkeeper_performance(match_id, home_team_season_id, gk):
                successful += 1
            else:
                failed += 1
        
        # Insert away goalkeepers
        for gk in gk_data['away_goalkeepers']:
            if insert_goalkeeper_performance(match_id, away_team_season_id, gk):
                successful += 1
            else:
                failed += 1
    
    # Final summary
    print("\n" + "=" * 80)
    print("EXTRACTION COMPLETE")
    print(f"  Successful insertions: {successful}")
    print(f"  Failed insertions: {failed}")
    print("=" * 80)
    
    # Verify final coverage
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            COUNT(DISTINCT m.match_id) as total_matches,
            COUNT(DISTINCT mgp.match_id) as matches_with_gk,
            ROUND(COUNT(DISTINCT mgp.match_id) * 100.0 / COUNT(DISTINCT m.match_id), 2) as coverage_pct
        FROM match m
        LEFT JOIN match_goalkeeper_performance mgp ON m.match_id = mgp.match_id
    """)
    
    result = cur.fetchone()
    print(f"\nFinal database coverage:")
    print(f"  Total matches: {result[0]}")
    print(f"  Matches with goalkeeper data: {result[1]}")
    print(f"  Coverage: {result[2]}%")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()