#!/usr/bin/env python3
"""
Extract comprehensive lineup data from FBref HTML files including:
- Starters (players who began the match)
- Substitutes who entered the game
- Unused substitutes (dressed but never played)
"""

import os
import sys
import json
import uuid
import psycopg2
from psycopg2.extras import execute_batch
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
from typing import Dict, List, Tuple, Optional
from io import StringIO

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

# HTML files directory
HTML_DIR = '/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files'

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**DB_CONFIG)

def get_matches_missing_lineups(conn) -> List[Dict]:
    """Get list of matches that have no lineup data."""
    query = """
    WITH lineup_coverage AS (
        SELECT 
            m.match_id,
            m.match_date,
            hts.team_name_season_1 as home_team,
            ats.team_name_season_1 as away_team,
            s.season_year as season,
            m.home_team_season_id,
            m.away_team_season_id
        FROM match m
        LEFT JOIN match_lineup ml ON m.match_id = ml.match_id
        LEFT JOIN season s ON m.season_uuid = s.id
        LEFT JOIN team_season hts ON m.home_team_season_id = hts.id
        LEFT JOIN team_season ats ON m.away_team_season_id = ats.id
        GROUP BY m.match_id, m.match_date, hts.team_name_season_1, ats.team_name_season_1, 
                 s.season_year, m.home_team_season_id, m.away_team_season_id
        HAVING COUNT(DISTINCT ml.lineup_id) = 0
    )
    SELECT 
        match_id,
        TO_CHAR(match_date, 'YYYY-MM-DD') as match_date,
        home_team,
        away_team,
        season,
        home_team_season_id,
        away_team_season_id
    FROM lineup_coverage
    ORDER BY match_date;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

def get_player_mappings(conn) -> Dict:
    """Get FBref ID to UUID mappings for players."""
    
    # Get player mappings
    player_query = """
    SELECT DISTINCT 
        p.player_id,
        p.id as player_uuid,
        p.player_name
    FROM player p
    WHERE p.player_id IS NOT NULL;
    """
    
    with conn.cursor() as cur:
        cur.execute(player_query)
        player_map = {row[0]: {'uuid': row[1], 'name': row[2]} 
                     for row in cur.fetchall()}
    
    return player_map

def get_team_mappings(conn) -> Tuple[Dict, Dict]:
    """Get team season mappings and FBref team ID to UUID mappings."""
    
    # Get team season mappings
    team_query = """
    SELECT 
        ts.id as team_season_id,
        ts.team_id as fbref_team_id,
        ts.team_name_season_1,
        ts.team_name_season_2
    FROM team_season ts;
    """
    
    with conn.cursor() as cur:
        cur.execute(team_query)
        team_season_map = {}
        for row in cur.fetchall():
            team_season_map[row[0]] = {
                'fbref_team_id': row[1], 
                'name': row[2],
                'alt_name': row[3]
            }
    
    # Get FBref team ID to UUID mapping
    team_uuid_query = """
    SELECT DISTINCT 
        ts.team_id as fbref_team_id,
        ml.team_uuid
    FROM match_lineup ml
    JOIN team_season ts ON ml.team_season_id = ts.id
    WHERE ml.team_uuid IS NOT NULL AND ts.team_id IS NOT NULL
    ORDER BY ts.team_id;
    """
    
    with conn.cursor() as cur:
        cur.execute(team_uuid_query)
        fbref_to_uuid_map = {row[0]: row[1] for row in cur.fetchall()}
    
    return team_season_map, fbref_to_uuid_map

def identify_team_from_caption(caption_text: str, home_team: str, away_team: str) -> str:
    """Identify which team (home/away) based on caption text."""
    caption_lower = caption_text.lower()
    home_lower = home_team.lower() if home_team else ""
    away_lower = away_team.lower() if away_team else ""
    
    # Check for team name in caption
    if home_lower and home_lower in caption_lower:
        return 'home'
    elif away_lower and away_lower in caption_lower:
        return 'away'
    
    # Check for common variations
    home_words = home_lower.split()
    away_words = away_lower.split()
    
    for word in home_words:
        if len(word) > 3 and word in caption_lower:
            return 'home'
    
    for word in away_words:
        if len(word) > 3 and word in caption_lower:
            return 'away'
    
    return None

def extract_lineup_from_html(html_file: str, match_info: Dict, player_map: Dict, 
                            team_season_map: Dict, fbref_to_uuid_map: Dict) -> List[Dict]:
    """
    Extract comprehensive lineup data from HTML file.
    Returns list of lineup entries including starters, used subs, and unused subs.
    """
    lineups = []
    
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        # Get team information
        home_team_season_id = match_info['home_team_season_id']
        away_team_season_id = match_info['away_team_season_id']
        
        home_team_info = team_season_map.get(home_team_season_id, {})
        away_team_info = team_season_map.get(away_team_season_id, {})
        
        # Get team UUIDs from FBref IDs
        home_team_uuid = fbref_to_uuid_map.get(home_team_info.get('fbref_team_id'))
        away_team_uuid = fbref_to_uuid_map.get(away_team_info.get('fbref_team_id'))
        
        # Find all player stats tables
        player_tables = soup.find_all('table', id=re.compile(r'stats_[a-f0-9]{8}_summary'))
        
        # Process each table (should be 2 - one for each team)
        for table_idx, table in enumerate(player_tables):
            table_id = table.get('id', '')
            
            # Determine which team this table belongs to
            caption = table.find('caption')
            team_type = None
            
            if caption:
                caption_text = caption.text.strip()
                team_type = identify_team_from_caption(
                    caption_text, 
                    match_info['home_team'], 
                    match_info['away_team']
                )
            
            # If we couldn't identify from caption, use table order
            if not team_type:
                team_type = 'home' if table_idx == 0 else 'away'
            
            # Get team information
            if team_type == 'home':
                team_uuid = home_team_uuid
                team_season_id = home_team_season_id
            else:
                team_uuid = away_team_uuid
                team_season_id = away_team_season_id
            
            # Process table rows to extract player data
            rows = table.find_all('tr')
            
            # Track player count to identify starters (first 11)
            player_count = 0
            
            for row in rows:
                # Skip header rows
                if row.find('th', {'scope': 'col'}):
                    continue
                
                # Get player link for FBref ID
                player_cell = row.find('th', {'data-stat': 'player'})
                if not player_cell:
                    continue
                
                player_link = player_cell.find('a')
                if not player_link:
                    continue
                
                player_href = player_link.get('href', '')
                player_fbref_id = None
                
                # Extract FBref player ID from href
                match = re.search(r'/players/([a-f0-9]{8})/', player_href)
                if match:
                    player_fbref_id = match.group(1)
                
                # Get player name
                player_name = player_link.text.strip()
                
                # Get data from row cells
                cells = row.find_all('td')
                
                # Extract key information
                position = None
                minutes = 0
                jersey_number = None
                
                for cell in cells:
                    stat = cell.get('data-stat', '')
                    cell_text = cell.text.strip()
                    
                    if stat == 'position':
                        position = cell_text if cell_text else None
                    elif stat == 'minutes':
                        try:
                            minutes = int(float(cell_text)) if cell_text else 0
                        except:
                            minutes = 0
                    elif stat == 'shirtnumber':
                        try:
                            jersey_number = int(cell_text) if cell_text else None
                        except:
                            jersey_number = None
                
                player_count += 1
                
                # Determine if player is a starter
                # Generally, first 11 players are starters
                is_starter = player_count <= 11
                
                # Refine based on minutes and position
                if minutes == 0:
                    # Player didn't play - unused substitute
                    is_starter = False
                elif minutes >= 60 and player_count <= 14:
                    # Played most of the game, likely a starter
                    is_starter = True
                
                # Get player UUID from mapping
                player_uuid = None
                if player_fbref_id and player_fbref_id in player_map:
                    player_uuid = player_map[player_fbref_id]['uuid']
                
                # Create lineup entry
                lineup_entry = {
                    'lineup_id': str(uuid.uuid4()),
                    'match_id': match_info['match_id'],
                    'player_id': player_fbref_id,
                    'player_name': player_name,
                    'position': position,
                    'jersey_number': jersey_number,
                    'is_starter': is_starter,
                    'formation': None,  # Will need to extract this separately if available
                    'player_uuid': player_uuid,
                    'team_uuid': team_uuid,
                    'team_season_id': team_season_id
                }
                
                lineups.append(lineup_entry)
        
        # Try to find goalkeeper information if not already captured
        keeper_tables = soup.find_all('table', id=re.compile(r'keeper_stats_[a-f0-9]{8}'))
        
        for table in keeper_tables:
            # Process goalkeeper tables for any additional info
            rows = table.find_all('tr')
            
            for row in rows:
                if row.find('th', {'scope': 'col'}):
                    continue
                
                player_cell = row.find('th', {'data-stat': 'player'})
                if not player_cell:
                    continue
                
                player_link = player_cell.find('a')
                if not player_link:
                    continue
                
                player_href = player_link.get('href', '')
                match = re.search(r'/players/([a-f0-9]{8})/', player_href)
                if match:
                    player_fbref_id = match.group(1)
                    
                    # Check if we already have this goalkeeper
                    existing = [l for l in lineups if l['player_id'] == player_fbref_id]
                    if not existing:
                        # Add goalkeeper if not already in lineup
                        player_name = player_link.text.strip()
                        
                        # Determine team
                        caption = table.find('caption')
                        team_type = None
                        if caption:
                            caption_text = caption.text.strip()
                            team_type = identify_team_from_caption(
                                caption_text, 
                                match_info['home_team'], 
                                match_info['away_team']
                            )
                        
                        if team_type == 'home':
                            team_uuid = home_team_uuid
                            team_season_id = home_team_season_id
                        elif team_type == 'away':
                            team_uuid = away_team_uuid
                            team_season_id = away_team_season_id
                        else:
                            continue
                        
                        player_uuid = player_map.get(player_fbref_id, {}).get('uuid')
                        
                        lineup_entry = {
                            'lineup_id': str(uuid.uuid4()),
                            'match_id': match_info['match_id'],
                            'player_id': player_fbref_id,
                            'player_name': player_name,
                            'position': 'GK',
                            'jersey_number': None,
                            'is_starter': True,  # Goalkeepers in this table usually started
                            'formation': None,
                            'player_uuid': player_uuid,
                            'team_uuid': team_uuid,
                            'team_season_id': team_season_id
                        }
                        
                        lineups.append(lineup_entry)
    
    except Exception as e:
        print(f"Error processing {html_file}: {str(e)}")
        import traceback
        traceback.print_exc()
    
    return lineups

def insert_lineups(conn, lineups: List[Dict]) -> int:
    """Insert lineup data into database."""
    if not lineups:
        return 0
    
    insert_query = """
    INSERT INTO match_lineup (
        lineup_id, match_id, player_id, player_name, position,
        jersey_number, is_starter, formation, player_uuid, 
        team_uuid, team_season_id
    ) VALUES (
        %(lineup_id)s, %(match_id)s, %(player_id)s, %(player_name)s, %(position)s,
        %(jersey_number)s, %(is_starter)s, %(formation)s, %(player_uuid)s,
        %(team_uuid)s, %(team_season_id)s
    )
    ON CONFLICT (lineup_id) DO NOTHING
    RETURNING lineup_id;
    """
    
    with conn.cursor() as cur:
        execute_batch(cur, insert_query, lineups)
        inserted = cur.rowcount
        conn.commit()
        return inserted

def main():
    """Main execution function."""
    print("Starting comprehensive lineup extraction...")
    print(f"Database: {DB_CONFIG['database']} on {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"HTML directory: {HTML_DIR}")
    print()
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        # Get matches missing lineups
        missing_matches = get_matches_missing_lineups(conn)
        print(f"Found {len(missing_matches)} matches missing lineup data")
        
        if not missing_matches:
            print("No matches missing lineup data!")
            return
        
        # Get player and team mappings
        print("Loading player and team mappings...")
        player_map = get_player_mappings(conn)
        team_season_map, fbref_to_uuid_map = get_team_mappings(conn)
        print(f"Loaded {len(player_map)} player mappings")
        print(f"Loaded {len(team_season_map)} team season mappings")
        print(f"Loaded {len(fbref_to_uuid_map)} FBref team ID to UUID mappings")
        print()
        
        # Process each match
        total_lineups = 0
        successful_matches = 0
        failed_matches = []
        
        for idx, match in enumerate(missing_matches):
            match_id = match['match_id']
            html_file = os.path.join(HTML_DIR, f"match_{match_id}.html")
            
            print(f"[{idx+1}/{len(missing_matches)}] Processing {match['match_date']}: {match['home_team']} vs {match['away_team']}...", end=' ')
            
            if not os.path.exists(html_file):
                print(f"HTML file not found!")
                failed_matches.append({
                    'match': match,
                    'reason': 'HTML file not found'
                })
                continue
            
            # Extract lineups
            lineups = extract_lineup_from_html(html_file, match, player_map, 
                                              team_season_map, fbref_to_uuid_map)
            
            if lineups:
                # Insert into database
                inserted = insert_lineups(conn, lineups)
                total_lineups += len(lineups)
                successful_matches += 1
                print(f"Extracted {len(lineups)} players, inserted {inserted}")
            else:
                print("No lineup data extracted")
                failed_matches.append({
                    'match': match,
                    'reason': 'No lineup data found in HTML'
                })
        
        # Print summary
        print("\n" + "="*60)
        print("EXTRACTION SUMMARY")
        print("="*60)
        print(f"Matches processed: {len(missing_matches)}")
        print(f"Successful: {successful_matches}")
        print(f"Failed: {len(failed_matches)}")
        print(f"Total lineup entries processed: {total_lineups}")
        
        if failed_matches:
            print("\nFailed matches:")
            for failure in failed_matches:
                m = failure['match']
                print(f"  - {m['match_date']}: {m['home_team']} vs {m['away_team']} - {failure['reason']}")
        
        # Check new coverage
        print("\nChecking new coverage...")
        remaining = get_matches_missing_lineups(conn)
        original_missing = len(missing_matches)
        new_missing = len(remaining)
        coverage_improved = original_missing - new_missing
        
        print(f"Original matches missing data: {original_missing}")
        print(f"Matches still missing data: {new_missing}")
        print(f"Coverage improved by: {coverage_improved} matches")
        
        if new_missing > 0:
            total_query = "SELECT COUNT(*) FROM match"
            with conn.cursor() as cur:
                cur.execute(total_query)
                total_matches = cur.fetchone()[0]
            
            coverage_pct = ((total_matches - new_missing) / total_matches) * 100
            print(f"Overall lineup coverage: {coverage_pct:.2f}% ({total_matches - new_missing}/{total_matches} matches)")
        
        if remaining and len(remaining) <= 10:
            print("\nRemaining matches without lineups:")
            for match in remaining:
                print(f"  - {match['match_date']}: {match['home_team']} vs {match['away_team']} (ID: {match['match_id']})")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()