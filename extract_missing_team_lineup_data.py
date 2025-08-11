#!/usr/bin/env python3
"""
Script to extract missing team performance and lineup data from FBref HTML files
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from bs4 import BeautifulSoup
import uuid
from datetime import datetime
import json
import re
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

# HTML files directory
HTML_DIR = '/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files'

def connect_to_db():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)

def get_database_coverage():
    """Analyze current database coverage for team and lineup data"""
    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get total matches
    cur.execute("SELECT COUNT(*) as total FROM match")
    total_matches = cur.fetchone()['total']
    
    # Check if match_team_performance exists
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'match_team%'
    """)
    team_tables = [row['table_name'] for row in cur.fetchall()]
    print(f"Found team-related tables: {team_tables}")
    
    # Get match_lineup coverage
    cur.execute("""
        SELECT COUNT(DISTINCT match_id) as covered 
        FROM match_lineup
    """)
    lineup_covered = cur.fetchone()['covered']
    
    # Get match_team coverage if table exists
    team_covered = 0
    if 'match_team' in team_tables:
        cur.execute("""
            SELECT COUNT(DISTINCT match_id) as covered 
            FROM match_team
        """)
        team_covered = cur.fetchone()['covered']
    
    print(f"\nDatabase Coverage Summary:")
    print(f"Total matches: {total_matches}")
    print(f"match_lineup coverage: {lineup_covered}/{total_matches} ({lineup_covered/total_matches*100:.2f}%)")
    if 'match_team' in team_tables:
        print(f"match_team coverage: {team_covered}/{total_matches} ({team_covered/total_matches*100:.2f}%)")
    
    cur.close()
    conn.close()
    
    return total_matches, lineup_covered, team_covered

def get_matches_missing_lineup_data():
    """Get list of matches missing lineup data"""
    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT DISTINCT m.match_id, m.match_date, m.home_team_name, m.away_team_name,
               m.home_team_id, m.away_team_id, m.season_id
        FROM match m
        WHERE NOT EXISTS (
            SELECT 1 FROM match_lineup ml 
            WHERE ml.match_id = m.match_id
        )
        ORDER BY m.match_date
    """)
    
    missing_matches = cur.fetchall()
    cur.close()
    conn.close()
    
    return missing_matches

def get_matches_missing_team_data():
    """Get list of matches missing team performance data"""
    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Check if match_team table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'match_team'
        )
    """)
    table_exists = cur.fetchone()['exists']
    
    if not table_exists:
        print("Note: match_team table does not exist, returning all matches")
        cur.execute("""
            SELECT DISTINCT m.match_id, m.match_date, m.home_team_name, m.away_team_name,
                   m.home_team_id, m.away_team_id, m.season_id
            FROM match m
            ORDER BY m.match_date
        """)
    else:
        cur.execute("""
            SELECT DISTINCT m.match_id, m.match_date, m.home_team_name, m.away_team_name,
                   m.home_team_id, m.away_team_id, m.season_id
            FROM match m
            WHERE NOT EXISTS (
                SELECT 1 FROM match_team mt 
                WHERE mt.match_id = m.match_id
            )
            ORDER BY m.match_date
        """)
    
    missing_matches = cur.fetchall()
    cur.close()
    conn.close()
    
    return missing_matches

def find_html_file_for_match(match_id: str, match_date: str) -> Optional[str]:
    """Find HTML file for a given match"""
    # Try different file naming patterns
    patterns = [
        f"{match_id}.html",
        f"*{match_id}*.html",
        f"*{match_date}*.html"
    ]
    
    for pattern in patterns:
        for file in Path(HTML_DIR).glob(pattern):
            return str(file)
    
    return None

def extract_lineup_from_html(soup: BeautifulSoup, match_id: str, team_id: str, team_name: str) -> List[Dict]:
    """Extract lineup data from HTML"""
    lineups = []
    
    # Look for lineup tables - different patterns for different years
    # Pattern 1: Table with id like "lineup_<team_id>"
    lineup_table = soup.find('table', {'id': f'lineup_{team_id}'})
    
    # Pattern 2: Look for section with team name and find lineup info
    if not lineup_table:
        # Find all tables and look for ones with player data
        for table in soup.find_all('table'):
            table_id = table.get('id', '')
            # Check for stats tables that might contain lineup info
            if team_id in table_id and ('summary' in table_id or 'stats' in table_id):
                # Extract starting lineup from stats table
                tbody = table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        cells = row.find_all(['td', 'th'])
                        if len(cells) > 2:
                            # Extract player info
                            player_cell = cells[0] if cells[0].name == 'th' else cells[1]
                            player_link = player_cell.find('a')
                            if player_link:
                                player_name = player_link.text.strip()
                                player_href = player_link.get('href', '')
                                player_id = extract_id_from_href(player_href)
                                
                                # Check if starter (usually by minutes played or position info)
                                position = ''
                                jersey_number = None
                                is_starter = True  # Assume starter if in main table
                                
                                # Try to extract position
                                for cell in cells:
                                    if cell.get('data-stat') == 'position':
                                        position = cell.text.strip()
                                        break
                                
                                # Try to extract jersey number
                                for cell in cells:
                                    if cell.get('data-stat') == 'shirtnumber':
                                        try:
                                            jersey_number = int(cell.text.strip())
                                        except:
                                            pass
                                        break
                                
                                lineup_entry = {
                                    'lineup_id': str(uuid.uuid4()),
                                    'match_id': match_id,
                                    'team_id': team_id,
                                    'player_id': player_id or str(uuid.uuid4()),
                                    'player_name': player_name,
                                    'position': position,
                                    'jersey_number': jersey_number,
                                    'is_starter': is_starter,
                                    'formation': None  # Will try to extract separately
                                }
                                lineups.append(lineup_entry)
                break
    
    return lineups

def extract_team_stats_from_html(soup: BeautifulSoup, match_id: str, team_id: str, match_date: str) -> Optional[Dict]:
    """Extract team statistics from HTML"""
    team_stats = {
        'match_team_id': str(uuid.uuid4()),
        'match_id': match_id,
        'team_id': team_id,
        'match_date': match_date
    }
    
    # Look for team stats table
    team_stats_table = soup.find('table', {'id': 'team_stats'})
    if team_stats_table:
        # Parse team stats
        rows = team_stats_table.find_all('tr')
        for row in rows:
            stat_name = row.find('th')
            if stat_name:
                stat_name_text = stat_name.text.strip().lower()
                cells = row.find_all('td')
                if len(cells) >= 2:
                    # Determine which column is for this team
                    home_val = cells[0].text.strip()
                    away_val = cells[1].text.strip() if len(cells) > 1 else None
                    
                    # Map stat names to database columns
                    stat_mapping = {
                        'possession': 'possession_pct',
                        'passing accuracy': 'passing_acc_pct',
                        'shots on target': 'sot_pct',
                        'saves': 'saves_pct',
                        'fouls': 'fouls',
                        'corners': 'corners',
                        'crosses': 'crosses',
                        'touches': 'touches',
                        'tackles': 'tackles',
                        'interceptions': 'interceptions',
                        'aerials won': 'aerials_won',
                        'clearances': 'clearances',
                        'offsides': 'offsides',
                        'goal kicks': 'goal_kicks',
                        'throw ins': 'throw_ins',
                        'long balls': 'long_balls'
                    }
                    
                    for key, col in stat_mapping.items():
                        if key in stat_name_text:
                            try:
                                # Parse value (remove % signs, convert to number)
                                value = home_val if team_id in soup.text[:1000] else away_val
                                if value:
                                    value = value.replace('%', '').replace(',', '')
                                    team_stats[col] = float(value) if '.' in value else int(value)
                            except:
                                pass
    
    return team_stats if len(team_stats) > 3 else None

def extract_id_from_href(href: str) -> Optional[str]:
    """Extract FBref hex ID from href"""
    if not href:
        return None
    # Pattern: /en/players/<hex_id>/Name
    match = re.search(r'/players/([a-f0-9]{8})/', href)
    if match:
        return match.group(1)
    return None

def process_match_html(match: Dict) -> Tuple[List[Dict], List[Dict]]:
    """Process HTML file for a match and extract lineup and team data"""
    lineups = []
    team_stats = []
    
    # Find HTML file
    html_file = find_html_file_for_match(match['match_id'], str(match['match_date']))
    if not html_file:
        print(f"  HTML file not found for match {match['match_id']} on {match['match_date']}")
        return lineups, team_stats
    
    print(f"  Processing {html_file}")
    
    # Parse HTML
    with open(html_file, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    # Extract data for both teams
    for team_id, team_name in [(match['home_team_id'], match['home_team_name']), 
                                (match['away_team_id'], match['away_team_name'])]:
        if team_id:
            # Extract lineup
            team_lineups = extract_lineup_from_html(soup, match['match_id'], team_id, team_name)
            lineups.extend(team_lineups)
            
            # Extract team stats
            team_stat = extract_team_stats_from_html(soup, match['match_id'], team_id, str(match['match_date']))
            if team_stat:
                team_stats.append(team_stat)
    
    return lineups, team_stats

def insert_lineup_data(lineups: List[Dict]):
    """Insert lineup data into database"""
    if not lineups:
        return 0
    
    conn = connect_to_db()
    cur = conn.cursor()
    
    inserted = 0
    for lineup in lineups:
        try:
            cur.execute("""
                INSERT INTO match_lineup (
                    lineup_id, match_id, team_id, player_id, player_name,
                    position, jersey_number, is_starter, formation
                ) VALUES (
                    %(lineup_id)s, %(match_id)s, %(team_id)s, %(player_id)s, %(player_name)s,
                    %(position)s, %(jersey_number)s, %(is_starter)s, %(formation)s
                )
                ON CONFLICT (lineup_id) DO NOTHING
            """, lineup)
            inserted += cur.rowcount
        except Exception as e:
            print(f"    Error inserting lineup: {e}")
            conn.rollback()
    
    conn.commit()
    cur.close()
    conn.close()
    
    return inserted

def insert_team_data(team_stats: List[Dict]):
    """Insert team statistics data into database"""
    if not team_stats:
        return 0
    
    conn = connect_to_db()
    cur = conn.cursor()
    
    # Check if match_team table exists, if not create it
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'match_team'
        )
    """)
    
    if not cur.fetchone()[0]:
        print("  match_team table does not exist - data will not be inserted")
        cur.close()
        conn.close()
        return 0
    
    inserted = 0
    for team_stat in team_stats:
        try:
            # Build dynamic insert based on available fields
            fields = [k for k in team_stat.keys() if team_stat[k] is not None]
            placeholders = [f"%({k})s" for k in fields]
            
            query = f"""
                INSERT INTO match_team ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT DO NOTHING
            """
            
            cur.execute(query, team_stat)
            inserted += cur.rowcount
        except Exception as e:
            print(f"    Error inserting team stats: {e}")
            conn.rollback()
    
    conn.commit()
    cur.close()
    conn.close()
    
    return inserted

def main():
    """Main extraction process"""
    print("=" * 80)
    print("FBref Missing Team & Lineup Data Extraction")
    print("=" * 80)
    
    # Get current coverage
    total_matches, lineup_covered, team_covered = get_database_coverage()
    
    # Get missing matches
    print("\nIdentifying matches with missing data...")
    missing_lineup_matches = get_matches_missing_lineup_data()
    missing_team_matches = get_matches_missing_team_data()
    
    print(f"\nMatches missing lineup data: {len(missing_lineup_matches)}")
    print(f"Matches missing team data: {len(missing_team_matches)}")
    
    # Process matches missing lineup data
    if missing_lineup_matches:
        print("\n" + "=" * 80)
        print("Processing Matches Missing Lineup Data")
        print("=" * 80)
        
        total_lineups_inserted = 0
        matches_processed = 0
        
        for match in missing_lineup_matches[:10]:  # Process first 10 as test
            print(f"\nProcessing match {match['match_id']}: {match['home_team_name']} vs {match['away_team_name']} ({match['match_date']})")
            
            lineups, _ = process_match_html(match)
            
            if lineups:
                inserted = insert_lineup_data(lineups)
                total_lineups_inserted += inserted
                if inserted > 0:
                    matches_processed += 1
                print(f"  Inserted {inserted} lineup records")
            else:
                print(f"  No lineup data extracted")
        
        print(f"\n{'-' * 40}")
        print(f"Lineup Extraction Summary:")
        print(f"  Matches processed: {matches_processed}")
        print(f"  Total lineup records inserted: {total_lineups_inserted}")
    
    # Process matches missing team data
    if missing_team_matches and team_covered > 0:  # Only if match_team exists and has data
        print("\n" + "=" * 80)
        print("Processing Matches Missing Team Data")
        print("=" * 80)
        
        total_team_stats_inserted = 0
        matches_processed = 0
        
        for match in missing_team_matches[:10]:  # Process first 10 as test
            print(f"\nProcessing match {match['match_id']}: {match['home_team_name']} vs {match['away_team_name']} ({match['match_date']})")
            
            _, team_stats = process_match_html(match)
            
            if team_stats:
                inserted = insert_team_data(team_stats)
                total_team_stats_inserted += inserted
                if inserted > 0:
                    matches_processed += 1
                print(f"  Inserted {inserted} team stat records")
            else:
                print(f"  No team stats extracted")
        
        print(f"\n{'-' * 40}")
        print(f"Team Stats Extraction Summary:")
        print(f"  Matches processed: {matches_processed}")
        print(f"  Total team stat records inserted: {total_team_stats_inserted}")
    
    # Final coverage check
    print("\n" + "=" * 80)
    print("Final Coverage Report")
    print("=" * 80)
    total_matches_new, lineup_covered_new, team_covered_new = get_database_coverage()
    
    print(f"\nLineup coverage improvement: {lineup_covered}/{total_matches} -> {lineup_covered_new}/{total_matches_new}")
    print(f"  Improvement: {(lineup_covered_new - lineup_covered)} matches")
    
    if team_covered > 0:
        print(f"\nTeam stats coverage improvement: {team_covered}/{total_matches} -> {team_covered_new}/{total_matches_new}")
        print(f"  Improvement: {(team_covered_new - team_covered)} matches")

if __name__ == "__main__":
    main()