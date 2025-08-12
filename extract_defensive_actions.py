#!/usr/bin/env python3
"""
Extract FBref defensive actions data from HTML files to populate match_player_defensive_actions table.
Handles all 20 columns in the expanded schema.
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_batch
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
from typing import Dict, List, Tuple, Optional
import json

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

# HTML files directory
HTML_DIR = '/Users/thomasmcmillan/projects/nwsl_db_migration/html_files/'

# Column mappings from FBref HTML to database columns
COLUMN_MAPPINGS = {
    'tackles': 'tackles',
    'tackles_won': 'tackles_won',
    'tackles_def_3rd': 'tackles_def_3rd',
    'tackles_mid_3rd': 'tackles_mid_3rd',
    'tackles_att_3rd': 'tackles_att_3rd',
    'challenge_tackles': 'challenges_tkl',  # Note: different name in HTML
    'challenges': 'challenges_att',
    'challenge_tackles_pct': 'challenges_tkl_pct',
    'challenges_lost': 'challenges_lost',
    'blocks': 'blocks',
    'blocked_shots': 'blocks_shots',
    'blocked_passes': 'blocks_passes',
    'interceptions': 'interceptions',
    'tackles_interceptions': 'tackles_interceptions',
    'clearances': 'clearances',
    'errors': 'errors'
}

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**DB_CONFIG)

def extract_match_id_from_filename(filename: str) -> str:
    """Extract match ID from HTML filename."""
    # Remove 'match_' prefix and '.html' suffix
    if filename.startswith('match_'):
        match_id = filename[6:]  # Remove 'match_'
    else:
        match_id = filename
    
    if match_id.endswith('.html'):
        match_id = match_id[:-5]  # Remove '.html'
    
    return match_id

def extract_defensive_actions_from_html(filepath: str) -> List[Dict]:
    """
    Extract defensive actions data from an FBref HTML file.
    
    Args:
        filepath: Path to the HTML file
    
    Returns:
        List of dictionaries containing player defensive actions data
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all defensive actions tables (one per team)
    defense_tables = []
    for table in soup.find_all('table'):
        table_id = table.get('id', '')
        if '_defense' in table_id and 'stats_' in table_id:
            defense_tables.append(table)
    
    all_player_data = []
    
    for table in defense_tables:
        # Extract team ID from table ID (e.g., stats_ae38d267_defense -> ae38d267)
        table_id = table.get('id', '')
        team_id_match = re.search(r'stats_([a-f0-9]+)_defense', table_id)
        team_id = team_id_match.group(1) if team_id_match else None
        
        # Parse table rows
        rows = table.find_all('tr')
        
        for row in rows:
            # Skip header rows
            if row.find('th', {'scope': 'col'}):
                continue
            
            # Get player info
            player_cell = row.find('th', {'data-stat': 'player'})
            if not player_cell:
                continue
            
            # Extract FBref player ID from data-append-csv attribute
            player_fbref_id = player_cell.get('data-append-csv', '')
            if not player_fbref_id:
                continue
            
            player_data = {
                'player_fbref_id': player_fbref_id,
                'team_fbref_id': team_id
            }
            
            # Extract all defensive stats
            for stat_name, db_column in COLUMN_MAPPINGS.items():
                stat_cell = row.find('td', {'data-stat': stat_name})
                if stat_cell:
                    value = stat_cell.text.strip()
                    
                    # Handle percentage values
                    if stat_name == 'challenge_tackles_pct':
                        if value and value != '':
                            try:
                                # Remove % sign if present and convert to decimal
                                value = value.replace('%', '').strip()
                                player_data[db_column] = float(value) if value else None
                            except ValueError:
                                player_data[db_column] = None
                        else:
                            player_data[db_column] = None
                    else:
                        # Handle integer values
                        try:
                            player_data[db_column] = int(value) if value and value != '' else 0
                        except ValueError:
                            player_data[db_column] = 0
                else:
                    # Set default value for missing columns
                    if db_column == 'challenges_tkl_pct':
                        player_data[db_column] = None
                    else:
                        player_data[db_column] = 0
            
            all_player_data.append(player_data)
    
    return all_player_data

def update_defensive_actions(conn, match_id: str, player_data: List[Dict]) -> Tuple[int, int, List[str]]:
    """
    Update match_player_defensive_actions table with extracted data.
    
    Args:
        conn: Database connection
        match_id: Match ID
        player_data: List of player defensive actions data
    
    Returns:
        Tuple of (successful_updates, failed_updates, error_messages)
    """
    cursor = conn.cursor()
    successful_updates = 0
    failed_updates = 0
    error_messages = []
    
    for player in player_data:
        try:
            # Get match_player.id using player_id and match_id
            cursor.execute("""
                SELECT mp.id 
                FROM match_player mp
                WHERE mp.player_id = %s AND mp.match_id = %s
            """, (player['player_fbref_id'], match_id))
            
            result = cursor.fetchone()
            if not result:
                error_messages.append(f"No match_player record found for player {player['player_fbref_id']} in match {match_id}")
                failed_updates += 1
                continue
            
            match_player_id = result[0]
            
            # Check if defensive actions record exists
            cursor.execute("""
                SELECT id FROM match_player_defensive_actions
                WHERE match_player_id = %s
            """, (match_player_id,))
            
            existing_record = cursor.fetchone()
            
            if existing_record:
                # Update existing record
                update_query = """
                    UPDATE match_player_defensive_actions SET
                        tackles = %s,
                        tackles_won = %s,
                        tackles_def_3rd = %s,
                        tackles_mid_3rd = %s,
                        tackles_att_3rd = %s,
                        challenges_tkl = %s,
                        challenges_att = %s,
                        challenges_tkl_pct = %s,
                        challenges_lost = %s,
                        blocks = %s,
                        blocks_shots = %s,
                        blocks_passes = %s,
                        interceptions = %s,
                        tackles_interceptions = %s,
                        clearances = %s,
                        errors = %s
                    WHERE match_player_id = %s
                """
                
                cursor.execute(update_query, (
                    player.get('tackles', 0),
                    player.get('tackles_won', 0),
                    player.get('tackles_def_3rd', 0),
                    player.get('tackles_mid_3rd', 0),
                    player.get('tackles_att_3rd', 0),
                    player.get('challenges_tkl', 0),
                    player.get('challenges_att', 0),
                    player.get('challenges_tkl_pct'),
                    player.get('challenges_lost', 0),
                    player.get('blocks', 0),
                    player.get('blocks_shots', 0),
                    player.get('blocks_passes', 0),
                    player.get('interceptions', 0),
                    player.get('tackles_interceptions', 0),
                    player.get('clearances', 0),
                    player.get('errors', 0),
                    match_player_id
                ))
            else:
                # Insert new record
                insert_query = """
                    INSERT INTO match_player_defensive_actions (
                        match_player_id,
                        tackles, tackles_won, tackles_def_3rd, tackles_mid_3rd, tackles_att_3rd,
                        challenges_tkl, challenges_att, challenges_tkl_pct, challenges_lost,
                        blocks, blocks_shots, blocks_passes,
                        interceptions, tackles_interceptions, clearances, errors
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_query, (
                    match_player_id,
                    player.get('tackles', 0),
                    player.get('tackles_won', 0),
                    player.get('tackles_def_3rd', 0),
                    player.get('tackles_mid_3rd', 0),
                    player.get('tackles_att_3rd', 0),
                    player.get('challenges_tkl', 0),
                    player.get('challenges_att', 0),
                    player.get('challenges_tkl_pct'),
                    player.get('challenges_lost', 0),
                    player.get('blocks', 0),
                    player.get('blocks_shots', 0),
                    player.get('blocks_passes', 0),
                    player.get('interceptions', 0),
                    player.get('tackles_interceptions', 0),
                    player.get('clearances', 0),
                    player.get('errors', 0)
                ))
            
            successful_updates += 1
            
        except Exception as e:
            error_messages.append(f"Error processing player {player.get('player_fbref_id', 'unknown')}: {str(e)}")
            failed_updates += 1
    
    conn.commit()
    cursor.close()
    
    return successful_updates, failed_updates, error_messages

def process_single_file(filepath: str, conn) -> Dict:
    """
    Process a single HTML file and extract defensive actions data.
    
    Args:
        filepath: Path to the HTML file
        conn: Database connection
    
    Returns:
        Dictionary with processing results
    """
    filename = os.path.basename(filepath)
    match_id = extract_match_id_from_filename(filename)
    
    result = {
        'filename': filename,
        'match_id': match_id,
        'status': 'pending'
    }
    
    try:
        # Extract data from HTML
        player_data = extract_defensive_actions_from_html(filepath)
        result['players_found'] = len(player_data)
        
        if player_data:
            # Update database
            successful, failed, errors = update_defensive_actions(conn, match_id, player_data)
            result['successful_updates'] = successful
            result['failed_updates'] = failed
            result['errors'] = errors
            result['status'] = 'success' if failed == 0 else 'partial'
        else:
            result['status'] = 'no_data'
            result['successful_updates'] = 0
            result['failed_updates'] = 0
            
    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['successful_updates'] = 0
        result['failed_updates'] = 0
    
    return result

def verify_test_match(conn, match_id: str = '07c68416'):
    """
    Verify that test match data was correctly extracted and populated.
    
    Args:
        conn: Database connection
        match_id: Match ID to verify
    """
    cursor = conn.cursor()
    
    print(f"\nVerifying defensive actions data for match {match_id}:")
    print("=" * 80)
    
    # Check specific players we know should have data
    test_players = [
        ('2fcc0a10', 'Alyssa Thompson', 4, 3, 1, 1),  # Expected: tackles=4, tackles_won=3, blocks=1, interceptions=1
        ('18dd307e', 'Riley Tiernan', 0, 0, 1, 0),    # Expected: tackles=0, tackles_won=0, blocks=1, interceptions=0
        ('f2284088', 'Claire Emslie', 0, 0, 0, 0)      # Expected: tackles=0, tackles_won=0, blocks=0, interceptions=0
    ]
    
    for player_id, player_name, exp_tackles, exp_tkl_won, exp_blocks, exp_int in test_players:
        cursor.execute("""
            SELECT 
                p.player_name,
                da.tackles, da.tackles_won, da.tackles_def_3rd, da.tackles_mid_3rd, da.tackles_att_3rd,
                da.challenges_tkl, da.challenges_att, da.challenges_tkl_pct, da.challenges_lost,
                da.blocks, da.blocks_shots, da.blocks_passes,
                da.interceptions, da.tackles_interceptions, da.clearances, da.errors
            FROM match_player mp
            JOIN player p ON mp.player_id = p.player_id
            LEFT JOIN match_player_defensive_actions da ON mp.id = da.match_player_id
            WHERE mp.match_id = %s AND p.player_id = %s
        """, (match_id, player_id))
        
        result = cursor.fetchone()
        if result:
            print(f"\n{player_name}:")
            print(f"  Tackles: {result[1]} (expected: {exp_tackles})")
            print(f"  Tackles Won: {result[2]} (expected: {exp_tkl_won})")
            print(f"  Def 3rd: {result[3]}, Mid 3rd: {result[4]}, Att 3rd: {result[5]}")
            print(f"  Challenges: Tkl={result[6]}, Att={result[7]}, Pct={result[8]}, Lost={result[9]}")
            print(f"  Blocks: {result[10]} (expected: {exp_blocks}), Shots={result[11]}, Passes={result[12]}")
            print(f"  Interceptions: {result[13]} (expected: {exp_int})")
            print(f"  Tkl+Int: {result[14]}, Clearances: {result[15]}, Errors: {result[16]}")
        else:
            print(f"\n{player_name}: No data found")
    
    cursor.close()

def main():
    """Main execution function."""
    
    # Test mode or full processing
    test_mode = '--test' in sys.argv
    
    conn = get_db_connection()
    
    if test_mode:
        print("Running in TEST MODE - Processing only match_07c68416.html")
        test_file = os.path.join(HTML_DIR, 'match_07c68416.html')
        
        if not os.path.exists(test_file):
            print(f"Test file not found: {test_file}")
            sys.exit(1)
        
        # Process test file
        result = process_single_file(test_file, conn)
        
        print("\nTest Results:")
        print(json.dumps(result, indent=2))
        
        # Verify the data
        verify_test_match(conn, '07c68416')
        
    else:
        print("Processing all HTML files in directory...")
        
        # Get all HTML files
        html_files = [f for f in os.listdir(HTML_DIR) if f.endswith('.html')]
        total_files = len(html_files)
        
        print(f"Found {total_files} HTML files to process")
        
        results = {
            'total_files': total_files,
            'processed': 0,
            'successful': 0,
            'partial': 0,
            'no_data': 0,
            'errors': 0,
            'total_updates': 0,
            'failed_updates': 0,
            'error_messages': []
        }
        
        # Process each file
        for i, filename in enumerate(html_files, 1):
            filepath = os.path.join(HTML_DIR, filename)
            
            if i % 100 == 0:
                print(f"Processing file {i}/{total_files}: {filename}")
            
            file_result = process_single_file(filepath, conn)
            
            results['processed'] += 1
            
            if file_result['status'] == 'success':
                results['successful'] += 1
            elif file_result['status'] == 'partial':
                results['partial'] += 1
            elif file_result['status'] == 'no_data':
                results['no_data'] += 1
            else:
                results['errors'] += 1
                results['error_messages'].append({
                    'file': filename,
                    'error': file_result.get('error', 'Unknown error')
                })
            
            if 'successful_updates' in file_result:
                results['total_updates'] += file_result['successful_updates']
            if 'failed_updates' in file_result:
                results['failed_updates'] += file_result['failed_updates']
        
        # Print summary
        print("\n" + "=" * 80)
        print("EXTRACTION COMPLETE")
        print("=" * 80)
        print(f"Total files processed: {results['processed']}/{results['total_files']}")
        print(f"Successful: {results['successful']}")
        print(f"Partial success: {results['partial']}")
        print(f"No data found: {results['no_data']}")
        print(f"Errors: {results['errors']}")
        print(f"\nTotal player updates: {results['total_updates']}")
        print(f"Failed updates: {results['failed_updates']}")
        
        if results['error_messages']:
            print("\nFirst 10 errors:")
            for err in results['error_messages'][:10]:
                print(f"  {err['file']}: {err['error']}")
        
        # Save detailed results to file
        with open('defensive_actions_extraction_report.json', 'w') as f:
            json.dump(results, f, indent=2)
        print("\nDetailed report saved to: defensive_actions_extraction_report.json")
    
    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()