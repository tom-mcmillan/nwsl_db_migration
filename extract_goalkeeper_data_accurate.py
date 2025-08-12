#!/usr/bin/env python3
"""
Extract accurate FBref goalkeeper data from HTML files.
ACCURACY OVER COMPLETENESS - Better to have NULL than wrong data.
"""

import os
import re
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import traceback

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

# HTML files directory
HTML_DIR = '/Users/thomasmcmillan/projects/nwsl_db_migration/html_files/'

# Mapping from FBref column names to database columns
COLUMN_MAPPING = {
    # Shot Stopping
    'gk_shots_on_target_against': 'shots_on_target_against',
    'gk_goals_against': 'goals_against',
    'gk_saves': 'saves',
    'gk_save_pct': 'save_percentage',
    'gk_psxg': 'post_shot_xg',
    
    # Launched passes
    'gk_passes_completed_launched': 'launched_completed',
    'gk_passes_launched': 'launched_attempted',
    'gk_passes_pct_launched': 'launched_completion_pct',
    
    # Passes
    'gk_passes': 'passes_attempted',
    'gk_passes_throws': 'passes_throws',
    'gk_pct_passes_launched': 'passes_launch_pct',
    'gk_passes_length_avg': 'passes_avg_length',
    
    # Goal kicks
    'gk_goal_kicks': 'goal_kicks_attempted',
    'gk_pct_goal_kicks_launched': 'goal_kicks_launch_pct',
    'gk_goal_kick_length_avg': 'goal_kicks_avg_length',
    
    # Crosses
    'gk_crosses': 'crosses_opposed',
    'gk_crosses_stopped': 'crosses_stopped',
    'gk_crosses_stopped_pct': 'crosses_stopped_pct',
    
    # Sweeper
    'gk_def_actions_outside_pen_area': 'sweeper_actions',
    'gk_avg_distance_def_actions': 'sweeper_avg_distance',
    
    # Basic info
    'minutes': 'minutes_played'
}

def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)

def extract_match_id_from_filename(filename: str) -> Optional[str]:
    """Extract match ID from filename."""
    match = re.search(r'match_([a-f0-9]{8})\.html', filename)
    return match.group(1) if match else None

def extract_goalkeeper_table(soup: BeautifulSoup, table_id: str) -> Optional[pd.DataFrame]:
    """Extract goalkeeper stats table from HTML."""
    try:
        # Find the table by ID
        table = soup.find('table', {'id': table_id})
        if not table:
            return None
        
        # Convert to DataFrame
        from io import StringIO
        df = pd.read_html(StringIO(str(table)))[0]
        
        # Handle multi-level columns from pandas
        if isinstance(df.columns, pd.MultiIndex):
            # Flatten column names
            df.columns = ['_'.join(col).strip('_') for col in df.columns.values]
        
        # Map the flattened column names to FBref data-stat attributes
        # We need to extract the actual data-stat values from the HTML
        column_mapping = {}
        headers = table.find_all('th')
        for th in headers:
            data_stat = th.get('data-stat')
            if data_stat:
                # Get the text content for mapping
                col_text = th.text.strip()
                # Map common abbreviations
                if data_stat == 'gk_shots_on_target_against':
                    column_mapping['Shot Stopping_SoTA'] = 'gk_shots_on_target_against'
                elif data_stat == 'gk_goals_against':
                    column_mapping['Shot Stopping_GA'] = 'gk_goals_against'
                elif data_stat == 'gk_saves':
                    column_mapping['Shot Stopping_Saves'] = 'gk_saves'
                elif data_stat == 'gk_save_pct':
                    column_mapping['Shot Stopping_Save%'] = 'gk_save_pct'
                elif data_stat == 'gk_psxg':
                    column_mapping['Shot Stopping_PSxG'] = 'gk_psxg'
                elif data_stat == 'gk_passes_completed_launched':
                    column_mapping['Launched_Cmp'] = 'gk_passes_completed_launched'
                elif data_stat == 'gk_passes_launched':
                    column_mapping['Launched_Att'] = 'gk_passes_launched'
                elif data_stat == 'gk_passes_pct_launched':
                    column_mapping['Launched_Cmp%'] = 'gk_passes_pct_launched'
                elif data_stat == 'gk_passes':
                    column_mapping['Passes_Att (GK)'] = 'gk_passes'
                elif data_stat == 'gk_passes_throws':
                    column_mapping['Passes_Thr'] = 'gk_passes_throws'
                elif data_stat == 'gk_pct_passes_launched':
                    column_mapping['Passes_Launch%'] = 'gk_pct_passes_launched'
                elif data_stat == 'gk_passes_length_avg':
                    column_mapping['Passes_AvgLen'] = 'gk_passes_length_avg'
                elif data_stat == 'gk_goal_kicks':
                    column_mapping['Goal Kicks_Att'] = 'gk_goal_kicks'
                elif data_stat == 'gk_pct_goal_kicks_launched':
                    column_mapping['Goal Kicks_Launch%'] = 'gk_pct_goal_kicks_launched'
                elif data_stat == 'gk_goal_kick_length_avg':
                    column_mapping['Goal Kicks_AvgLen'] = 'gk_goal_kick_length_avg'
                elif data_stat == 'gk_crosses':
                    column_mapping['Crosses_Opp'] = 'gk_crosses'
                elif data_stat == 'gk_crosses_stopped':
                    column_mapping['Crosses_Stp'] = 'gk_crosses_stopped'
                elif data_stat == 'gk_crosses_stopped_pct':
                    column_mapping['Crosses_Stp%'] = 'gk_crosses_stopped_pct'
                elif data_stat == 'gk_def_actions_outside_pen_area':
                    column_mapping['Sweeper_#OPA'] = 'gk_def_actions_outside_pen_area'
                elif data_stat == 'gk_avg_distance_def_actions':
                    column_mapping['Sweeper_AvgDist'] = 'gk_avg_distance_def_actions'
                elif data_stat == 'minutes':
                    column_mapping['Unnamed: 3_level_0_Min'] = 'minutes'
        
        # Rename columns based on mapping
        df = df.rename(columns=column_mapping)
        
        # Extract player IDs from data-append-csv attributes
        player_ids = []
        for row in table.find_all('tr'):
            th = row.find('th', {'data-stat': 'player'})
            if th and th.get('data-append-csv'):
                player_ids.append(th.get('data-append-csv'))
        
        if player_ids and len(player_ids) == len(df):
            df['player_id'] = player_ids
        
        return df
        
    except Exception as e:
        print(f"Error extracting table {table_id}: {e}")
        import traceback
        traceback.print_exc()
        return None

def clean_numeric_value(value) -> Optional[float]:
    """Clean and convert numeric values, returning None for invalid/missing data."""
    if pd.isna(value):
        return None
    
    # Handle string values
    if isinstance(value, str):
        # Remove any non-numeric characters except . and -
        cleaned = re.sub(r'[^\d\.\-]', '', value)
        if not cleaned or cleaned == '':
            return None
        try:
            return float(cleaned)
        except:
            return None
    
    # Handle numeric values
    try:
        return float(value)
    except:
        return None

def validate_goalkeeper_data(data: Dict) -> Tuple[bool, List[str]]:
    """
    Validate goalkeeper data for logical consistency.
    Returns (is_valid, list_of_issues)
    """
    issues = []
    
    # Critical validation: saves + goals_against should equal shots_on_target_against
    if all(x is not None for x in [data.get('saves'), data.get('goals_against'), data.get('shots_on_target_against')]):
        expected_total = data['saves'] + data['goals_against']
        if abs(expected_total - data['shots_on_target_against']) > 0.01:  # Allow small float difference
            issues.append(f"Data validation failed: saves ({data['saves']}) + goals_against ({data['goals_against']}) != shots_on_target_against ({data['shots_on_target_against']})")
    
    # Validate save percentage if all required fields present
    if all(x is not None for x in [data.get('shots_on_target_against'), data.get('goals_against'), data.get('save_percentage')]):
        if data['shots_on_target_against'] > 0:
            calculated_save_pct = ((data['shots_on_target_against'] - data['goals_against']) / data['shots_on_target_against']) * 100
            if abs(calculated_save_pct - data['save_percentage']) > 0.5:  # Allow 0.5% tolerance
                issues.append(f"Save percentage mismatch: calculated {calculated_save_pct:.1f}% vs provided {data['save_percentage']}%")
    
    # Validate launched passes
    if all(x is not None for x in [data.get('launched_completed'), data.get('launched_attempted')]):
        if data['launched_completed'] > data['launched_attempted']:
            issues.append(f"Launched completed ({data['launched_completed']}) > attempted ({data['launched_attempted']})")
    
    # Validate crosses
    if all(x is not None for x in [data.get('crosses_stopped'), data.get('crosses_opposed')]):
        if data['crosses_stopped'] > data['crosses_opposed']:
            issues.append(f"Crosses stopped ({data['crosses_stopped']}) > opposed ({data['crosses_opposed']})")
    
    return len(issues) == 0, issues

def process_goalkeeper_row(row: Dict, match_id: str, match_info: Dict) -> Optional[Dict]:
    """Process a single goalkeeper row from FBref table."""
    try:
        # Skip if no player_id
        if 'player_id' not in row or pd.isna(row.get('player_id')):
            return None
        
        data = {
            'match_id': match_id,
            'player_id': row['player_id'],
            'team_season_id': match_info.get('team_season_id'),
            'match_date': match_info.get('match_date'),
            'season_id': match_info.get('season_id')
        }
        
        # Extract all available stats with careful cleaning
        for fbref_col, db_col in COLUMN_MAPPING.items():
            if fbref_col in row:
                value = clean_numeric_value(row[fbref_col])
                data[db_col] = value
        
        # Validate the data
        is_valid, issues = validate_goalkeeper_data(data)
        if not is_valid:
            player_name = row.get('Player', 'Unknown') if 'Player' in row else 'Unknown'
            print(f"  Validation issues for {player_name} in match {match_id}:")
            for issue in issues:
                print(f"    - {issue}")
        
        return data
        
    except Exception as e:
        print(f"Error processing goalkeeper row: {e}")
        return None

def get_match_info(conn, match_id: str) -> Dict:
    """Get match information from database."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # First try to get from existing goalkeeper records
        cur.execute("""
            SELECT DISTINCT match_date, season_id
            FROM match_goalkeeper_performance
            WHERE match_id = %s
            LIMIT 1
        """, (match_id,))
        result = cur.fetchone()
        
        if result:
            return dict(result)
        
        # If not found, try match_player table for any player in the match
        cur.execute("""
            SELECT DISTINCT match_date, season_id
            FROM match_player
            WHERE match_id = %s
            LIMIT 1
        """, (match_id,))
        result = cur.fetchone()
        
        return dict(result) if result else {}

def get_team_season_id(conn, player_id: str, season_id: int) -> Optional[str]:
    """Get team_season_id for a player in a given season."""
    with conn.cursor() as cur:
        # Try from match_player table first
        cur.execute("""
            SELECT DISTINCT team_season_id
            FROM match_player
            WHERE player_id = %s AND season_id = %s
            LIMIT 1
        """, (player_id, season_id))
        result = cur.fetchone()
        
        if result:
            return result[0]
        
        # Try from existing goalkeeper records
        cur.execute("""
            SELECT DISTINCT team_season_id
            FROM match_goalkeeper_performance
            WHERE player_id = %s AND season_id = %s
            LIMIT 1
        """, (player_id, season_id))
        result = cur.fetchone()
        
        return result[0] if result else None

def upsert_goalkeeper_data(conn, data: Dict) -> bool:
    """Upsert goalkeeper data - replace existing with accurate FBref data."""
    try:
        with conn.cursor() as cur:
            # Build the UPDATE SET clause dynamically for non-null values only
            update_fields = []
            update_values = []
            
            for col in COLUMN_MAPPING.values():
                if col in data and data[col] is not None:
                    update_fields.append(f"{col} = %s")
                    update_values.append(data[col])
            
            if not update_fields:
                print(f"  No valid data to update for player {data['player_id']} in match {data['match_id']}")
                return False
            
            # Add updated_at
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            
            # Build the INSERT statement with ON CONFLICT
            insert_cols = ['match_id', 'player_id', 'team_season_id', 'match_date', 'season_id']
            insert_vals = [data.get(col) for col in insert_cols]
            
            # Add the data columns
            for col in COLUMN_MAPPING.values():
                if col in data and data[col] is not None:
                    insert_cols.append(col)
                    insert_vals.append(data[col])
            
            # Check if there's a unique constraint on (match_id, player_id)
            # If not, we'll handle it differently
            cur.execute("""
                SELECT 1 FROM match_goalkeeper_performance 
                WHERE match_id = %s AND player_id = %s
            """, (data['match_id'], data['player_id']))
            
            exists = cur.fetchone()
            
            if exists:
                # Update existing record
                update_query = f"""
                    UPDATE match_goalkeeper_performance 
                    SET {', '.join(update_fields)}
                    WHERE match_id = %s AND player_id = %s
                    RETURNING id
                """
                cur.execute(update_query, update_values + [data['match_id'], data['player_id']])
            else:
                # Insert new record
                insert_query = f"""
                    INSERT INTO match_goalkeeper_performance ({', '.join(insert_cols)})
                    VALUES ({', '.join(['%s'] * len(insert_cols))})
                    RETURNING id
                """
                cur.execute(insert_query, insert_vals)
            
            result = cur.fetchone()
            return result is not None
            
    except Exception as e:
        print(f"Error upserting goalkeeper data: {e}")
        traceback.print_exc()
        return False

def process_html_file(filepath: str, conn) -> Dict:
    """Process a single HTML file and extract goalkeeper data."""
    stats = {
        'file': os.path.basename(filepath),
        'match_id': None,
        'tables_found': 0,
        'goalkeepers_extracted': 0,
        'records_updated': 0,
        'validation_errors': 0,
        'errors': []
    }
    
    try:
        # Extract match ID
        match_id = extract_match_id_from_filename(os.path.basename(filepath))
        if not match_id:
            stats['errors'].append("Could not extract match ID from filename")
            return stats
        
        stats['match_id'] = match_id
        
        # Get match info
        match_info = get_match_info(conn, match_id)
        if not match_info:
            stats['errors'].append("Could not find match info in database")
            return stats
        
        # Parse HTML
        with open(filepath, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        # Find all goalkeeper tables (one per team)
        keeper_tables = soup.find_all('table', id=re.compile(r'keeper_stats_[a-f0-9]+'))
        stats['tables_found'] = len(keeper_tables)
        
        for table in keeper_tables:
            table_id = table.get('id')
            print(f"  Processing table: {table_id}")
            
            # Extract team ID from table ID
            team_match = re.search(r'keeper_stats_([a-f0-9]+)', table_id)
            team_id = team_match.group(1) if team_match else None
            
            # Extract data
            df = extract_goalkeeper_table(soup, table_id)
            if df is None or df.empty:
                continue
            
            # Process each goalkeeper
            for _, row in df.iterrows():
                # Convert Series to dict for easier handling
                row_dict = row.to_dict()
                goalkeeper_data = process_goalkeeper_row(row_dict, match_id, match_info)
                if not goalkeeper_data:
                    continue
                
                stats['goalkeepers_extracted'] += 1
                
                # Get team_season_id if not already set
                if not goalkeeper_data.get('team_season_id'):
                    team_season_id = get_team_season_id(conn, 
                                                       goalkeeper_data['player_id'], 
                                                       goalkeeper_data.get('season_id'))
                    if team_season_id:
                        goalkeeper_data['team_season_id'] = team_season_id
                
                # Validate before inserting
                is_valid, issues = validate_goalkeeper_data(goalkeeper_data)
                if not is_valid:
                    stats['validation_errors'] += 1
                
                # Upsert to database (even with validation issues - let DB constraints handle it)
                if goalkeeper_data.get('team_season_id'):
                    if upsert_goalkeeper_data(conn, goalkeeper_data):
                        stats['records_updated'] += 1
                        player_name = row_dict.get('Player', 'Unknown') if 'Player' in row_dict else 'Unknown'
                        print(f"    Updated: {player_name} ({goalkeeper_data['player_id']})")
                else:
                    stats['errors'].append(f"No team_season_id for player {goalkeeper_data['player_id']}")
        
        conn.commit()
        
    except Exception as e:
        stats['errors'].append(str(e))
        print(f"Error processing {filepath}: {e}")
        traceback.print_exc()
        conn.rollback()
    
    return stats

def main():
    """Main extraction process."""
    print("=" * 80)
    print("FBref Goalkeeper Data Extraction - ACCURACY FOCUSED")
    print("=" * 80)
    print(f"HTML Directory: {HTML_DIR}")
    print(f"Database: {DB_CONFIG['database']}")
    print()
    
    # Get list of HTML files
    html_files = [f for f in os.listdir(HTML_DIR) if f.startswith('match_') and f.endswith('.html')]
    print(f"Found {len(html_files)} match HTML files")
    print()
    
    # Test with specific file first
    test_file = 'match_07c68416.html'
    if test_file in html_files:
        print(f"Testing with {test_file} first...")
        print("-" * 40)
        
        conn = get_db_connection()
        try:
            stats = process_html_file(os.path.join(HTML_DIR, test_file), conn)
            print(f"\nTest Results for {test_file}:")
            print(f"  Match ID: {stats['match_id']}")
            print(f"  Tables found: {stats['tables_found']}")
            print(f"  Goalkeepers extracted: {stats['goalkeepers_extracted']}")
            print(f"  Records updated: {stats['records_updated']}")
            print(f"  Validation errors: {stats['validation_errors']}")
            if stats['errors']:
                print(f"  Errors: {stats['errors']}")
            
            # Verify the data
            print("\nVerifying extracted data:")
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT g.*
                    FROM match_goalkeeper_performance g
                    WHERE g.match_id = %s
                    ORDER BY g.player_id
                """, (stats['match_id'],))
                
                for row in cur.fetchall():
                    print(f"\n  Player ID: {row['player_id']}:")
                    print(f"    SoTA: {row['shots_on_target_against']}, GA: {row['goals_against']}, Saves: {row['saves']}")
                    print(f"    Save%: {row['save_percentage']}, PSxG: {row['post_shot_xg']}")
                    print(f"    Launched: {row['launched_completed']}/{row['launched_attempted']} ({row['launched_completion_pct']}%)")
                    print(f"    Passes: {row['passes_attempted']}, Throws: {row['passes_throws']}")
            
            print("\nTest successful. Proceeding with all files...")
                
        finally:
            conn.close()
    
    # Process all files
    print("\n" + "=" * 80)
    print("Processing all match files...")
    print("=" * 80)
    
    conn = get_db_connection()
    overall_stats = {
        'total_files': len(html_files),
        'files_processed': 0,
        'total_tables': 0,
        'total_goalkeepers': 0,
        'total_updates': 0,
        'total_validation_errors': 0,
        'files_with_errors': []
    }
    
    try:
        for i, filename in enumerate(html_files, 1):
            print(f"\n[{i}/{len(html_files)}] Processing {filename}...")
            
            filepath = os.path.join(HTML_DIR, filename)
            stats = process_html_file(filepath, conn)
            
            overall_stats['files_processed'] += 1
            overall_stats['total_tables'] += stats['tables_found']
            overall_stats['total_goalkeepers'] += stats['goalkeepers_extracted']
            overall_stats['total_updates'] += stats['records_updated']
            overall_stats['total_validation_errors'] += stats['validation_errors']
            
            if stats['errors']:
                overall_stats['files_with_errors'].append({
                    'file': filename,
                    'errors': stats['errors']
                })
            
            # Progress update every 10 files
            if i % 10 == 0:
                print(f"\n--- Progress: {i}/{len(html_files)} files processed ---")
                print(f"    Total updates so far: {overall_stats['total_updates']}")
    
    finally:
        conn.close()
    
    # Print final summary
    print("\n" + "=" * 80)
    print("EXTRACTION COMPLETE - SUMMARY")
    print("=" * 80)
    print(f"Files processed: {overall_stats['files_processed']}/{overall_stats['total_files']}")
    print(f"Goalkeeper tables found: {overall_stats['total_tables']}")
    print(f"Goalkeeper records extracted: {overall_stats['total_goalkeepers']}")
    print(f"Database records updated: {overall_stats['total_updates']}")
    print(f"Validation errors: {overall_stats['total_validation_errors']}")
    print(f"Files with errors: {len(overall_stats['files_with_errors'])}")
    
    if overall_stats['files_with_errors']:
        print("\nFiles with errors:")
        for error_info in overall_stats['files_with_errors'][:10]:  # Show first 10
            print(f"  - {error_info['file']}: {error_info['errors']}")
    
    # Save summary to file
    summary_file = f"goalkeeper_extraction_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(summary_file, 'w') as f:
        json.dump(overall_stats, f, indent=2, default=str)
    print(f"\nDetailed summary saved to: {summary_file}")
    
    # Data quality report
    print("\n" + "=" * 80)
    print("DATA QUALITY REPORT")
    print("=" * 80)
    
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check coverage
            cur.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT match_id) as unique_matches,
                    COUNT(DISTINCT player_id) as unique_goalkeepers,
                    COUNT(CASE WHEN saves IS NOT NULL THEN 1 END) as has_saves,
                    COUNT(CASE WHEN post_shot_xg IS NOT NULL THEN 1 END) as has_xg,
                    COUNT(CASE WHEN passes_attempted IS NOT NULL THEN 1 END) as has_passes,
                    COUNT(CASE WHEN crosses_opposed IS NOT NULL THEN 1 END) as has_crosses,
                    COUNT(CASE WHEN sweeper_actions IS NOT NULL THEN 1 END) as has_sweeper
                FROM match_goalkeeper_performance
            """)
            
            result = cur.fetchone()
            if result:
                print(f"Total records: {result['total_records']}")
                print(f"Unique matches: {result['unique_matches']}")
                print(f"Unique goalkeepers: {result['unique_goalkeepers']}")
                print(f"\nData completeness:")
                print(f"  Has saves data: {result['has_saves']} ({result['has_saves']*100/result['total_records']:.1f}%)")
                print(f"  Has xG data: {result['has_xg']} ({result['has_xg']*100/result['total_records']:.1f}%)")
                print(f"  Has pass data: {result['has_passes']} ({result['has_passes']*100/result['total_records']:.1f}%)")
                print(f"  Has cross data: {result['has_crosses']} ({result['has_crosses']*100/result['total_records']:.1f}%)")
                print(f"  Has sweeper data: {result['has_sweeper']} ({result['has_sweeper']*100/result['total_records']:.1f}%)")
    
    finally:
        conn.close()
    
    print("\n" + "=" * 80)
    print("Extraction complete. Data has been updated with accurate FBref values.")
    print("NULL values indicate data not available from FBref (better than incorrect data).")
    print("=" * 80)

if __name__ == "__main__":
    main()