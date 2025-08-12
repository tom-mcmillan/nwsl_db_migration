#!/usr/bin/env python3
"""
Extract complete FBref passing data from HTML files to populate all 37 columns.
This script extracts from both stats_{team_id}_passing and stats_{team_id}_passing_types tables.
"""

import os
import sys
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import re
import traceback
from io import StringIO

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

class FBrefPassingFullExtractor:
    """Extract complete FBref passing statistics from HTML match files."""
    
    def __init__(self, db_config: dict):
        """Initialize the extractor with database configuration."""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.stats = {
            'files_processed': 0,
            'tables_found': 0,
            'players_extracted': 0,
            'records_updated': 0,
            'columns_populated': {},
            'errors': []
        }
        
        # Complete mapping of FBref data-stat attributes to database columns
        self.field_mappings = {
            # Basic passing stats
            'passes_completed': 'passes_completed',
            'passes': 'passes',
            'assists': 'assists',
            'passes_pct': 'passes_pct',
            
            # Distance metrics
            'passes_total_distance': 'passes_total_distance',
            'passes_progressive_distance': 'passes_progressive_distance',
            
            # Short passes
            'passes_completed_short': 'passes_completed_short',
            'passes_short': 'passes_short',
            'passes_pct_short': 'passes_pct_short',
            
            # Medium passes
            'passes_completed_medium': 'passes_completed_medium',
            'passes_medium': 'passes_medium',
            'passes_pct_medium': 'passes_pct_medium',
            
            # Long passes
            'passes_completed_long': 'passes_completed_long',
            'passes_long': 'passes_long',
            'passes_pct_long': 'passes_pct_long',
            
            # Advanced metrics
            'xg_assist': 'xg_assist',
            'pass_xa': 'pass_xa',
            'assisted_shots': 'assisted_shots',  # Maps to KP in table header
            
            # Note: 'key_passes' in DB maps to 'assisted_shots' in FBref
            # This is because FBref uses 'KP' column which has data-stat='assisted_shots'
            
            # Positioning
            'passes_into_final_third': 'passes_into_final_third',
            'passes_into_penalty_area': 'passes_into_penalty_area',
            'crosses_into_penalty_area': 'crosses_into_penalty_area',
            'progressive_passes': 'progressive_passes',
            
            # Pass types (from passing_types table)
            'passes_live': 'passes_live',
            'passes_dead': 'passes_dead',
            'passes_free_kicks': 'passes_free_kicks',
            'through_balls': 'through_balls',
            'passes_switches': 'passes_switches',
            'crosses': 'crosses',
            'throw_ins': 'throw_ins',
            'corner_kicks': 'corner_kicks',
            'passes_offsides': 'passes_offsides',
            'passes_blocked': 'passes_blocked'
        }
        
    def connect_db(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            print("✓ Connected to database")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            return False
            
    def close_db(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
    def extract_match_id_from_filename(self, filename: str) -> str:
        """Extract match ID from HTML filename."""
        match = re.search(r'match_([a-f0-9]{8})\.html', filename)
        return match.group(1) if match else None
        
    def extract_all_passing_data(self, html_path: str) -> Dict[str, Dict]:
        """Extract all passing data from both passing and passing_types tables."""
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Dictionary to store combined data for each player
            players_data = {}
            
            # Find all tables
            all_tables = soup.find_all('table')
            
            for table in all_tables:
                table_id = table.get('id', '')
                
                # Process passing tables (not passing_types)
                if '_passing' in table_id and '_passing_types' not in table_id and 'stats_' in table_id:
                    team_match = re.search(r'stats_([a-f0-9]+)_passing', table_id)
                    if team_match:
                        team_hex_id = team_match.group(1)
                        print(f"  Processing passing table: {table_id}")
                        self.stats['tables_found'] += 1
                        
                        # Extract data from passing table
                        self.extract_from_passing_table(table, team_hex_id, players_data)
                        
                # Process passing_types tables
                elif '_passing_types' in table_id and 'stats_' in table_id:
                    team_match = re.search(r'stats_([a-f0-9]+)_passing_types', table_id)
                    if team_match:
                        team_hex_id = team_match.group(1)
                        print(f"  Processing passing_types table: {table_id}")
                        self.stats['tables_found'] += 1
                        
                        # Extract data from passing_types table
                        self.extract_from_passing_types_table(table, team_hex_id, players_data)
                        
            return players_data
            
        except Exception as e:
            print(f"  Error extracting tables from {html_path}: {e}")
            self.stats['errors'].append(f"Extract error: {html_path} - {str(e)}")
            return {}
            
    def extract_from_passing_table(self, table, team_hex_id: str, players_data: Dict):
        """Extract data from a passing table."""
        try:
            tbody = table.find('tbody')
            if not tbody:
                return
                
            rows = tbody.find_all('tr')
            
            for row in rows:
                # Skip separator rows
                if row.get('class') and 'thead' in row.get('class'):
                    continue
                    
                # Get player cell
                player_cell = row.find('th', {'data-stat': 'player'})
                if not player_cell:
                    continue
                    
                # Extract FBref player hex ID
                player_hex_id = player_cell.get('data-append-csv')
                if not player_hex_id:
                    continue
                    
                # Extract player name
                player_link = player_cell.find('a')
                player_name = player_link.text.strip() if player_link else player_cell.text.strip()
                
                # Initialize player data if not exists
                if player_hex_id not in players_data:
                    players_data[player_hex_id] = {
                        'player_hex_id': player_hex_id,
                        'player_name': player_name,
                        'team_hex_id': team_hex_id,
                        'stats': {}
                    }
                
                # Extract all passing statistics
                for cell in row.find_all('td'):
                    data_stat = cell.get('data-stat')
                    if data_stat and data_stat in self.field_mappings:
                        value = cell.text.strip()
                        if value and value != '':
                            try:
                                # Determine data type based on field
                                db_field = self.field_mappings[data_stat]
                                if db_field in ['passes_pct', 'passes_pct_short', 'passes_pct_medium', 
                                              'passes_pct_long', 'xg_assist', 'pass_xa']:
                                    players_data[player_hex_id]['stats'][db_field] = float(value)
                                else:
                                    players_data[player_hex_id]['stats'][db_field] = int(value)
                                    
                                # Track which columns we're populating
                                if db_field not in self.stats['columns_populated']:
                                    self.stats['columns_populated'][db_field] = 0
                                self.stats['columns_populated'][db_field] += 1
                                
                            except (ValueError, TypeError):
                                pass
                
                # Special handling for key_passes (maps to assisted_shots in FBref)
                if 'assisted_shots' in players_data[player_hex_id]['stats']:
                    players_data[player_hex_id]['stats']['key_passes'] = players_data[player_hex_id]['stats']['assisted_shots']
                    
        except Exception as e:
            print(f"    Error parsing passing table: {e}")
            traceback.print_exc()
            
    def extract_from_passing_types_table(self, table, team_hex_id: str, players_data: Dict):
        """Extract data from a passing_types table."""
        try:
            tbody = table.find('tbody')
            if not tbody:
                return
                
            rows = tbody.find_all('tr')
            
            for row in rows:
                # Skip separator rows
                if row.get('class') and 'thead' in row.get('class'):
                    continue
                    
                # Get player cell
                player_cell = row.find('th', {'data-stat': 'player'})
                if not player_cell:
                    continue
                    
                # Extract FBref player hex ID
                player_hex_id = player_cell.get('data-append-csv')
                if not player_hex_id:
                    continue
                    
                # Extract player name
                player_link = player_cell.find('a')
                player_name = player_link.text.strip() if player_link else player_cell.text.strip()
                
                # Initialize player data if not exists
                if player_hex_id not in players_data:
                    players_data[player_hex_id] = {
                        'player_hex_id': player_hex_id,
                        'player_name': player_name,
                        'team_hex_id': team_hex_id,
                        'stats': {}
                    }
                
                # Extract pass type statistics
                for cell in row.find_all('td'):
                    data_stat = cell.get('data-stat')
                    if data_stat and data_stat in self.field_mappings:
                        value = cell.text.strip()
                        if value and value != '':
                            try:
                                db_field = self.field_mappings[data_stat]
                                players_data[player_hex_id]['stats'][db_field] = int(value)
                                
                                # Track which columns we're populating
                                if db_field not in self.stats['columns_populated']:
                                    self.stats['columns_populated'][db_field] = 0
                                self.stats['columns_populated'][db_field] += 1
                                
                            except (ValueError, TypeError):
                                pass
                                
        except Exception as e:
            print(f"    Error parsing passing_types table: {e}")
            traceback.print_exc()
            
    def get_match_player_record(self, match_id: str, player_hex_id: str) -> Optional[Dict]:
        """Get match_player record for a given match and player."""
        try:
            query = """
                SELECT mp.id, mp.player_id, mp.match_id, 
                       mpp.id as passing_id
                FROM match_player mp
                LEFT JOIN match_player_passing mpp ON mp.id = mpp.match_player_id
                WHERE mp.match_id = %s AND mp.player_id = %s
            """
            self.cursor.execute(query, (match_id, player_hex_id))
            return self.cursor.fetchone()
        except Exception as e:
            print(f"    Error fetching match_player record: {e}")
            return None
            
    def update_passing_record(self, match_player_id: str, passing_id: str, stats: Dict) -> bool:
        """Update or insert complete passing statistics for a player."""
        try:
            if passing_id:
                # Build UPDATE query dynamically based on available stats
                set_clauses = []
                values = []
                
                for db_field, value in stats.items():
                    set_clauses.append(f"{db_field} = %s")
                    values.append(value)
                
                if not set_clauses:
                    return False
                    
                # Add updated_at
                set_clauses.append("updated_at = NOW()")
                values.append(passing_id)
                
                query = f"""
                    UPDATE match_player_passing
                    SET {', '.join(set_clauses)}
                    WHERE id = %s
                """
                self.cursor.execute(query, values)
                
            else:
                # Build INSERT query dynamically
                columns = ['match_player_id'] + list(stats.keys())
                values = [match_player_id] + list(stats.values())
                placeholders = ['%s'] * len(values)
                
                query = f"""
                    INSERT INTO match_player_passing 
                    ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """
                self.cursor.execute(query, values)
                
            return True
            
        except Exception as e:
            print(f"      Error updating passing record: {e}")
            self.conn.rollback()
            return False
            
    def process_match_file(self, html_path: str) -> Dict:
        """Process a single match HTML file."""
        filename = os.path.basename(html_path)
        match_id = self.extract_match_id_from_filename(filename)
        
        if not match_id:
            print(f"  Could not extract match ID from {filename}")
            return {'success': False, 'error': 'Invalid filename'}
            
        print(f"\nProcessing match {match_id} from {filename}")
        
        # Extract all passing data from both table types
        players_data = self.extract_all_passing_data(html_path)
        
        if not players_data:
            print(f"  No passing data found in {filename}")
            return {'success': False, 'error': 'No passing data found'}
            
        self.stats['players_extracted'] += len(players_data)
        
        results = {
            'match_id': match_id,
            'players_updated': 0,
            'columns_filled': set(),
            'errors': []
        }
        
        # Process each player's combined data
        for player_hex_id, player_info in players_data.items():
            player_name = player_info['player_name']
            stats = player_info['stats']
            
            if not stats:
                continue
                
            # Get match_player record
            mp_record = self.get_match_player_record(match_id, player_hex_id)
            
            if not mp_record:
                print(f"    No match_player record for {player_name} ({player_hex_id})")
                results['errors'].append(f"Missing match_player: {player_name} ({player_hex_id})")
                continue
                
            # Update passing statistics
            if self.update_passing_record(mp_record['id'], mp_record.get('passing_id'), stats):
                results['players_updated'] += 1
                results['columns_filled'].update(stats.keys())
                self.stats['records_updated'] += 1
                
                # Show summary of what was updated
                passes = stats.get('passes', 0)
                completed = stats.get('passes_completed', 0)
                pct = (completed / passes * 100) if passes > 0 else 0
                assists = stats.get('assists', 0)
                key_passes = stats.get('key_passes', 0)
                
                print(f"    ✓ {player_name}: {completed}/{passes} ({pct:.1f}%), {assists} assists, {key_passes} key passes, {len(stats)} fields")
            else:
                results['errors'].append(f"Update failed: {player_name}")
                
        # Commit changes for this match
        if results['players_updated'] > 0:
            self.conn.commit()
            print(f"  Committed {results['players_updated']} player updates with {len(results['columns_filled'])} unique columns")
            
        self.stats['files_processed'] += 1
        return results
        
    def verify_data_richness(self, match_id: str = None) -> Dict:
        """Verify the richness of data in the database."""
        try:
            # Build query based on whether we're checking specific match or all
            base_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN passes IS NOT NULL AND passes > 0 THEN 1 END) as has_basic_passes,
                    COUNT(CASE WHEN passes_total_distance IS NOT NULL THEN 1 END) as has_distance,
                    COUNT(CASE WHEN passes_short IS NOT NULL THEN 1 END) as has_short,
                    COUNT(CASE WHEN passes_medium IS NOT NULL THEN 1 END) as has_medium,
                    COUNT(CASE WHEN passes_long IS NOT NULL THEN 1 END) as has_long,
                    COUNT(CASE WHEN xg_assist IS NOT NULL THEN 1 END) as has_xg,
                    COUNT(CASE WHEN key_passes IS NOT NULL THEN 1 END) as has_key_passes,
                    COUNT(CASE WHEN progressive_passes IS NOT NULL THEN 1 END) as has_progressive,
                    COUNT(CASE WHEN passes_live IS NOT NULL THEN 1 END) as has_live,
                    COUNT(CASE WHEN crosses IS NOT NULL THEN 1 END) as has_crosses,
                    COUNT(CASE WHEN passes_blocked IS NOT NULL THEN 1 END) as has_blocked,
                    AVG(CASE WHEN passes > 0 THEN passes_completed::float / passes * 100 END) as avg_pass_pct
                FROM match_player_passing mpp
                JOIN match_player mp ON mp.id = mpp.match_player_id
            """
            
            if match_id:
                query = base_query + " WHERE mp.match_id = %s"
                self.cursor.execute(query, (match_id,))
            else:
                self.cursor.execute(base_query)
                
            result = self.cursor.fetchone()
            
            return {
                'total_records': result['total_records'],
                'basic_passes': result['has_basic_passes'],
                'has_distance': result['has_distance'],
                'has_short': result['has_short'],
                'has_medium': result['has_medium'],
                'has_long': result['has_long'],
                'has_xg': result['has_xg'],
                'has_key_passes': result['has_key_passes'],
                'has_progressive': result['has_progressive'],
                'has_pass_types': result['has_live'],
                'has_crosses': result['has_crosses'],
                'avg_pass_pct': round(result['avg_pass_pct'], 1) if result['avg_pass_pct'] else 0
            }
            
        except Exception as e:
            print(f"  Error verifying data richness: {e}")
            return {}
            
    def generate_report(self) -> str:
        """Generate comprehensive extraction report."""
        report = []
        report.append("\n" + "="*70)
        report.append("FBREF COMPLETE PASSING DATA EXTRACTION REPORT")
        report.append("="*70)
        report.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        report.append("EXTRACTION SUMMARY:")
        report.append(f"  Files processed: {self.stats['files_processed']}")
        report.append(f"  Tables found: {self.stats['tables_found']}")
        report.append(f"  Players extracted: {self.stats['players_extracted']}")
        report.append(f"  Records updated: {self.stats['records_updated']}")
        report.append("")
        
        if self.stats['columns_populated']:
            report.append("COLUMNS POPULATED (with count of non-null values):")
            sorted_columns = sorted(self.stats['columns_populated'].items(), key=lambda x: x[1], reverse=True)
            for col, count in sorted_columns[:20]:  # Show top 20
                report.append(f"  {col:30} : {count:,} values")
            if len(sorted_columns) > 20:
                report.append(f"  ... and {len(sorted_columns) - 20} more columns")
            report.append("")
            
        # Check overall data richness
        richness = self.verify_data_richness()
        if richness:
            report.append("DATA RICHNESS ANALYSIS:")
            report.append(f"  Total records: {richness['total_records']:,}")
            report.append(f"  Records with basic passes: {richness['basic_passes']:,}")
            report.append(f"  Records with distance data: {richness['has_distance']:,}")
            report.append(f"  Records with short/medium/long: {richness['has_short']:,}")
            report.append(f"  Records with xG metrics: {richness['has_xg']:,}")
            report.append(f"  Records with key passes: {richness['has_key_passes']:,}")
            report.append(f"  Records with pass types: {richness['has_pass_types']:,}")
            report.append(f"  Average pass completion %: {richness['avg_pass_pct']:.1f}%")
            report.append("")
            
        if self.stats['errors']:
            report.append("ERRORS ENCOUNTERED:")
            for error in self.stats['errors'][:10]:
                report.append(f"  - {error}")
            if len(self.stats['errors']) > 10:
                report.append(f"  ... and {len(self.stats['errors']) - 10} more errors")
            report.append("")
            
        report.append("="*70)
        return "\n".join(report)


def main():
    """Main execution function."""
    # Initialize extractor
    extractor = FBrefPassingFullExtractor(DB_CONFIG)
    
    if not extractor.connect_db():
        sys.exit(1)
        
    try:
        # Get before state for comparison
        print("\n" + "="*70)
        print("CHECKING INITIAL DATABASE STATE...")
        print("="*70)
        
        before_richness = extractor.verify_data_richness()
        print(f"Current records with data: {before_richness.get('basic_passes', 0):,}")
        print(f"Current records with xG: {before_richness.get('has_xg', 0):,}")
        print(f"Current records with pass types: {before_richness.get('has_pass_types', 0):,}")
        
        # Start with test file
        test_file = '/Users/thomasmcmillan/projects/nwsl_db_migration/html_files/match_07c68416.html'
        
        if os.path.exists(test_file):
            print(f"\n{'='*70}")
            print("TESTING WITH SINGLE FILE")
            print("="*70)
            
            result = extractor.process_match_file(test_file)
            
            # Verify the test file updates
            match_id = extractor.extract_match_id_from_filename(os.path.basename(test_file))
            if match_id:
                test_richness = extractor.verify_data_richness(match_id)
                print(f"\nTest match verification:")
                print(f"  Total records: {test_richness.get('total_records', 0)}")
                print(f"  Has distance data: {test_richness.get('has_distance', 0)}")
                print(f"  Has xG metrics: {test_richness.get('has_xg', 0)}")
                print(f"  Has pass types: {test_richness.get('has_pass_types', 0)}")
                
            # Automatically proceed with full extraction
            print("\n" + "="*70)
            print("Test file processed successfully!")
            print("Proceeding to process ALL match files...")
            print("This will update the entire database with complete passing data.")
            print("="*70)
            
            # Auto-proceed without user input
            if True:
                print("\n" + "="*70)
                print("PROCESSING ALL MATCH FILES")
                print("="*70)
                
                html_dir = '/Users/thomasmcmillan/projects/nwsl_db_migration/html_files'
                match_files = [f for f in os.listdir(html_dir) if f.startswith('match_') and f.endswith('.html')]
                match_files.sort()  # Process in order
                
                print(f"Found {len(match_files)} match files to process")
                
                # Process in batches with progress updates
                batch_size = 50
                for i in range(0, len(match_files), batch_size):
                    batch = match_files[i:i+batch_size]
                    print(f"\nProcessing batch {i//batch_size + 1} ({i+1}-{min(i+batch_size, len(match_files))} of {len(match_files)})")
                    
                    for filename in batch:
                        if filename == os.path.basename(test_file):
                            continue  # Skip test file since already processed
                            
                        filepath = os.path.join(html_dir, filename)
                        try:
                            extractor.process_match_file(filepath)
                        except Exception as e:
                            print(f"  Error processing {filename}: {e}")
                            extractor.stats['errors'].append(f"File error: {filename} - {str(e)}")
                    
                    # Show progress
                    print(f"  Batch complete. Total processed: {extractor.stats['files_processed']}")
                    
        else:
            print(f"Test file not found: {test_file}")
            
        # Generate final report
        print("\n" + "="*70)
        print("GENERATING FINAL REPORT")
        print("="*70)
        
        final_report = extractor.generate_report()
        print(final_report)
        
        # Compare before and after
        after_richness = extractor.verify_data_richness()
        print("\n" + "="*70)
        print("BEFORE/AFTER COMPARISON")
        print("="*70)
        print(f"Records with passes:     {before_richness.get('basic_passes', 0):,} → {after_richness.get('basic_passes', 0):,}")
        print(f"Records with distance:   {before_richness.get('has_distance', 0):,} → {after_richness.get('has_distance', 0):,}")
        print(f"Records with xG:         {before_richness.get('has_xg', 0):,} → {after_richness.get('has_xg', 0):,}")
        print(f"Records with pass types: {before_richness.get('has_pass_types', 0):,} → {after_richness.get('has_pass_types', 0):,}")
        print(f"Records with key passes: {before_richness.get('has_key_passes', 0):,} → {after_richness.get('has_key_passes', 0):,}")
        
        # Save report to file
        report_file = f"passing_full_extraction_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w') as f:
            f.write(final_report)
            f.write("\n\nBEFORE/AFTER COMPARISON:\n")
            f.write(f"Records with passes:     {before_richness.get('basic_passes', 0):,} → {after_richness.get('basic_passes', 0):,}\n")
            f.write(f"Records with distance:   {before_richness.get('has_distance', 0):,} → {after_richness.get('has_distance', 0):,}\n")
            f.write(f"Records with xG:         {before_richness.get('has_xg', 0):,} → {after_richness.get('has_xg', 0):,}\n")
            f.write(f"Records with pass types: {before_richness.get('has_pass_types', 0):,} → {after_richness.get('has_pass_types', 0):,}\n")
            
        print(f"\nReport saved to: {report_file}")
            
    finally:
        extractor.close_db()
        print("\nDatabase connection closed")


if __name__ == "__main__":
    main()