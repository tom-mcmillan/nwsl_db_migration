#!/usr/bin/env python3
"""
Comprehensive FBref Pass Types Data Extraction Script
Extracts all 25 columns from FBref HTML pass types tables
Handles missing columns gracefully for older match files
"""

import os
import re
import json
import psycopg2
from psycopg2.extras import execute_batch
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

# FBref column to database column mapping
COLUMN_MAPPING = {
    # Core Pass Types
    'passes': 'passes',                    # Total passes attempted
    'passes_live': 'passes_live',          # Live-ball passes
    'passes_dead': 'passes_dead',          # Dead-ball passes
    'passes_free_kicks': 'passes_free_kicks',  # Passes from free kicks
    'through_balls': 'through_balls',      # Through balls
    'passes_switches': 'passes_switches',  # 40+ yard width passes
    'crosses': 'crosses',                  # Crosses
    
    # Set Pieces
    'throw_ins': 'throw_ins',              # Throw-ins taken
    'corner_kicks': 'corner_kicks',        # Total corner kicks
    'corner_kicks_in': 'corner_kicks_in',  # Inswinging corners
    'corner_kicks_out': 'corner_kicks_out', # Outswinging corners
    'corner_kicks_straight': 'corner_kicks_straight',  # Straight corners
    
    # Pass Outcomes
    'passes_completed': 'passes_completed',  # Total completed passes
    'passes_offsides': 'passes_offsides',   # Passes resulting in offside
    'passes_blocked': 'passes_blocked',      # Passes blocked by opponent
    
    # Advanced Metrics (may not be in all files)
    'passes_pressure': 'passes_pressure',    # Passes under pressure
    'passes_ground': 'passes_ground',        # Ground passes
    'passes_low': 'passes_low',              # Low passes
    'passes_high': 'passes_high',            # High passes
}

# Additional columns that might appear in newer formats
ADDITIONAL_COLUMNS = [
    'passes_left_foot', 'passes_right_foot', 'passes_head',
    'passes_other_body', 'passes_cutback', 'passes_deflected',
    'passes_launched', 'passes_cross', 'passes_ti', 'passes_corner'
]

class PassTypesExtractor:
    def __init__(self):
        self.conn = None
        self.extracted_data = []
        self.extraction_stats = {
            'files_processed': 0,
            'tables_found': 0,
            'rows_extracted': 0,
            'columns_found': set(),
            'missing_columns': set(),
            'errors': []
        }
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**DB_PARAMS)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
            
    def close_db(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
            
    def extract_match_id(self, filename: str) -> Optional[str]:
        """Extract match ID from filename"""
        match = re.search(r'match_([a-f0-9]+)\.html', filename)
        return match.group(1) if match else None
        
    def extract_team_id(self, table_id: str) -> Optional[str]:
        """Extract team FBref ID from table ID"""
        # Format: stats_{team_id}_passing_types
        match = re.search(r'stats_([a-f0-9]+)_passing_types', table_id)
        return match.group(1) if match else None
        
    def get_match_player_id(self, match_fbref_id: str, player_fbref_id: str) -> Optional[str]:
        """Get match_player UUID for the given match and player FBref IDs"""
        cursor = self.conn.cursor()
        
        # Get match_player UUID
        cursor.execute("""
            SELECT id FROM match_player 
            WHERE match_id = %s AND player_id = %s
        """, (match_fbref_id, player_fbref_id))
        result = cursor.fetchone()
        match_player_id = result[0] if result else None
        
        cursor.close()
        
        return match_player_id
        
    def parse_pass_types_table(self, table, match_fbref_id: str, team_fbref_id: str) -> List[Dict]:
        """Parse a single pass types table and extract all available columns"""
        rows_data = []
        
        try:
            # Convert to DataFrame
            df = pd.read_html(str(table))[0]
            
            # Handle multi-level column headers
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten column names
                df.columns = ['_'.join(col).strip('_').lower() if col[1] != '' else col[0].lower() 
                             for col in df.columns.values]
            else:
                df.columns = [col.lower() for col in df.columns]
            
            # Log available columns for debugging
            available_columns = set(df.columns)
            self.extraction_stats['columns_found'].update(available_columns)
            
            # Process each row
            tbody = table.find('tbody')
            if tbody:
                for idx, row in enumerate(tbody.find_all('tr')):
                    player_cell = row.find('th', {'data-stat': 'player'})
                    if not player_cell:
                        continue
                        
                    # Extract player FBref ID
                    player_fbref_id = player_cell.get('data-append-csv', '')
                    if not player_fbref_id:
                        continue
                        
                    # Get match_player ID
                    match_player_id = self.get_match_player_id(match_fbref_id, player_fbref_id)
                    
                    if not match_player_id:
                        logger.warning(f"No match_player record for match {match_fbref_id}, player {player_fbref_id}")
                        continue
                    
                    # Extract all available pass types columns
                    row_data = {
                        'match_player_id': match_player_id
                    }
                    
                    # Extract each mapped column
                    for fbref_col, db_col in COLUMN_MAPPING.items():
                        cell = row.find('td', {'data-stat': fbref_col})
                        if cell:
                            value = cell.text.strip()
                            # Convert to integer, handling empty strings and zeros
                            if value and value != '':
                                try:
                                    row_data[db_col] = int(value) if value != '0' and not value.startswith('0.') else (0 if value == '0' else None)
                                except ValueError:
                                    row_data[db_col] = None
                            else:
                                row_data[db_col] = None
                        else:
                            # Column not found in this table
                            self.extraction_stats['missing_columns'].add(fbref_col)
                            row_data[db_col] = None
                    
                    # Set metadata
                    row_data['data_source'] = 'fbref_html'
                    
                    # Determine completeness based on available data
                    # Consider data complete if we have at least the core columns
                    core_columns = ['passes', 'passes_live', 'passes_dead', 'crosses', 'passes_completed']
                    has_core_data = any(row_data.get(col) is not None for col in core_columns)
                    row_data['is_complete'] = has_core_data
                    
                    rows_data.append(row_data)
                    self.extraction_stats['rows_extracted'] += 1
                    
        except Exception as e:
            logger.error(f"Error parsing pass types table: {e}")
            self.extraction_stats['errors'].append(str(e))
            
        return rows_data
        
    def process_file(self, filepath: str) -> int:
        """Process a single HTML file and extract pass types data"""
        try:
            match_fbref_id = self.extract_match_id(os.path.basename(filepath))
            if not match_fbref_id:
                logger.warning(f"Could not extract match ID from {filepath}")
                return 0
                
            with open(filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all pass types tables
            pass_types_tables = soup.find_all('table', id=re.compile(r'stats_.*_passing_types'))
            
            if not pass_types_tables:
                logger.info(f"No pass types tables found in {filepath}")
                return 0
                
            self.extraction_stats['tables_found'] += len(pass_types_tables)
            
            for table in pass_types_tables:
                table_id = table.get('id', '')
                team_fbref_id = self.extract_team_id(table_id)
                
                if not team_fbref_id:
                    logger.warning(f"Could not extract team ID from table {table_id}")
                    continue
                    
                # Extract data from this table
                table_data = self.parse_pass_types_table(table, match_fbref_id, team_fbref_id)
                self.extracted_data.extend(table_data)
                
            return len(pass_types_tables)
            
        except Exception as e:
            logger.error(f"Error processing file {filepath}: {e}")
            self.extraction_stats['errors'].append(f"{filepath}: {str(e)}")
            return 0
            
    def check_existing_data(self) -> Dict:
        """Check current state of pass types data in database"""
        cursor = self.conn.cursor()
        
        # Count total records
        cursor.execute("SELECT COUNT(*) FROM match_player_pass_types")
        total_records = cursor.fetchone()[0]
        
        # Count records with rich data (more than just basic columns)
        cursor.execute("""
            SELECT COUNT(*) FROM match_player_pass_types 
            WHERE passes_pressure IS NOT NULL 
               OR passes_ground IS NOT NULL 
               OR passes_high IS NOT NULL
               OR corner_kicks_in IS NOT NULL
        """)
        rich_records = cursor.fetchone()[0]
        
        # Get unique matches through match_player table
        cursor.execute("""
            SELECT COUNT(DISTINCT mp.match_id) 
            FROM match_player_pass_types mppt
            JOIN match_player mp ON mppt.match_player_id = mp.id
        """)
        unique_matches = cursor.fetchone()[0]
        
        # Sample some records to show current state
        cursor.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(passes) as has_passes,
                COUNT(passes_live) as has_live,
                COUNT(passes_dead) as has_dead,
                COUNT(through_balls) as has_through_balls,
                COUNT(passes_switches) as has_switches,
                COUNT(corner_kicks_in) as has_corner_in,
                COUNT(corner_kicks_out) as has_corner_out,
                COUNT(passes_pressure) as has_pressure,
                COUNT(passes_ground) as has_ground,
                COUNT(passes_high) as has_high
            FROM match_player_pass_types
        """)
        
        stats = cursor.fetchone()
        cursor.close()
        
        return {
            'total_records': total_records,
            'rich_records': rich_records,
            'unique_matches': unique_matches,
            'total_rows': stats[0],
            'column_coverage': {
                'passes': stats[1],
                'passes_live': stats[2],
                'passes_dead': stats[3],
                'through_balls': stats[4],
                'passes_switches': stats[5],
                'corner_kicks_in': stats[6],
                'corner_kicks_out': stats[7],
                'passes_pressure': stats[8],
                'passes_ground': stats[9],
                'passes_high': stats[10]
            }
        }
        
    def insert_data(self):
        """Insert extracted data into database"""
        if not self.extracted_data:
            logger.info("No data to insert")
            return
            
        cursor = self.conn.cursor()
        
        # Prepare insert query with all columns
        insert_query = """
            INSERT INTO match_player_pass_types (
                match_player_id,
                passes, passes_live, passes_dead, passes_free_kicks,
                through_balls, passes_switches, crosses,
                throw_ins, corner_kicks, corner_kicks_in, corner_kicks_out, corner_kicks_straight,
                passes_completed, passes_offsides, passes_blocked,
                passes_pressure, passes_ground, passes_low, passes_high,
                data_source, is_complete,
                created_at, updated_at
            ) VALUES (
                %(match_player_id)s,
                %(passes)s, %(passes_live)s, %(passes_dead)s, %(passes_free_kicks)s,
                %(through_balls)s, %(passes_switches)s, %(crosses)s,
                %(throw_ins)s, %(corner_kicks)s, %(corner_kicks_in)s, %(corner_kicks_out)s, %(corner_kicks_straight)s,
                %(passes_completed)s, %(passes_offsides)s, %(passes_blocked)s,
                %(passes_pressure)s, %(passes_ground)s, %(passes_low)s, %(passes_high)s,
                %(data_source)s, %(is_complete)s,
                NOW(), NOW()
            )
            ON CONFLICT (match_player_id) 
            DO UPDATE SET
                passes = EXCLUDED.passes,
                passes_live = EXCLUDED.passes_live,
                passes_dead = EXCLUDED.passes_dead,
                passes_free_kicks = EXCLUDED.passes_free_kicks,
                through_balls = EXCLUDED.through_balls,
                passes_switches = EXCLUDED.passes_switches,
                crosses = EXCLUDED.crosses,
                throw_ins = EXCLUDED.throw_ins,
                corner_kicks = EXCLUDED.corner_kicks,
                corner_kicks_in = EXCLUDED.corner_kicks_in,
                corner_kicks_out = EXCLUDED.corner_kicks_out,
                corner_kicks_straight = EXCLUDED.corner_kicks_straight,
                passes_completed = EXCLUDED.passes_completed,
                passes_offsides = EXCLUDED.passes_offsides,
                passes_blocked = EXCLUDED.passes_blocked,
                passes_pressure = EXCLUDED.passes_pressure,
                passes_ground = EXCLUDED.passes_ground,
                passes_low = EXCLUDED.passes_low,
                passes_high = EXCLUDED.passes_high,
                data_source = EXCLUDED.data_source,
                is_complete = EXCLUDED.is_complete,
                updated_at = NOW()
        """
        
        try:
            # Ensure all required fields are present
            for row in self.extracted_data:
                # Add any missing columns with None values
                for col in COLUMN_MAPPING.values():
                    if col not in row:
                        row[col] = None
                        
            execute_batch(cursor, insert_query, self.extracted_data, page_size=100)
            self.conn.commit()
            logger.info(f"Successfully inserted/updated {len(self.extracted_data)} records")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting data: {e}")
            raise
        finally:
            cursor.close()
            
    def generate_report(self) -> Dict:
        """Generate comprehensive extraction report"""
        report = {
            'extraction_summary': {
                'files_processed': self.extraction_stats['files_processed'],
                'tables_found': self.extraction_stats['tables_found'],
                'rows_extracted': self.extraction_stats['rows_extracted'],
                'unique_columns_found': len(self.extraction_stats['columns_found']),
                'columns_found': sorted(list(self.extraction_stats['columns_found'])),
                'missing_columns': sorted(list(self.extraction_stats['missing_columns'])),
                'errors_count': len(self.extraction_stats['errors'])
            }
        }
        
        if self.extraction_stats['errors']:
            report['errors'] = self.extraction_stats['errors'][:10]  # First 10 errors
            
        # Add database state after extraction
        if self.conn:
            report['database_state'] = self.check_existing_data()
            
        return report
        
    def test_single_file(self, filepath: str):
        """Test extraction with a single file"""
        logger.info(f"Testing extraction with {filepath}")
        
        self.connect_db()
        
        # Check initial state
        initial_state = self.check_existing_data()
        logger.info(f"Initial database state: {json.dumps(initial_state, indent=2)}")
        
        # Process the file
        tables_found = self.process_file(filepath)
        self.extraction_stats['files_processed'] = 1
        
        if self.extracted_data:
            # Show sample of extracted data
            logger.info(f"Sample extracted data (first record):")
            sample = self.extracted_data[0] if self.extracted_data else {}
            for key, value in sample.items():
                if value is not None:
                    logger.info(f"  {key}: {value}")
                    
            # Insert data
            self.insert_data()
            
            # Check final state
            final_state = self.check_existing_data()
            logger.info(f"Final database state: {json.dumps(final_state, indent=2)}")
            
        # Generate report
        report = self.generate_report()
        logger.info(f"Extraction report: {json.dumps(report, indent=2)}")
        
        self.close_db()
        
        return report
        
    def process_all_files(self, directory: str):
        """Process all HTML files in directory"""
        logger.info(f"Processing all files in {directory}")
        
        self.connect_db()
        
        # Check initial state
        initial_state = self.check_existing_data()
        logger.info(f"Initial database state: {json.dumps(initial_state, indent=2)}")
        
        # Get all HTML files
        html_files = [f for f in os.listdir(directory) if f.startswith('match_') and f.endswith('.html')]
        logger.info(f"Found {len(html_files)} HTML files to process")
        
        # Process each file
        for filename in html_files:
            filepath = os.path.join(directory, filename)
            self.process_file(filepath)
            self.extraction_stats['files_processed'] += 1
            
            # Insert data in batches
            if len(self.extracted_data) >= 1000:
                self.insert_data()
                self.extracted_data = []
                
        # Insert remaining data
        if self.extracted_data:
            self.insert_data()
            
        # Check final state
        final_state = self.check_existing_data()
        logger.info(f"Final database state: {json.dumps(final_state, indent=2)}")
        
        # Generate report
        report = self.generate_report()
        logger.info(f"Extraction report: {json.dumps(report, indent=2)}")
        
        self.close_db()
        
        return report


def main():
    """Main execution function"""
    import sys
    
    extractor = PassTypesExtractor()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        # Test mode with single file
        test_file = '/Users/thomasmcmillan/projects/nwsl_db_migration/html_files/match_07c68416.html'
        report = extractor.test_single_file(test_file)
        
        # Save report
        with open('pass_types_test_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        logger.info("Test report saved to pass_types_test_report.json")
        
    else:
        # Process all files
        html_dir = '/Users/thomasmcmillan/projects/nwsl_db_migration/html_files'
        report = extractor.process_all_files(html_dir)
        
        # Save report
        with open('pass_types_extraction_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        logger.info("Full extraction report saved to pass_types_extraction_report.json")


if __name__ == "__main__":
    main()