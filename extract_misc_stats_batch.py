#!/usr/bin/env python3
"""
Batch extract FBref miscellaneous statistics from HTML files with progress tracking.
"""

import os
import re
import json
import psycopg2
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging
from pathlib import Path
import time
from io import StringIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'nwsl_data',
    'user': 'postgres',
    'password': 'postgres'
}

# FBref stat column mappings to database columns
STAT_MAPPINGS = {
    'cards_yellow': 'yellow_cards',
    'cards_red': 'red_cards', 
    'cards_yellow_red': 'second_yellow_cards',
    'fouls': 'fouls_committed',
    'fouled': 'fouled',
    'offsides': 'offsides',
    'crosses': 'crosses',
    'interceptions': 'interceptions',
    'tackles_won': 'tackles_won',
    'pens_won': 'penalty_kicks_won',
    'pens_conceded': 'penalty_kicks_conceded',
    'own_goals': 'own_goals',
    'ball_recoveries': 'ball_recoveries',
    'aerials_won': 'aerial_duels_won',
    'aerials_lost': 'aerial_duels_lost',
    'aerials_won_pct': 'aerial_duels_won_pct'
}

class BatchMiscStatsExtractor:
    """Batch extract miscellaneous statistics from FBref HTML files."""
    
    def __init__(self, batch_size=50):
        """Initialize the extractor with database connection."""
        self.conn = None
        self.batch_size = batch_size
        self.match_player_cache = {}  # Cache match_player IDs
        self.stats_extracted = 0
        self.files_processed = 0
        self.files_skipped = 0
        self.errors = []
        
    def connect_db(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
            
    def close_db(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
            
    def load_match_player_cache(self):
        """Pre-load all match_player IDs into cache for faster lookups."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT match_id, player_id, id 
            FROM match_player
        """)
        
        for match_id, player_id, mp_id in cursor.fetchall():
            key = f"{match_id}_{player_id}"
            self.match_player_cache[key] = mp_id
            
        cursor.close()
        logger.info(f"Loaded {len(self.match_player_cache)} match_player records into cache")
        
    def get_match_player_id(self, match_id: str, player_id: str) -> Optional[str]:
        """Get match_player ID from cache."""
        key = f"{match_id}_{player_id}"
        return self.match_player_cache.get(key)
        
    def extract_misc_stats_from_html(self, filepath: str) -> List[Dict]:
        """Extract miscellaneous statistics from an HTML file."""
        stats_records = []
        
        # Extract match ID from filename
        match_fbref_id = os.path.basename(filepath).replace('match_', '').replace('.html', '')
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all miscellaneous stats tables
            misc_tables = soup.find_all('table', id=re.compile(r'stats_.*_misc'))
            
            if not misc_tables:
                return stats_records
            
            for table in misc_tables:
                # Extract team ID from table ID
                table_id = table.get('id', '')
                team_match = re.search(r'stats_([a-f0-9]+)_misc', table_id)
                if not team_match:
                    continue
                    
                team_fbref_id = team_match.group(1)
                
                # Process each player row
                rows = table.find_all('tr')[2:]  # Skip header rows
                
                for row in rows:
                    player_cell = row.find('th', {'data-stat': 'player'})
                    if not player_cell:
                        continue
                        
                    # Extract player FBref ID
                    player_fbref_id = player_cell.get('data-append-csv')
                    if not player_fbref_id:
                        continue
                        
                    # Get match_player_id
                    match_player_id = self.get_match_player_id(match_fbref_id, player_fbref_id)
                    if not match_player_id:
                        continue
                        
                    # Extract stats
                    stats_record = {
                        'match_player_id': match_player_id
                    }
                    
                    # Extract each stat
                    for fbref_col, db_col in STAT_MAPPINGS.items():
                        stat_cell = row.find('td', {'data-stat': fbref_col})
                        if stat_cell:
                            value = stat_cell.text.strip()
                            
                            # Handle percentage values
                            if fbref_col == 'aerials_won_pct':
                                if value and value != '':
                                    try:
                                        stats_record[db_col] = float(value)
                                    except ValueError:
                                        stats_record[db_col] = None
                                else:
                                    stats_record[db_col] = None
                            else:
                                # Handle integer values
                                try:
                                    stats_record[db_col] = int(value) if value and value != '' else 0
                                except ValueError:
                                    stats_record[db_col] = 0
                        else:
                            # Set default values for missing columns
                            if db_col == 'aerial_duels_won_pct':
                                stats_record[db_col] = None
                            else:
                                stats_record[db_col] = 0
                                
                    stats_records.append(stats_record)
                    
        except Exception as e:
            logger.error(f"Error processing file {filepath}: {e}")
            self.errors.append({'file': filepath, 'error': str(e)})
            
        return stats_records
        
    def process_batch(self, batch_records: List[Dict]) -> tuple:
        """Process a batch of stats records."""
        inserted = 0
        updated = 0
        
        cursor = self.conn.cursor()
        
        # Check existing records
        match_player_ids = [r['match_player_id'] for r in batch_records]
        cursor.execute("""
            SELECT match_player_id FROM match_player_misc 
            WHERE match_player_id = ANY(%s::uuid[])
        """, (match_player_ids,))
        
        existing_ids = set(row[0] for row in cursor.fetchall())
        
        # Prepare batch insert/update
        insert_records = []
        update_records = []
        
        for record in batch_records:
            if record['match_player_id'] in existing_ids:
                update_records.append(record)
            else:
                insert_records.append(record)
                
        # Batch insert new records
        if insert_records:
            insert_sql = """
                INSERT INTO match_player_misc (
                    match_player_id, yellow_cards, red_cards, 
                    second_yellow_cards, fouls_committed, fouled, offsides,
                    crosses, interceptions, tackles_won, penalty_kicks_won,
                    penalty_kicks_conceded, own_goals, ball_recoveries,
                    aerial_duels_won, aerial_duels_lost, aerial_duels_won_pct
                ) VALUES %s
            """
            
            values = []
            for r in insert_records:
                values.append((
                    r['match_player_id'],
                    r.get('yellow_cards', 0),
                    r.get('red_cards', 0),
                    r.get('second_yellow_cards', 0),
                    r.get('fouls_committed', 0),
                    r.get('fouled', 0),
                    r.get('offsides', 0),
                    r.get('crosses', 0),
                    r.get('interceptions', 0),
                    r.get('tackles_won', 0),
                    r.get('penalty_kicks_won', 0),
                    r.get('penalty_kicks_conceded', 0),
                    r.get('own_goals', 0),
                    r.get('ball_recoveries', 0),
                    r.get('aerial_duels_won', 0),
                    r.get('aerial_duels_lost', 0),
                    r.get('aerial_duels_won_pct')
                ))
                
            from psycopg2.extras import execute_values
            execute_values(cursor, insert_sql, values)
            inserted = len(insert_records)
            
        # Batch update existing records
        for record in update_records:
            cursor.execute("""
                UPDATE match_player_misc SET
                    yellow_cards = %s,
                    red_cards = %s,
                    second_yellow_cards = %s,
                    fouls_committed = %s,
                    fouled = %s,
                    offsides = %s,
                    crosses = %s,
                    interceptions = %s,
                    tackles_won = %s,
                    penalty_kicks_won = %s,
                    penalty_kicks_conceded = %s,
                    own_goals = %s,
                    ball_recoveries = %s,
                    aerial_duels_won = %s,
                    aerial_duels_lost = %s,
                    aerial_duels_won_pct = %s
                WHERE match_player_id = %s
            """, (
                record.get('yellow_cards', 0),
                record.get('red_cards', 0),
                record.get('second_yellow_cards', 0),
                record.get('fouls_committed', 0),
                record.get('fouled', 0),
                record.get('offsides', 0),
                record.get('crosses', 0),
                record.get('interceptions', 0),
                record.get('tackles_won', 0),
                record.get('penalty_kicks_won', 0),
                record.get('penalty_kicks_conceded', 0),
                record.get('own_goals', 0),
                record.get('ball_recoveries', 0),
                record.get('aerial_duels_won', 0),
                record.get('aerial_duels_lost', 0),
                record.get('aerial_duels_won_pct'),
                record['match_player_id']
            ))
            
        updated = len(update_records)
        
        self.conn.commit()
        cursor.close()
        
        return inserted, updated
        
    def process_all_files(self, html_dir: str):
        """Process all HTML files in the directory with batch processing."""
        html_path = Path(html_dir)
        html_files = sorted(html_path.glob('match_*.html'))
        
        total_files = len(html_files)
        logger.info(f"Found {total_files} HTML files to process")
        
        # Load cache
        self.load_match_player_cache()
        
        batch_records = []
        start_time = time.time()
        
        for i, filepath in enumerate(html_files, 1):
            # Extract stats from file
            file_records = self.extract_misc_stats_from_html(str(filepath))
            
            if file_records:
                batch_records.extend(file_records)
                self.files_processed += 1
            else:
                self.files_skipped += 1
                
            # Process batch when it reaches batch size or at the end
            if len(batch_records) >= self.batch_size or i == total_files:
                if batch_records:
                    inserted, updated = self.process_batch(batch_records)
                    self.stats_extracted += inserted + updated
                    
                    # Progress update
                    elapsed = time.time() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (total_files - i) / rate if rate > 0 else 0
                    
                    logger.info(f"Progress: {i}/{total_files} files ({100*i/total_files:.1f}%) | "
                              f"Extracted: {self.stats_extracted} | "
                              f"Rate: {rate:.1f} files/sec | "
                              f"ETA: {eta/60:.1f} min")
                    
                    batch_records = []
                    
    def generate_report(self) -> Dict:
        """Generate extraction report."""
        cursor = self.conn.cursor()
        
        # Get statistics
        cursor.execute("SELECT COUNT(*) FROM match_player_misc")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM match_player_misc 
            WHERE ball_recoveries IS NOT NULL AND ball_recoveries > 0
        """)
        with_recoveries = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM match_player_misc 
            WHERE aerial_duels_won IS NOT NULL OR aerial_duels_lost IS NOT NULL
        """)
        with_aerials = cursor.fetchone()[0]
        
        cursor.close()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'files_processed': self.files_processed,
            'files_skipped': self.files_skipped,
            'stats_extracted': self.stats_extracted,
            'total_records': total_records,
            'records_with_recoveries': with_recoveries,
            'records_with_aerials': with_aerials,
            'errors': self.errors[:10]  # Only include first 10 errors
        }
        
        return report
        
def main():
    """Main execution function."""
    extractor = BatchMiscStatsExtractor(batch_size=100)
    
    try:
        # Connect to database
        extractor.connect_db()
        
        # Process all files
        extractor.process_all_files('/Users/thomasmcmillan/projects/nwsl_db_migration/html_files/')
        
        # Generate report
        report = extractor.generate_report()
        
        # Save report
        report_file = '/Users/thomasmcmillan/projects/nwsl_db_migration/misc_stats_batch_report.json'
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
            
        # Print summary
        print("\n" + "="*60)
        print("MISCELLANEOUS STATS EXTRACTION SUMMARY")
        print("="*60)
        print(f"Files processed: {report['files_processed']}")
        print(f"Files skipped: {report['files_skipped']}")
        print(f"Stats extracted: {report['stats_extracted']}")
        print(f"Total records in database: {report['total_records']}")
        print(f"Records with ball recoveries: {report['records_with_recoveries']}")
        print(f"Records with aerial duels: {report['records_with_aerials']}")
        
        if report['errors']:
            print(f"\nErrors encountered: {len(extractor.errors)} (showing first 10)")
            for error in report['errors']:
                print(f"  - {error['file']}: {error['error']}")
                
        print(f"\nDetailed report saved to: {report_file}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        extractor.close_db()
        
if __name__ == "__main__":
    main()