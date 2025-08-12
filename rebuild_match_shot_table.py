#!/usr/bin/env python3
"""
Complete rebuild of match_shot table from FBref HTML files.
Extracts all shot data with proper standardization and validation.
"""

import os
import re
import json
import psycopg2
from psycopg2.extras import execute_values
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

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'nwsl_data'
}

# HTML files directory
HTML_DIR = '/Users/thomasmcmillan/projects/nwsl_db_migration/html_files'

class MatchShotExtractor:
    """Extract shot data from FBref HTML files."""
    
    def __init__(self):
        self.conn = None
        self.player_uuid_map = {}
        self.match_season_map = {}
        self.stats = {
            'files_processed': 0,
            'files_with_shots': 0,
            'total_shots_extracted': 0,
            'shots_with_player_uuid': 0,
            'shots_without_player_uuid': 0,
            'errors': []
        }
        
    def connect_db(self):
        """Establish database connection."""
        self.conn = psycopg2.connect(**DB_CONFIG)
        return self.conn
    
    def load_mappings(self):
        """Load player UUID mappings and match season mappings."""
        cur = self.conn.cursor()
        
        # Load player UUID mappings
        cur.execute("""
            SELECT player_id, id 
            FROM player 
            WHERE player_id IS NOT NULL AND id IS NOT NULL
        """)
        self.player_uuid_map = {row[0]: row[1] for row in cur.fetchall()}
        logger.info(f"Loaded {len(self.player_uuid_map)} player UUID mappings")
        
        # Load match season mappings
        cur.execute("""
            SELECT m.match_id, s.season_year 
            FROM match m
            LEFT JOIN season s ON m.season_uuid = s.id
            WHERE m.match_id IS NOT NULL
        """)
        self.match_season_map = {row[0]: row[1] for row in cur.fetchall()}
        logger.info(f"Loaded {len(self.match_season_map)} match season mappings")
        
        cur.close()
    
    def standardize_outcome(self, outcome: str) -> str:
        """Standardize shot outcome values."""
        if not outcome:
            return None
            
        outcome = outcome.strip().lower()
        
        # Remove 'so_' prefix if present
        if outcome.startswith('so_'):
            outcome = outcome[3:]
        
        # Map to standard values
        outcome_map = {
            'goal': 'Goal',
            'saved': 'Saved',
            'off target': 'Off Target',
            'off_target': 'Off Target',
            'blocked': 'Blocked',
            'woodwork': 'Woodwork',
            'saved off target': 'Saved off Target',
            'saved_off_target': 'Saved off Target',
            'unknown': 'Unknown'
        }
        
        return outcome_map.get(outcome, outcome.title())
    
    def extract_player_id(self, cell) -> Optional[str]:
        """Extract FBref player hex ID from table cell."""
        if cell and hasattr(cell, 'get'):
            player_id = cell.get('data-append-csv')
            if player_id:
                return player_id
        return None
    
    def extract_shots_from_file(self, filepath: str) -> List[Dict]:
        """Extract shot data from a single HTML file."""
        shots = []
        match_id = os.path.basename(filepath).replace('match_', '').replace('.html', '')
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            
            # Find shots_all table
            shots_table = soup.find('table', {'id': 'shots_all'})
            
            if not shots_table:
                return shots
            
            # Convert to DataFrame
            df = pd.read_html(str(shots_table))[0]
            
            # Find all rows in the table body
            tbody = shots_table.find('tbody')
            if not tbody:
                return shots
            
            rows = tbody.find_all('tr')
            
            for idx, row in enumerate(rows):
                # Skip header rows
                if 'thead' in row.get('class', []):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 13:  # Minimum required columns
                    continue
                
                try:
                    # Extract data from cells
                    minute_text = cells[0].get_text(strip=True)
                    minute = None
                    if minute_text and minute_text.isdigit():
                        minute = int(minute_text)
                    
                    # Player info
                    player_cell = cells[1]
                    player_name = player_cell.get_text(strip=True)
                    player_id = self.extract_player_id(player_cell)
                    
                    # Team info
                    team_name = cells[2].get_text(strip=True)
                    
                    # Shot details
                    xg_text = cells[3].get_text(strip=True)
                    xg = float(xg_text) if xg_text and xg_text not in ['', '—'] else None
                    
                    psxg_text = cells[4].get_text(strip=True)
                    psxg = float(psxg_text) if psxg_text and psxg_text not in ['', '—', '0.00'] else None
                    
                    outcome = self.standardize_outcome(cells[5].get_text(strip=True))
                    
                    distance_text = cells[6].get_text(strip=True)
                    distance = int(distance_text) if distance_text and distance_text.isdigit() else None
                    
                    body_part = cells[7].get_text(strip=True) or None
                    notes = cells[8].get_text(strip=True) or None
                    
                    # SCA1 info
                    sca1_player_name = cells[9].get_text(strip=True) or None
                    sca1_event = cells[10].get_text(strip=True) or None
                    
                    # SCA2 info
                    sca2_player_name = cells[11].get_text(strip=True) or None
                    sca2_event = cells[12].get_text(strip=True) or None
                    
                    # Get player UUID from mapping
                    player_uuid = self.player_uuid_map.get(player_id) if player_id else None
                    
                    # Get season year from match
                    season_year = self.match_season_map.get(match_id)
                    
                    shot = {
                        'match_id': match_id,
                        'minute': minute,
                        'player_name': player_name,
                        'player_id': player_id,
                        'player_uuid': player_uuid,
                        'team_name': team_name,
                        'xg': xg,
                        'psxg': psxg,
                        'outcome': outcome,
                        'distance': distance,
                        'body_part': body_part,
                        'notes': notes,
                        'sca1_player_name': sca1_player_name,
                        'sca1_event': sca1_event,
                        'sca2_player_name': sca2_player_name,
                        'sca2_event': sca2_event,
                        'season_year': season_year
                    }
                    
                    shots.append(shot)
                    
                except Exception as e:
                    logger.debug(f"Error parsing row {idx} in {filepath}: {e}")
                    continue
            
            if shots:
                self.stats['files_with_shots'] += 1
                self.stats['total_shots_extracted'] += len(shots)
                
                # Count UUID mappings
                for shot in shots:
                    if shot['player_uuid']:
                        self.stats['shots_with_player_uuid'] += 1
                    else:
                        self.stats['shots_without_player_uuid'] += 1
                
                logger.info(f"Extracted {len(shots)} shots from {os.path.basename(filepath)}")
            
        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")
            self.stats['errors'].append({
                'file': os.path.basename(filepath),
                'error': str(e)
            })
        
        return shots
    
    def create_new_table(self):
        """Drop and recreate the match_shot table with proper structure."""
        cur = self.conn.cursor()
        
        logger.info("Dropping existing match_shot table...")
        cur.execute("DROP TABLE IF EXISTS match_shot CASCADE")
        
        logger.info("Creating new match_shot table...")
        cur.execute("""
            CREATE TABLE match_shot (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                match_id TEXT REFERENCES match(match_id),
                minute INTEGER,
                player_name TEXT,
                player_id TEXT,
                player_uuid UUID REFERENCES player(id),
                team_name TEXT,
                xg REAL,
                psxg REAL,
                outcome TEXT,
                distance INTEGER,
                body_part TEXT,
                notes TEXT,
                sca1_player_name TEXT,
                sca1_event TEXT,
                sca2_player_name TEXT,
                sca2_event TEXT,
                season_year INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes
        logger.info("Creating indexes...")
        cur.execute("CREATE INDEX idx_match_shot_match_id ON match_shot(match_id)")
        cur.execute("CREATE INDEX idx_match_shot_player_id ON match_shot(player_id)")
        cur.execute("CREATE INDEX idx_match_shot_player_uuid ON match_shot(player_uuid)")
        cur.execute("CREATE INDEX idx_match_shot_season_year ON match_shot(season_year)")
        cur.execute("CREATE INDEX idx_match_shot_outcome ON match_shot(outcome)")
        
        self.conn.commit()
        cur.close()
        logger.info("New match_shot table created successfully")
    
    def insert_shots(self, shots: List[Dict]):
        """Insert shot data into the database."""
        if not shots:
            return
        
        cur = self.conn.cursor()
        
        # Prepare data for insertion
        values = []
        for shot in shots:
            values.append((
                shot['match_id'],
                shot['minute'],
                shot['player_name'],
                shot['player_id'],
                shot['player_uuid'],
                shot['team_name'],
                shot['xg'],
                shot['psxg'],
                shot['outcome'],
                shot['distance'],
                shot['body_part'],
                shot['notes'],
                shot['sca1_player_name'],
                shot['sca1_event'],
                shot['sca2_player_name'],
                shot['sca2_event'],
                shot['season_year']
            ))
        
        # Insert using execute_values for efficiency
        execute_values(
            cur,
            """
            INSERT INTO match_shot (
                match_id, minute, player_name, player_id, player_uuid,
                team_name, xg, psxg, outcome, distance, body_part, notes,
                sca1_player_name, sca1_event, sca2_player_name, sca2_event,
                season_year
            ) VALUES %s
            """,
            values
        )
        
        self.conn.commit()
        cur.close()
    
    def process_all_files(self):
        """Process all HTML files and extract shot data."""
        # Get all HTML files
        html_files = [
            os.path.join(HTML_DIR, f) 
            for f in os.listdir(HTML_DIR) 
            if f.startswith('match_') and f.endswith('.html')
        ]
        
        logger.info(f"Found {len(html_files)} HTML files to process")
        
        # Process in batches
        batch_size = 100
        all_shots = []
        
        for i in range(0, len(html_files), batch_size):
            batch_files = html_files[i:i+batch_size]
            batch_shots = []
            
            for filepath in batch_files:
                self.stats['files_processed'] += 1
                shots = self.extract_shots_from_file(filepath)
                batch_shots.extend(shots)
                
                if self.stats['files_processed'] % 50 == 0:
                    logger.info(f"Processed {self.stats['files_processed']}/{len(html_files)} files")
            
            # Insert batch
            if batch_shots:
                self.insert_shots(batch_shots)
                all_shots.extend(batch_shots)
                logger.info(f"Inserted {len(batch_shots)} shots from batch {i//batch_size + 1}")
        
        return all_shots
    
    def validate_data(self):
        """Validate the extracted data."""
        cur = self.conn.cursor()
        
        # Get statistics
        cur.execute("SELECT COUNT(*) FROM match_shot")
        total_shots = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT match_id) FROM match_shot")
        unique_matches = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM match_shot WHERE player_uuid IS NOT NULL")
        shots_with_uuid = cur.fetchone()[0]
        
        cur.execute("SELECT outcome, COUNT(*) FROM match_shot GROUP BY outcome ORDER BY COUNT(*) DESC")
        outcome_distribution = cur.fetchall()
        
        cur.execute("""
            SELECT season_year, COUNT(*) 
            FROM match_shot 
            WHERE season_year IS NOT NULL 
            GROUP BY season_year 
            ORDER BY season_year
        """)
        season_distribution = cur.fetchall()
        
        cur.close()
        
        return {
            'total_shots': total_shots,
            'unique_matches': unique_matches,
            'shots_with_uuid': shots_with_uuid,
            'shots_without_uuid': total_shots - shots_with_uuid,
            'outcome_distribution': outcome_distribution,
            'season_distribution': season_distribution
        }
    
    def run(self):
        """Main execution method."""
        logger.info("Starting match_shot table rebuild...")
        
        try:
            # Connect to database
            self.connect_db()
            
            # Load mappings
            self.load_mappings()
            
            # Create new table
            self.create_new_table()
            
            # Process all files
            logger.info("Processing HTML files...")
            all_shots = self.process_all_files()
            
            # Validate data
            logger.info("Validating extracted data...")
            validation = self.validate_data()
            
            # Generate report
            report = {
                'extraction_stats': self.stats,
                'validation': validation,
                'timestamp': datetime.now().isoformat()
            }
            
            # Save report
            report_path = '/Users/thomasmcmillan/projects/nwsl_db_migration/match_shot_rebuild_report.json'
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            # Print summary
            print("\n" + "="*60)
            print("MATCH_SHOT TABLE REBUILD COMPLETE")
            print("="*60)
            print(f"Files processed: {self.stats['files_processed']}")
            print(f"Files with shots: {self.stats['files_with_shots']}")
            print(f"Total shots extracted: {self.stats['total_shots_extracted']}")
            print(f"Shots with player UUID: {self.stats['shots_with_player_uuid']}")
            print(f"Shots without player UUID: {self.stats['shots_without_player_uuid']}")
            print(f"Errors encountered: {len(self.stats['errors'])}")
            print("\nDatabase Statistics:")
            print(f"Total shots in DB: {validation['total_shots']}")
            print(f"Unique matches: {validation['unique_matches']}")
            print(f"Shots with UUID mapping: {validation['shots_with_uuid']}")
            print("\nOutcome Distribution:")
            for outcome, count in validation['outcome_distribution'][:10]:
                print(f"  {outcome}: {count}")
            print("\nSeason Distribution:")
            for season, count in validation['season_distribution']:
                print(f"  {season}: {count}")
            print(f"\nDetailed report saved to: {report_path}")
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()


if __name__ == "__main__":
    extractor = MatchShotExtractor()
    extractor.run()