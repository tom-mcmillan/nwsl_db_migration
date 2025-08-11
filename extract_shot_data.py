#!/usr/bin/env python3
"""
FBref HTML Shot Data Extractor for NWSL Database
Extracts shot-by-shot data from FBref HTML match files and populates the match_shot table.
"""

import os
import sys
import json
import uuid
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import re
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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


class ShotDataExtractor:
    """Extracts shot data from FBref HTML files."""
    
    def __init__(self, db_config: Dict, html_dir: str):
        self.db_config = db_config
        self.html_dir = html_dir
        self.conn = None
        self.cursor = None
        self.player_mapping = {}
        self.team_mapping = {}
        
    def connect_db(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
            
    def close_db(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
        
    def load_mappings(self):
        """Load player and team mappings from database."""
        # Load player mappings (player_id is the FBref hex ID, id is the UUID)
        self.cursor.execute("""
            SELECT player_id, id, player_name 
            FROM player
        """)
        for row in self.cursor.fetchall():
            self.player_mapping[row[0]] = {'uuid': row[1], 'name': row[2]}
        logger.info(f"Loaded {len(self.player_mapping)} player mappings")
        
        # Load team mappings
        self.cursor.execute("""
            SELECT DISTINCT team_id, team_name_season_1 
            FROM team_season
        """)
        self.team_mapping = {row[0]: row[1] for row in self.cursor.fetchall()}
        logger.info(f"Loaded {len(self.team_mapping)} team mappings")
        
    def get_matches_without_shots(self) -> List[Tuple[str, datetime]]:
        """Get list of matches without shot data."""
        self.cursor.execute("""
            SELECT m.match_id, m.match_date
            FROM match m
            LEFT JOIN (SELECT DISTINCT match_id FROM match_shot) ms ON m.match_id = ms.match_id
            WHERE ms.match_id IS NULL
            ORDER BY m.match_date
        """)
        matches = self.cursor.fetchall()
        logger.info(f"Found {len(matches)} matches without shot data")
        return matches
        
    def extract_shot_data_from_html(self, filepath: str, match_id: str) -> List[Dict]:
        """Extract shot data from HTML file."""
        shots = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the main shots table
            shots_table = soup.find('table', {'id': 'shots_all'})
            if not shots_table:
                logger.debug(f"No shots_all table found in {filepath}")
                return shots
                
            # Parse table with pandas
            df = pd.read_html(StringIO(str(shots_table)))[0]
            
            # Process each shot row
            for idx, row in df.iterrows():
                try:
                    shot = self.parse_shot_row(row, match_id, idx)
                    if shot:
                        shots.append(shot)
                except Exception as e:
                    logger.warning(f"Error parsing shot row {idx}: {e}")
                    continue
                    
            logger.info(f"Extracted {len(shots)} shots from {filepath}")
            
        except Exception as e:
            logger.error(f"Error extracting shots from {filepath}: {e}")
            
        return shots
        
    def parse_shot_row(self, row: pd.Series, match_id: str, shot_index: int) -> Optional[Dict]:
        """Parse a single shot row from the dataframe."""
        try:
            # Generate unique shot ID based on match and index
            shot_id = f"{match_id}_{shot_index:03d}"
            
            shot = {
                'shot_id': shot_id,
                'match_id': match_id,
                'minute': self.parse_minute(row.get('Minute', '')),
                'player_name': row.get('Player', ''),
                'player_id': None,  # Will be extracted from HTML
                'squad': row.get('Squad', ''),
                'xg': self.safe_float(row.get('xG', None)),
                'psxg': self.safe_float(row.get('PSxG', None)),
                'outcome_id': row.get('Outcome', ''),
                'distance': self.safe_float(row.get('Distance', None)),
                'body_part': row.get('Body Part', ''),
                'notes': row.get('Notes', ''),
                'sca1_player_name': None,  # These are in nested columns
                'sca1_event': None,
                'sca2_player_name': None,
                'sca2_event': None,
                'player_uuid': None  # Will be mapped if player_id found
            }
            
            # Only return if we have essential data
            if shot['minute'] is not None and shot['player_name']:
                return shot
                
        except Exception as e:
            logger.debug(f"Error parsing shot row: {e}")
            
        return None
        
    def parse_minute(self, minute_str: str) -> Optional[int]:
        """Parse minute string to integer."""
        if not minute_str:
            return None
        try:
            # Handle formats like "45+2"
            if '+' in str(minute_str):
                parts = str(minute_str).split('+')
                return int(parts[0]) + int(parts[1])
            return int(minute_str)
        except:
            return None
            
    def safe_float(self, value) -> Optional[float]:
        """Safely convert value to float."""
        if pd.isna(value) or value == '':
            return None
        try:
            return float(value)
        except:
            return None
            
    def insert_shots_to_db(self, shots: List[Dict]):
        """Insert shot data into database."""
        if not shots:
            return
            
        # Prepare insert query matching actual table schema
        insert_query = """
            INSERT INTO match_shot (
                shot_id, match_id, minute, player_name, player_id, squad,
                xg, psxg, outcome_id, distance, body_part, notes,
                sca1_player_name, sca1_event, sca2_player_name, sca2_event,
                player_uuid
            ) VALUES (
                %(shot_id)s, %(match_id)s, %(minute)s, %(player_name)s, %(player_id)s, %(squad)s,
                %(xg)s, %(psxg)s, %(outcome_id)s, %(distance)s, %(body_part)s, %(notes)s,
                %(sca1_player_name)s, %(sca1_event)s, %(sca2_player_name)s, %(sca2_event)s,
                %(player_uuid)s
            )
            ON CONFLICT (shot_id) DO NOTHING
        """
        
        try:
            execute_batch(self.cursor, insert_query, shots)
            self.conn.commit()
            logger.info(f"Inserted {len(shots)} shots to database")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting shots: {e}")
            raise
            
    def process_matches(self, limit: Optional[int] = None):
        """Process matches without shot data."""
        matches = self.get_matches_without_shots()
        
        if limit:
            matches = matches[:limit]
            
        total_shots_extracted = 0
        matches_with_shots = 0
        matches_without_shots = 0
        
        for match_id, match_date in matches:
            # Check if HTML file exists
            html_file = f"match_{match_id}.html"
            filepath = os.path.join(self.html_dir, html_file)
            
            if not os.path.exists(filepath):
                logger.warning(f"HTML file not found: {html_file}")
                continue
                
            # Extract shot data
            shots = self.extract_shot_data_from_html(filepath, match_id)
            
            if shots:
                self.insert_shots_to_db(shots)
                total_shots_extracted += len(shots)
                matches_with_shots += 1
                logger.info(f"Processed {match_id} ({match_date}): {len(shots)} shots")
            else:
                matches_without_shots += 1
                logger.debug(f"No shots found for {match_id} ({match_date})")
                
        # Final report
        logger.info("=" * 60)
        logger.info("EXTRACTION COMPLETE")
        logger.info(f"Matches processed: {len(matches)}")
        logger.info(f"Matches with shot data: {matches_with_shots}")
        logger.info(f"Matches without shot data: {matches_without_shots}")
        logger.info(f"Total shots extracted: {total_shots_extracted}")
        logger.info("=" * 60)
        
    def verify_coverage(self):
        """Verify shot data coverage after extraction."""
        self.cursor.execute("""
            SELECT 
                COUNT(DISTINCT m.match_id) as total_matches,
                COUNT(DISTINCT ms.match_id) as matches_with_shots,
                COUNT(DISTINCT CASE WHEN ms.match_id IS NULL THEN m.match_id END) as matches_without_shots
            FROM match m
            LEFT JOIN (SELECT DISTINCT match_id FROM match_shot) ms ON m.match_id = ms.match_id
        """)
        
        result = self.cursor.fetchone()
        total, with_shots, without_shots = result
        coverage = (with_shots / total) * 100 if total > 0 else 0
        
        logger.info("=" * 60)
        logger.info("COVERAGE VERIFICATION")
        logger.info(f"Total matches: {total}")
        logger.info(f"Matches with shot data: {with_shots}")
        logger.info(f"Matches without shot data: {without_shots}")
        logger.info(f"Coverage: {coverage:.1f}%")
        logger.info("=" * 60)
        
        # Check coverage by year
        self.cursor.execute("""
            SELECT 
                EXTRACT(YEAR FROM m.match_date) as year,
                COUNT(DISTINCT m.match_id) as total,
                COUNT(DISTINCT ms.match_id) as with_shots
            FROM match m
            LEFT JOIN (SELECT DISTINCT match_id FROM match_shot) ms ON m.match_id = ms.match_id
            GROUP BY EXTRACT(YEAR FROM m.match_date)
            ORDER BY year
        """)
        
        logger.info("\nCoverage by Year:")
        for year, total, with_shots in self.cursor.fetchall():
            coverage = (with_shots / total) * 100 if total > 0 else 0
            logger.info(f"  {int(year)}: {with_shots}/{total} ({coverage:.1f}%)")


def main():
    """Main execution function."""
    logger.info("Starting FBref Shot Data Extraction")
    logger.info("=" * 60)
    
    extractor = ShotDataExtractor(DB_CONFIG, HTML_DIR)
    
    try:
        # Connect to database
        extractor.connect_db()
        
        # Load mappings
        extractor.load_mappings()
        
        # Process matches
        logger.info("Processing matches without shot data...")
        extractor.process_matches(limit=10)  # Start with 10 for testing
        
        # Verify coverage
        extractor.verify_coverage()
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)
        
    finally:
        extractor.close_db()
        
    logger.info("Extraction complete!")


if __name__ == "__main__":
    main()