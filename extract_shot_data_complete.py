#!/usr/bin/env python3
"""
FBref HTML Shot Data Extractor for NWSL Database - COMPLETE VERSION
Extracts ALL shot-by-shot data from FBref HTML match files and populates the match_shot table.
Fixes issue where shots were being missed due to incomplete HTML parsing.
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

# HTML files directory - check both locations
HTML_DIRS = [
    '/Users/thomasmcmillan/projects/nwsl_db_migration/html_files',
    '/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files'
]


class CompleteShotDataExtractor:
    """Extracts ALL shot data from FBref HTML files with complete coverage."""
    
    def __init__(self, db_config: Dict, html_dirs: List[str]):
        self.db_config = db_config
        self.html_dirs = html_dirs
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
            WHERE player_id IS NOT NULL
        """)
        for row in self.cursor.fetchall():
            self.player_mapping[row[0]] = {'uuid': row[1], 'name': row[2]}
        logger.info(f"Loaded {len(self.player_mapping)} player mappings")
        
        # Load team mappings
        self.cursor.execute("""
            SELECT DISTINCT team_id, team_name_season_1 
            FROM team_season
            WHERE team_id IS NOT NULL
        """)
        self.team_mapping = {row[0]: row[1] for row in self.cursor.fetchall()}
        logger.info(f"Loaded {len(self.team_mapping)} team mappings")
        
    def find_html_file(self, match_id: str) -> Optional[str]:
        """Find HTML file for a match ID across multiple directories."""
        html_file = f"match_{match_id}.html"
        for html_dir in self.html_dirs:
            filepath = os.path.join(html_dir, html_file)
            if os.path.exists(filepath):
                return filepath
        return None
        
    def extract_shot_data_from_html(self, filepath: str, match_id: str) -> List[Dict]:
        """Extract ALL shot data from HTML file using BeautifulSoup directly."""
        shots = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find the shots_all table
            shots_table = soup.find('table', {'id': 'shots_all'})
            if not shots_table:
                logger.debug(f"No shots_all table found in {filepath}")
                return shots
            
            # Find all data rows (tr elements with class containing 'shots_')
            shot_rows = shots_table.find_all('tr', class_=re.compile(r'shots_[a-z0-9]+'))
            
            logger.info(f"Found {len(shot_rows)} shot rows in HTML for match {match_id}")
            
            shot_index = 0
            for row in shot_rows:
                # Skip header rows
                if 'thead' in row.get('class', []) or 'over_header' in row.get('class', []):
                    continue
                    
                try:
                    shot = self.parse_shot_row_from_html(row, match_id, shot_index)
                    if shot:
                        shots.append(shot)
                        shot_index += 1
                except Exception as e:
                    logger.warning(f"Error parsing shot row {shot_index}: {e}")
                    continue
                    
            logger.info(f"Successfully extracted {len(shots)} shots from {filepath}")
            
            # Verify we got critical shots (for debugging)
            goals = [s for s in shots if s['outcome'] == 'Goal']
            logger.info(f"  - Found {len(goals)} goals in match {match_id}")
            for goal in goals:
                logger.info(f"    Goal: {goal['player_name']} at minute {goal['minute']}")
            
        except Exception as e:
            logger.error(f"Error extracting shots from {filepath}: {e}")
            
        return shots
        
    def parse_shot_row_from_html(self, row, match_id: str, shot_index: int) -> Optional[Dict]:
        """Parse a single shot row from the HTML directly."""
        try:
            # Extract data from td/th elements
            cells = row.find_all(['td', 'th'])
            if len(cells) < 13:  # Need at least 13 columns for basic shot data
                return None
            
            # Extract minute
            minute_text = cells[0].get_text(strip=True)
            minute = self.parse_minute(minute_text)
            
            # Extract player info - get both name and ID
            player_cell = cells[1]
            player_name = player_cell.get_text(strip=True)
            player_id = player_cell.get('data-append-csv', '')
            
            # If no ID in cell, check for link
            if not player_id:
                player_link = player_cell.find('a')
                if player_link and player_link.get('href'):
                    # Extract player ID from href like /en/players/fe95f6e7/Christen-Press
                    href_parts = player_link['href'].split('/')
                    if len(href_parts) >= 4 and href_parts[2] == 'players':
                        player_id = href_parts[3]
            
            # Extract team/squad
            squad_cell = cells[2]
            squad = squad_cell.get_text(strip=True)
            
            # Extract xG and PSxG
            xg = self.safe_float(cells[3].get_text(strip=True))
            psxg = self.safe_float(cells[4].get_text(strip=True))
            
            # Extract outcome
            outcome = cells[5].get_text(strip=True)
            
            # Extract distance
            distance = self.safe_float(cells[6].get_text(strip=True))
            
            # Extract body part
            body_part = cells[7].get_text(strip=True)
            
            # Extract notes
            notes = cells[8].get_text(strip=True) if cells[8].get_text(strip=True) else None
            
            # Extract SCA players and events if available
            sca1_player_name = None
            sca1_event = None
            sca2_player_name = None
            sca2_event = None
            
            if len(cells) > 9:
                sca1_player_cell = cells[9]
                sca1_player_name = sca1_player_cell.get_text(strip=True) or None
                
            if len(cells) > 10:
                sca1_event = cells[10].get_text(strip=True) or None
                
            if len(cells) > 11:
                sca2_player_cell = cells[11]
                sca2_player_name = sca2_player_cell.get_text(strip=True) or None
                
            if len(cells) > 12:
                sca2_event = cells[12].get_text(strip=True) or None
            
            # Map player UUID if we have the ID
            player_uuid = None
            if player_id and player_id in self.player_mapping:
                player_uuid = self.player_mapping[player_id]['uuid']
            
            shot = {
                'match_id': match_id,
                'minute': minute,
                'player_name': player_name,
                'player_id': player_id if player_id else None,
                'team_name': squad,  # Changed from 'squad' to 'team_name' to match schema
                'xg': xg,
                'psxg': psxg,
                'outcome': outcome,  # Changed from 'outcome_id' to 'outcome' to match schema
                'distance': int(distance) if distance else None,  # Convert to int for schema
                'body_part': body_part,
                'notes': notes,
                'sca1_player_name': sca1_player_name,
                'sca1_event': sca1_event,
                'sca2_player_name': sca2_player_name,
                'sca2_event': sca2_event,
                'player_uuid': player_uuid
            }
            
            # Only return if we have essential data
            if shot['minute'] is not None and shot['player_name']:
                return shot
                
        except Exception as e:
            logger.debug(f"Error parsing shot row: {e}")
            
        return None
        
    def parse_minute(self, minute_str: str) -> Optional[int]:
        """Parse minute string to integer, handling stoppage time."""
        if not minute_str:
            return None
        try:
            # Remove any non-numeric characters except + 
            minute_str = str(minute_str).strip()
            
            # Handle formats like "90+7"
            if '+' in minute_str:
                parts = minute_str.split('+')
                base = int(parts[0])
                added = int(parts[1]) if len(parts) > 1 else 0
                # Store as base + added (e.g., 90+7 = 97)
                return base + added
            
            # Regular minute
            return int(minute_str)
        except Exception as e:
            logger.debug(f"Could not parse minute '{minute_str}': {e}")
            return None
            
    def safe_float(self, value) -> Optional[float]:
        """Safely convert value to float."""
        if not value or value == '' or value == '—':
            return None
        try:
            # Remove any non-numeric characters except . and -
            value = str(value).strip()
            return float(value)
        except:
            return None
            
    def clear_existing_shots(self, match_id: str = None):
        """Clear existing shot data for a match or all matches."""
        try:
            if match_id:
                self.cursor.execute("DELETE FROM match_shot WHERE match_id = %s", (match_id,))
                logger.info(f"Cleared existing shots for match {match_id}")
            else:
                self.cursor.execute("DELETE FROM match_shot")
                logger.info("Cleared all existing shot data")
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error clearing shots: {e}")
            raise
            
    def insert_shots_to_db(self, shots: List[Dict]):
        """Insert shot data into database."""
        if not shots:
            return
            
        # Prepare insert query - let database generate UUID primary key
        insert_query = """
            INSERT INTO match_shot (
                match_id, minute, player_name, player_id, team_name,
                xg, psxg, outcome, distance, body_part, notes,
                sca1_player_name, sca1_event, sca2_player_name, sca2_event,
                player_uuid
            ) VALUES (
                %(match_id)s, %(minute)s, %(player_name)s, %(player_id)s, %(team_name)s,
                %(xg)s, %(psxg)s, %(outcome)s, %(distance)s, %(body_part)s, %(notes)s,
                %(sca1_player_name)s, %(sca1_event)s, %(sca2_player_name)s, %(sca2_event)s,
                %(player_uuid)s
            )
        """
        
        try:
            execute_batch(self.cursor, insert_query, shots)
            self.conn.commit()
            logger.info(f"Inserted/updated {len(shots)} shots in database")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting shots: {e}")
            raise
            
    def test_specific_match(self, match_id: str):
        """Test extraction for a specific match."""
        logger.info(f"Testing extraction for match {match_id}")
        
        # Find HTML file
        filepath = self.find_html_file(match_id)
        if not filepath:
            logger.error(f"HTML file not found for match {match_id}")
            return
            
        # Clear existing shots for this match
        self.clear_existing_shots(match_id)
        
        # Extract shots
        shots = self.extract_shot_data_from_html(filepath, match_id)
        
        if shots:
            # Insert to database
            self.insert_shots_to_db(shots)
            
            # Verify in database
            self.cursor.execute("""
                SELECT COUNT(*) as total_shots,
                       COUNT(CASE WHEN outcome = 'Goal' THEN 1 END) as goals,
                       COUNT(CASE WHEN minute > 90 THEN 1 END) as stoppage_time_shots
                FROM match_shot
                WHERE match_id = %s
            """, (match_id,))
            
            result = self.cursor.fetchone()
            logger.info(f"Database verification for {match_id}:")
            logger.info(f"  Total shots: {result[0]}")
            logger.info(f"  Goals: {result[1]}")
            logger.info(f"  Stoppage time shots: {result[2]}")
            
            # Show all goals
            self.cursor.execute("""
                SELECT minute, player_name, xg, psxg, distance, body_part
                FROM match_shot
                WHERE match_id = %s AND outcome = 'Goal'
                ORDER BY minute
            """, (match_id,))
            
            goals = self.cursor.fetchall()
            if goals:
                logger.info("Goals scored:")
                for goal in goals:
                    logger.info(f"  Min {goal[0]}: {goal[1]} (xG: {goal[2]}, PSxG: {goal[3]}, {goal[4]} yards, {goal[5]})")
        else:
            logger.warning(f"No shots found for match {match_id}")
            
    def process_all_matches(self):
        """Process all matches in the database."""
        # Get all matches
        self.cursor.execute("""
            SELECT match_id, match_date
            FROM match
            ORDER BY match_date
        """)
        matches = self.cursor.fetchall()
        logger.info(f"Found {len(matches)} total matches to process")
        
        total_shots_extracted = 0
        matches_with_shots = 0
        matches_without_html = 0
        matches_without_shots = 0
        
        for match_id, match_date in matches:
            # Find HTML file
            filepath = self.find_html_file(match_id)
            
            if not filepath:
                matches_without_html += 1
                logger.debug(f"No HTML file for {match_id} ({match_date})")
                continue
                
            # Clear existing shots for this match
            self.clear_existing_shots(match_id)
            
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
        logger.info(f"Matches without HTML files: {matches_without_html}")
        logger.info(f"Matches without shot data: {matches_without_shots}")
        logger.info(f"Total shots extracted: {total_shots_extracted}")
        logger.info("=" * 60)
        
    def verify_coverage(self):
        """Verify shot data coverage after extraction."""
        self.cursor.execute("""
            SELECT 
                COUNT(DISTINCT m.match_id) as total_matches,
                COUNT(DISTINCT ms.match_id) as matches_with_shots,
                COUNT(DISTINCT CASE WHEN ms.match_id IS NULL THEN m.match_id END) as matches_without_shots,
                COUNT(ms.id) as total_shots,
                COUNT(CASE WHEN ms.outcome = 'Goal' THEN 1 END) as total_goals
            FROM match m
            LEFT JOIN match_shot ms ON m.match_id = ms.match_id
        """)
        
        result = self.cursor.fetchone()
        total, with_shots, without_shots, total_shots, total_goals = result
        coverage = (with_shots / total) * 100 if total > 0 else 0
        
        logger.info("=" * 60)
        logger.info("COVERAGE VERIFICATION")
        logger.info(f"Total matches: {total}")
        logger.info(f"Matches with shot data: {with_shots}")
        logger.info(f"Matches without shot data: {without_shots}")
        logger.info(f"Coverage: {coverage:.1f}%")
        logger.info(f"Total shots in database: {total_shots}")
        logger.info(f"Total goals in database: {total_goals}")
        logger.info("=" * 60)
        
        # Check coverage by year
        self.cursor.execute("""
            SELECT 
                EXTRACT(YEAR FROM m.match_date) as year,
                COUNT(DISTINCT m.match_id) as total,
                COUNT(DISTINCT ms.match_id) as with_shots,
                COUNT(ms.id) as total_shots,
                COUNT(CASE WHEN ms.outcome = 'Goal' THEN 1 END) as goals
            FROM match m
            LEFT JOIN match_shot ms ON m.match_id = ms.match_id
            GROUP BY EXTRACT(YEAR FROM m.match_date)
            ORDER BY year
        """)
        
        logger.info("\nCoverage by Year:")
        for year, total, with_shots, total_shots, goals in self.cursor.fetchall():
            coverage = (with_shots / total) * 100 if total > 0 else 0
            avg_shots = total_shots / with_shots if with_shots > 0 else 0
            logger.info(f"  {int(year)}: {with_shots}/{total} matches ({coverage:.1f}%), "
                       f"{total_shots} shots ({avg_shots:.1f} avg), {goals} goals")


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract shot data from FBref HTML files')
    parser.add_argument('--test-match', type=str, help='Test extraction for a specific match ID')
    parser.add_argument('--clear-all', action='store_true', help='Clear all existing shot data before extraction')
    parser.add_argument('--process-all', action='store_true', help='Process all matches')
    args = parser.parse_args()
    
    logger.info("Starting Complete FBref Shot Data Extraction")
    logger.info("=" * 60)
    
    extractor = CompleteShotDataExtractor(DB_CONFIG, HTML_DIRS)
    
    try:
        # Connect to database
        extractor.connect_db()
        
        # Load mappings
        extractor.load_mappings()
        
        if args.test_match:
            # Test specific match
            extractor.test_specific_match(args.test_match)
        elif args.process_all:
            # Clear all if requested
            if args.clear_all:
                extractor.clear_existing_shots()
            
            # Process all matches
            logger.info("Processing all matches...")
            extractor.process_all_matches()
        else:
            # Default: test with the problematic match
            logger.info("Testing with match_07c68416 (known to have missing shots)...")
            extractor.test_specific_match('07c68416')
        
        # Always verify coverage at the end
        extractor.verify_coverage()
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)
        
    finally:
        extractor.close_db()
        
    logger.info("Extraction complete!")


if __name__ == "__main__":
    main()