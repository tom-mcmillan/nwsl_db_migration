#!/usr/bin/env python3
"""
Tier 1 Data Extraction from FBref HTML Files
Extracts goalkeeper and shot data to fill critical database gaps
"""

import os
import re
import json
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
from collections import defaultdict
import traceback
import warnings
from io import StringIO

# Suppress pandas FutureWarning for read_html
warnings.filterwarnings('ignore', category=FutureWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tier1_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'nwsl_data'
}

HTML_DIR = '/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/'

class FBrefDataExtractor:
    """Main extractor class for FBref HTML data"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stats = {
            'goalkeeper': {'processed': 0, 'extracted': 0, 'inserted': 0, 'errors': 0},
            'shots': {'processed': 0, 'extracted': 0, 'inserted': 0, 'errors': 0}
        }
        self.cache = {
            'teams': {},  # hex_id -> uuid mapping
            'players': {},  # hex_id -> uuid mapping
            'matches': {}  # hex_id -> uuid mapping
        }
        
    def connect_db(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def close_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def load_cache(self):
        """Load existing mappings from database"""
        try:
            # Load team mappings (team_id is the hex ID, id is the UUID)
            self.cursor.execute("SELECT team_id, id FROM team")
            for row in self.cursor.fetchall():
                self.cache['teams'][row['team_id']] = row['id']
            logger.info(f"Loaded {len(self.cache['teams'])} team mappings")
            
            # Load player mappings (player_id is the hex ID, id is the UUID)
            self.cursor.execute("SELECT player_id, id FROM player")
            for row in self.cursor.fetchall():
                self.cache['players'][row['player_id']] = row['id']
            logger.info(f"Loaded {len(self.cache['players'])} player mappings")
            
            # Load match mappings (match_id is the hex ID)
            self.cursor.execute("SELECT match_id FROM match")
            for row in self.cursor.fetchall():
                # For matches, the hex ID is both the key and value since match_id is text
                self.cache['matches'][row['match_id']] = row['match_id']
            logger.info(f"Loaded {len(self.cache['matches'])} match mappings")
            
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
            
    def extract_match_id_from_filename(self, filename: str) -> Optional[str]:
        """Extract FBref match ID from filename"""
        match = re.search(r'match_([a-f0-9]{8})\.html', filename)
        return match.group(1) if match else None
    
    def parse_html_file(self, filepath: str) -> Optional[BeautifulSoup]:
        """Parse HTML file and return BeautifulSoup object"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return BeautifulSoup(f.read(), 'html.parser')
        except Exception as e:
            logger.error(f"Error parsing {filepath}: {e}")
            return None
    
    def extract_goalkeeper_data(self, soup: BeautifulSoup, match_hex_id: str) -> List[Dict]:
        """Extract goalkeeper statistics from HTML"""
        goalkeeper_data = []
        
        try:
            # Find all keeper_stats tables
            keeper_tables = soup.find_all('table', id=re.compile(r'keeper_stats_[a-f0-9]{8}'))
            
            for table in keeper_tables:
                table_id = table.get('id', '')
                # Extract team hex ID from table ID
                team_hex_match = re.search(r'keeper_stats_([a-f0-9]{8})', table_id)
                if not team_hex_match:
                    continue
                    
                team_hex_id = team_hex_match.group(1)
                
                # Convert table to DataFrame
                try:
                    df = pd.read_html(StringIO(str(table)))[0]
                    
                    # Handle multi-level columns if present
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(col).strip('_') for col in df.columns.values]
                    
                    # Process each row (goalkeeper)
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
                    
                    for idx, row in enumerate(rows):
                        # Skip separator rows
                        if 'spacer' in row.get('class', []):
                            continue
                        
                        # Extract player hex ID from data-append-csv
                        player_cell = row.find('th', {'data-append-csv': True})
                        if not player_cell:
                            continue
                            
                        player_hex_id = player_cell.get('data-append-csv')
                        if not player_hex_id:
                            continue
                        
                        # Get player name
                        player_name = player_cell.get_text(strip=True)
                        
                        # Extract statistics from DataFrame row
                        if idx < len(df):
                            row_data = df.iloc[idx].to_dict()
                            
                            # Handle different column name formats for multi-level columns
                            gk_stats = {
                                'match_hex_id': match_hex_id,
                                'team_hex_id': team_hex_id,
                                'player_hex_id': player_hex_id,
                                'player_name': player_name,
                                'minutes_played': self._parse_numeric(
                                    row_data.get(('Unnamed: 3_level_0', 'Min'), 
                                    row_data.get('Min', 
                                    row_data.get('Minutes', 0)))),
                                'shots_on_target_against': self._parse_numeric(
                                    row_data.get(('Shot Stopping', 'SoTA'),
                                    row_data.get('SoTA', 0))),
                                'goals_against': self._parse_numeric(
                                    row_data.get(('Shot Stopping', 'GA'),
                                    row_data.get('GA', 0))),
                                'saves': self._parse_numeric(
                                    row_data.get(('Shot Stopping', 'Saves'),
                                    row_data.get('Saves', 0))),
                                'save_percentage': self._parse_percentage(
                                    row_data.get(('Shot Stopping', 'Save%'),
                                    row_data.get('Save%', 0))),
                                'psxg': self._parse_numeric(
                                    row_data.get(('Shot Stopping', 'PSxG'),
                                    row_data.get('PSxG', 0))),
                                'psxg_per_sot': self._parse_numeric(row_data.get('PSxG/SoT', 0)),
                                'psxg_minus_ga': self._parse_numeric(row_data.get('PSxG+/-', 0)),
                                'passes_completed': self._parse_numeric(
                                    row_data.get(('Launched', 'Cmp'),
                                    row_data.get('Cmp', 0))),
                                'passes_attempted': self._parse_numeric(
                                    row_data.get(('Launched', 'Att'),
                                    row_data.get('Att', 0))),
                                'pass_completion_pct': self._parse_percentage(
                                    row_data.get(('Launched', 'Cmp%'),
                                    row_data.get('Cmp%', 0))),
                                'passes_att': self._parse_numeric(
                                    row_data.get(('Passes', 'Att (GK)'),
                                    row_data.get('Passes_Att', 0))),
                                'throws_att': self._parse_numeric(
                                    row_data.get(('Passes', 'Thr'),
                                    row_data.get('Thr', 0))),
                                'launch_pct': self._parse_percentage(
                                    row_data.get(('Passes', 'Launch%'),
                                    row_data.get('Launch%', 0))),
                                'avg_pass_length': self._parse_numeric(
                                    row_data.get(('Passes', 'AvgLen'),
                                    row_data.get('AvgLen', 0))),
                                'goal_kicks': self._parse_numeric(
                                    row_data.get(('Goal Kicks', 'Att'),
                                    row_data.get('Goal Kicks_Att', 0))),
                                'goal_kick_launch_pct': self._parse_percentage(
                                    row_data.get(('Goal Kicks', 'Launch%'),
                                    row_data.get('Goal Kicks_Launch%', 0))),
                                'goal_kick_avg_length': self._parse_numeric(
                                    row_data.get(('Goal Kicks', 'AvgLen'),
                                    row_data.get('Goal Kicks_AvgLen', 0))),
                                'crosses_faced': self._parse_numeric(
                                    row_data.get(('Crosses', 'Opp'),
                                    row_data.get('Crosses_Opp', 0))),
                                'crosses_stopped': self._parse_numeric(
                                    row_data.get(('Crosses', 'Stp'),
                                    row_data.get('Crosses_Stp', 0))),
                                'crosses_stopped_pct': self._parse_percentage(
                                    row_data.get(('Crosses', 'Stp%'),
                                    row_data.get('Crosses_Stp%', 0))),
                                'sweeper_opa': self._parse_numeric(
                                    row_data.get(('Sweeper', '#OPA'),
                                    row_data.get('Sweeper_#OPA', 0))),
                                'sweeper_avg_distance': self._parse_numeric(
                                    row_data.get(('Sweeper', 'AvgDist'),
                                    row_data.get('Sweeper_AvgDist', 0)))
                            }
                            
                            goalkeeper_data.append(gk_stats)
                            
                except Exception as e:
                    logger.error(f"Error processing goalkeeper table {table_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting goalkeeper data: {e}")
            
        return goalkeeper_data
    
    def extract_shot_data(self, soup: BeautifulSoup, match_hex_id: str) -> List[Dict]:
        """Extract shot-by-shot data from HTML (for 2022-2025 matches)"""
        shot_data = []
        
        try:
            # Look for shots tables (various formats across years)
            shots_tables = soup.find_all('table', id=re.compile(r'shots_[a-f0-9]{8}'))
            
            if not shots_tables:
                # Try alternative pattern
                shots_tables = soup.find_all('table', id='shots_all')
            
            for table in shots_tables:
                table_id = table.get('id', '')
                
                # Extract team hex ID if present
                team_hex_match = re.search(r'shots_([a-f0-9]{8})', table_id)
                squad_name = None
                
                # Convert table to DataFrame for easier parsing
                try:
                    df = pd.read_html(str(table))[0]
                    
                    # Handle multi-level columns
                    if isinstance(df.columns, pd.MultiIndex):
                        # Flatten column names
                        df.columns = [col[1] if col[0].startswith('Unnamed') else '_'.join(col).strip() for col in df.columns.values]
                    
                    # Find all shot rows in tbody
                    tbody = table.find('tbody')
                    if not tbody:
                        continue
                        
                    rows = tbody.find_all('tr')
                    
                    for idx, row in enumerate(rows):
                        # Skip separator rows
                        if 'spacer' in row.get('class', []):
                            continue
                        
                        # Extract player info from HTML row (for hex ID)
                        player_cell = None
                        cells = row.find_all(['td', 'th'])
                        
                        # Find player cell with data-append-csv
                        for cell in cells:
                            if cell.get('data-append-csv'):
                                player_cell = cell
                                break
                        
                        if not player_cell:
                            continue
                        
                        player_hex_id = player_cell.get('data-append-csv')
                        player_name = player_cell.get_text(strip=True)
                        
                        # Get data from DataFrame row
                        if idx < len(df):
                            row_data = df.iloc[idx].to_dict()
                            
                            # Extract shot details
                            shot_info = {
                                'match_hex_id': match_hex_id,
                                'player_hex_id': player_hex_id,
                                'player_name': player_name,
                                'squad': row_data.get('Squad', squad_name),
                                'minute': self._parse_minute(row_data.get('Minute')),
                                'xg': self._parse_numeric(row_data.get('xG')),
                                'psxg': self._parse_numeric(row_data.get('PSxG')),
                                'outcome': row_data.get('Outcome'),
                                'distance': self._parse_numeric(row_data.get('Distance')),
                                'body_part': row_data.get('Body Part'),
                                'notes': row_data.get('Notes'),
                                'sca1_player_name': row_data.get('SCA 1_Player', row_data.get('Player.1')),
                                'sca1_event': row_data.get('SCA 1_Event', row_data.get('Event')),
                                'sca2_player_name': row_data.get('SCA 2_Player', row_data.get('Player.2')),
                                'sca2_event': row_data.get('SCA 2_Event', row_data.get('Event.1'))
                            }
                            
                            # Only add if we have meaningful data
                            if shot_info['minute'] is not None or shot_info['xg'] is not None:
                                shot_data.append(shot_info)
                
                except Exception as e:
                    logger.error(f"Error processing shot table {table_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting shot data: {e}")
            
        return shot_data
    
    def _parse_minute(self, value) -> Optional[int]:
        """Parse minute value, handling extra time notation"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            # Remove + for extra time and convert to int
            minute_str = str(value).replace('+', '').strip()
            # Handle format like "45+2" by just taking the main minute
            if minute_str:
                return int(float(minute_str.split('+')[0]))
            return None
        except:
            return None
    
    def _parse_numeric(self, value) -> Optional[float]:
        """Parse numeric value from various formats"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            # Remove any non-numeric characters except . and -
            cleaned = re.sub(r'[^0-9.-]', '', str(value))
            return float(cleaned) if cleaned else None
        except:
            return None
    
    def _parse_percentage(self, value) -> Optional[float]:
        """Parse percentage value"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            # Remove % sign and convert
            cleaned = str(value).replace('%', '').strip()
            return float(cleaned) if cleaned else None
        except:
            return None
    
    def insert_goalkeeper_records(self, records: List[Dict]) -> int:
        """Insert goalkeeper records into database"""
        inserted = 0
        
        for record in records:
            try:
                # For match_goalkeeper_summary, we use hex IDs directly as the foreign keys are to text columns
                match_hex_id = record['match_hex_id']
                team_hex_id = record['team_hex_id']
                player_hex_id = record['player_hex_id']
                
                # Verify the IDs exist in database
                if match_hex_id not in self.cache['matches']:
                    logger.warning(f"Match {match_hex_id} not in database")
                    continue
                    
                if team_hex_id not in self.cache['teams']:
                    logger.warning(f"Team {team_hex_id} not in database")
                    continue
                    
                if player_hex_id not in self.cache['players']:
                    logger.warning(f"Player {player_hex_id} not in database")
                    continue
                
                # Check if record already exists
                self.cursor.execute("""
                    SELECT match_goalkeeper_id FROM match_goalkeeper_summary 
                    WHERE match_id = %s AND player_id = %s AND team_id = %s
                """, (match_hex_id, player_hex_id, team_hex_id))
                
                if self.cursor.fetchone():
                    logger.debug(f"Goalkeeper record already exists for {record['player_name']}")
                    continue
                
                # Generate unique ID for this record
                gk_id = f"{match_hex_id}_{player_hex_id}"
                
                # Insert new record with correct column names
                self.cursor.execute("""
                    INSERT INTO match_goalkeeper_summary (
                        match_goalkeeper_id, match_id, team_id, player_id, player_name,
                        minutes_played, shots_on_target_against, goals_against,
                        saves, save_percentage, post_shot_xg,
                        launched_cmp, launched_att, launched_cmp_pct,
                        passes_att, passes_thr, passes_launch_pct, passes_avg_len,
                        goal_kicks_att, goal_kicks_launch_pct, goal_kicks_avg_len,
                        crosses_opp, crosses_stp, crosses_stp_pct,
                        sweeper_opa, sweeper_avg_dist
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                """, (
                    gk_id, match_hex_id, team_hex_id, player_hex_id, record['player_name'],
                    record.get('minutes_played'),
                    record.get('shots_on_target_against'),
                    record.get('goals_against'),
                    record.get('saves'),
                    record.get('save_percentage'),
                    record.get('psxg'),  # post_shot_xg
                    record.get('passes_completed'),  # launched_cmp
                    record.get('passes_attempted'),  # launched_att
                    record.get('pass_completion_pct'),  # launched_cmp_pct
                    record.get('passes_att'),  # passes_att
                    record.get('throws_att'),  # passes_thr
                    record.get('launch_pct'),  # passes_launch_pct
                    record.get('avg_pass_length'),  # passes_avg_len
                    record.get('goal_kicks'),  # goal_kicks_att
                    record.get('goal_kick_launch_pct'),  # goal_kicks_launch_pct
                    record.get('goal_kick_avg_length'),  # goal_kicks_avg_len
                    record.get('crosses_faced'),  # crosses_opp
                    record.get('crosses_stopped'),  # crosses_stp
                    record.get('crosses_stopped_pct'),  # crosses_stp_pct
                    record.get('sweeper_opa'),  # sweeper_opa
                    record.get('sweeper_avg_distance')  # sweeper_avg_dist
                ))
                
                inserted += 1
                
            except Exception as e:
                logger.error(f"Error inserting goalkeeper record: {e}")
                self.conn.rollback()
                continue
        
        if inserted > 0:
            self.conn.commit()
            logger.info(f"Inserted {inserted} goalkeeper records")
        
        return inserted
    
    def insert_shot_records(self, records: List[Dict]) -> int:
        """Insert shot records into database"""
        inserted = 0
        
        for record in records:
            try:
                # Map hex IDs to UUIDs
                match_uuid = self.cache['matches'].get(record['match_hex_id'])
                player_uuid = self.cache['players'].get(record['player_hex_id'])
                
                if not match_uuid:
                    logger.warning(f"Missing match mapping for shot record: match={record['match_hex_id']}")
                    continue
                
                # Generate unique shot_id (combination of match, player, minute)
                shot_id = f"{record['match_hex_id']}_{record['player_hex_id']}_{record.get('minute', 0)}"
                
                # Check if record already exists
                self.cursor.execute("""
                    SELECT shot_id FROM match_shot 
                    WHERE shot_id = %s
                """, (shot_id,))
                
                if self.cursor.fetchone():
                    logger.debug(f"Shot record already exists: {shot_id}")
                    continue
                
                # Insert new shot record
                self.cursor.execute("""
                    INSERT INTO match_shot (
                        shot_id, match_id, minute, player_name, player_id,
                        squad, xg, psxg, outcome_id, distance, body_part, notes,
                        sca1_player_name, sca1_event, sca2_player_name, sca2_event,
                        player_uuid
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s
                    )
                """, (
                    shot_id,
                    record['match_hex_id'],  # Use hex ID directly as match_id is text
                    record.get('minute'),
                    record.get('player_name'),
                    record['player_hex_id'],  # Use hex ID directly as player_id is text
                    record.get('squad'),
                    record.get('xg'),
                    record.get('psxg'),
                    record.get('outcome'),
                    record.get('distance'),
                    record.get('body_part'),
                    record.get('notes'),
                    record.get('sca1_player_name'),
                    record.get('sca1_event'),
                    record.get('sca2_player_name'),
                    record.get('sca2_event'),
                    player_uuid  # Map to UUID for player_uuid column
                ))
                
                inserted += 1
                
            except Exception as e:
                logger.error(f"Error inserting shot record: {e}")
                logger.error(f"Record data: {record}")
                self.conn.rollback()
                continue
        
        if inserted > 0:
            self.conn.commit()
            logger.info(f"Inserted {inserted} shot records")
        
        return inserted
    
    def process_all_files(self, focus_years: List[int] = None):
        """Process all HTML files"""
        html_files = [f for f in os.listdir(HTML_DIR) if f.endswith('.html')]
        total_files = len(html_files)
        
        logger.info(f"Found {total_files} HTML files to process")
        
        for i, filename in enumerate(html_files, 1):
            if i % 100 == 0:
                logger.info(f"Progress: {i}/{total_files} files processed")
            
            filepath = os.path.join(HTML_DIR, filename)
            match_hex_id = self.extract_match_id_from_filename(filename)
            
            if not match_hex_id:
                logger.warning(f"Could not extract match ID from {filename}")
                continue
            
            # Check if match exists in database
            if match_hex_id not in self.cache['matches']:
                logger.debug(f"Match {match_hex_id} not in database, skipping")
                continue
            
            # Parse HTML
            soup = self.parse_html_file(filepath)
            if not soup:
                continue
            
            # Extract and insert goalkeeper data
            try:
                gk_data = self.extract_goalkeeper_data(soup, match_hex_id)
                if gk_data:
                    self.stats['goalkeeper']['extracted'] += len(gk_data)
                    inserted = self.insert_goalkeeper_records(gk_data)
                    self.stats['goalkeeper']['inserted'] += inserted
            except Exception as e:
                logger.error(f"Error processing goalkeeper data for {filename}: {e}")
                self.stats['goalkeeper']['errors'] += 1
            
            # Extract shot data (focus on recent years if specified)
            if focus_years:
                # Try to determine year from match data
                self.cursor.execute("""
                    SELECT EXTRACT(YEAR FROM match_date) as year 
                    FROM match 
                    WHERE match_id = %s
                """, (self.cache['matches'][match_hex_id],))
                result = self.cursor.fetchone()
                
                if result and result['year'] and int(result['year']) in focus_years:
                    try:
                        shot_data = self.extract_shot_data(soup, match_hex_id)
                        if shot_data:
                            self.stats['shots']['extracted'] += len(shot_data)
                            inserted = self.insert_shot_records(shot_data)
                            self.stats['shots']['inserted'] += inserted
                    except Exception as e:
                        logger.error(f"Error processing shot data for {filename}: {e}")
                        self.stats['shots']['errors'] += 1
            
            self.stats['goalkeeper']['processed'] += 1
            self.stats['shots']['processed'] += 1
    
    def generate_report(self):
        """Generate extraction report"""
        report = []
        report.append("\n" + "="*60)
        report.append("TIER 1 DATA EXTRACTION REPORT")
        report.append("="*60)
        report.append(f"Extraction completed at: {datetime.now()}")
        report.append("")
        
        # Goalkeeper stats
        report.append("GOALKEEPER DATA:")
        report.append(f"  Files processed: {self.stats['goalkeeper']['processed']}")
        report.append(f"  Records extracted: {self.stats['goalkeeper']['extracted']}")
        report.append(f"  Records inserted: {self.stats['goalkeeper']['inserted']}")
        report.append(f"  Errors: {self.stats['goalkeeper']['errors']}")
        
        # Check final count
        self.cursor.execute("SELECT COUNT(*) as count FROM match_goalkeeper_summary")
        final_count = self.cursor.fetchone()['count']
        report.append(f"  Final database count: {final_count}")
        report.append("")
        
        # Shot stats
        report.append("SHOT DATA:")
        report.append(f"  Files processed: {self.stats['shots']['processed']}")
        report.append(f"  Records extracted: {self.stats['shots']['extracted']}")
        report.append(f"  Records inserted: {self.stats['shots']['inserted']}")
        report.append(f"  Errors: {self.stats['shots']['errors']}")
        
        # Check final count
        self.cursor.execute("SELECT COUNT(*) as count FROM match_shot")
        final_count = self.cursor.fetchone()['count']
        report.append(f"  Final database count: {final_count}")
        report.append("")
        
        # Data quality checks
        report.append("DATA QUALITY CHECKS:")
        
        # Check for missing mappings
        unmapped_teams = set()
        unmapped_players = set()
        
        for filename in os.listdir(HTML_DIR):
            if not filename.endswith('.html'):
                continue
            filepath = os.path.join(HTML_DIR, filename)
            soup = self.parse_html_file(filepath)
            if soup:
                # Check for unmapped teams
                keeper_tables = soup.find_all('table', id=re.compile(r'keeper_stats_([a-f0-9]{8})'))
                for table in keeper_tables:
                    team_hex = re.search(r'keeper_stats_([a-f0-9]{8})', table.get('id', '')).group(1)
                    if team_hex not in self.cache['teams']:
                        unmapped_teams.add(team_hex)
        
        report.append(f"  Unmapped team IDs: {len(unmapped_teams)}")
        if unmapped_teams and len(unmapped_teams) < 10:
            report.append(f"    {list(unmapped_teams)[:10]}")
        
        report.append("="*60)
        
        return "\n".join(report)

def main():
    """Main execution function"""
    extractor = FBrefDataExtractor()
    
    try:
        # Connect to database
        if not extractor.connect_db():
            logger.error("Failed to connect to database")
            return
        
        # Load cache
        extractor.load_cache()
        
        # Process all files (focus on 2022-2025 for shots)
        logger.info("Starting Tier 1 data extraction...")
        extractor.process_all_files(focus_years=[2022, 2023, 2024, 2025])
        
        # Generate and print report
        report = extractor.generate_report()
        print(report)
        
        # Save report to file
        with open('tier1_extraction_report.txt', 'w') as f:
            f.write(report)
        
    except Exception as e:
        logger.error(f"Fatal error in extraction: {e}")
        traceback.print_exc()
    finally:
        extractor.close_db()

if __name__ == "__main__":
    main()