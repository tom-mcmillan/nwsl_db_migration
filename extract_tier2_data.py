#!/usr/bin/env python3
"""
Tier 2 Data Extraction from FBref HTML Files
Extracts and populates 5 empty performance tables with ~135,000 records
Targets: match_player_passing, match_player_possession, match_player_defensive_actions,
         match_player_misc, match_player_pass_types
"""

import os
import re
import json
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import logging
from collections import defaultdict
import traceback
import warnings
from io import StringIO
import uuid
import time

# Suppress pandas FutureWarning for read_html
warnings.filterwarnings('ignore', category=FutureWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tier2_extraction.log'),
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

# Table mapping for FBref tables to database tables
TABLE_MAPPING = {
    'passing': 'match_player_passing',
    'possession': 'match_player_possession',
    'defense': 'match_player_defensive_actions',
    'misc': 'match_player_misc',
    'passing_types': 'match_player_pass_types'
}

# Batch sizes for bulk operations
BATCH_SIZE = 1000
COPY_BUFFER_SIZE = 5000

class Tier2DataExtractor:
    """Main extractor class for Tier 2 performance data"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stats = {
            'passing': {'processed': 0, 'extracted': 0, 'inserted': 0, 'errors': 0},
            'possession': {'processed': 0, 'extracted': 0, 'inserted': 0, 'errors': 0},
            'defense': {'processed': 0, 'extracted': 0, 'inserted': 0, 'errors': 0},
            'misc': {'processed': 0, 'extracted': 0, 'inserted': 0, 'errors': 0},
            'pass_types': {'processed': 0, 'extracted': 0, 'inserted': 0, 'errors': 0}
        }
        self.cache = {
            'teams': {},  # hex_id -> uuid mapping
            'players': {},  # hex_id -> uuid mapping
            'matches': {},  # hex_id -> match_id mapping
            'match_players': {}  # (match_id, player_id) -> match_player_id
        }
        self.batch_data = defaultdict(list)
        self.total_records = 0
        self.start_time = None
        
    def connect_db(self):
        """Establish database connection with optimizations"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            
            # Set connection parameters for bulk operations
            self.cursor.execute("SET synchronous_commit TO OFF")
            self.cursor.execute("SET maintenance_work_mem TO '256MB'")
            self.cursor.execute("SET work_mem TO '128MB'")
            
            logger.info("Database connection established with bulk optimizations")
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
            # Load team mappings
            self.cursor.execute("SELECT team_id, id FROM team")
            for row in self.cursor.fetchall():
                self.cache['teams'][row['team_id']] = row['id']
            logger.info(f"Loaded {len(self.cache['teams'])} team mappings")
            
            # Load player mappings
            self.cursor.execute("SELECT player_id, id FROM player")
            for row in self.cursor.fetchall():
                self.cache['players'][row['player_id']] = row['id']
            logger.info(f"Loaded {len(self.cache['players'])} player mappings")
            
            # Load match mappings
            self.cursor.execute("SELECT match_id FROM match")
            for row in self.cursor.fetchall():
                self.cache['matches'][row['match_id']] = row['match_id']
            logger.info(f"Loaded {len(self.cache['matches'])} match mappings")
            
            # Load match_player mappings
            self.cursor.execute("SELECT match_player_id, match_id, player_id FROM match_player")
            for row in self.cursor.fetchall():
                key = (row['match_id'], row['player_id'])
                self.cache['match_players'][key] = row['match_player_id']
            logger.info(f"Loaded {len(self.cache['match_players'])} match_player mappings")
            
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
    
    def get_or_create_match_player_id(self, match_id: str, player_id: str, team_id: str = None) -> Optional[str]:
        """Get existing or create new match_player_id"""
        key = (match_id, player_id)
        
        # Check cache first
        if key in self.cache['match_players']:
            return self.cache['match_players'][key]
        
        # Check database
        self.cursor.execute("""
            SELECT match_player_id FROM match_player 
            WHERE match_id = %s AND player_id = %s
        """, (match_id, player_id))
        
        result = self.cursor.fetchone()
        if result:
            self.cache['match_players'][key] = result['match_player_id']
            return result['match_player_id']
        
        # Create new match_player record if needed
        match_player_id = f"{match_id}_{player_id}"
        
        try:
            # Insert new match_player record
            self.cursor.execute("""
                INSERT INTO match_player (match_player_id, match_id, player_id, team_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (match_player_id) DO NOTHING
                RETURNING match_player_id
            """, (match_player_id, match_id, player_id, team_id))
            
            self.conn.commit()
            self.cache['match_players'][key] = match_player_id
            logger.debug(f"Created new match_player record: {match_player_id}")
            return match_player_id
            
        except Exception as e:
            logger.error(f"Error creating match_player record: {e}")
            self.conn.rollback()
            return None
    
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
    
    def _parse_numeric(self, value) -> Optional[float]:
        """Parse numeric value from various formats"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            cleaned = re.sub(r'[^0-9.-]', '', str(value))
            return float(cleaned) if cleaned and cleaned != '-' else None
        except:
            return None
    
    def _parse_percentage(self, value) -> Optional[float]:
        """Parse percentage value"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            cleaned = str(value).replace('%', '').strip()
            return float(cleaned) if cleaned and cleaned != '-' else None
        except:
            return None
    
    def extract_passing_data(self, soup: BeautifulSoup, match_id: str) -> List[Dict]:
        """Extract passing statistics from HTML"""
        passing_data = []
        
        try:
            # Find all passing tables
            passing_tables = soup.find_all('table', id=re.compile(r'stats_[a-f0-9]{8}_passing'))
            
            for table in passing_tables:
                table_id = table.get('id', '')
                team_hex_match = re.search(r'stats_([a-f0-9]{8})_passing', table_id)
                if not team_hex_match:
                    continue
                
                team_hex_id = team_hex_match.group(1)
                
                try:
                    df = pd.read_html(StringIO(str(table)))[0]
                    
                    # Handle multi-level columns
                    if isinstance(df.columns, pd.MultiIndex):
                        # Flatten column names appropriately for passing stats
                        new_cols = []
                        for col in df.columns.values:
                            if col[0].startswith('Unnamed'):
                                new_cols.append(col[1])
                            else:
                                new_cols.append('_'.join([str(c) for c in col if c != '']))
                        df.columns = new_cols
                    
                    # Process each row
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
                    
                    for idx, row in enumerate(rows):
                        if 'spacer' in row.get('class', []):
                            continue
                        
                        # Extract player hex ID
                        player_cell = row.find('th', {'data-append-csv': True})
                        if not player_cell:
                            continue
                        
                        player_hex_id = player_cell.get('data-append-csv')
                        if not player_hex_id:
                            continue
                        
                        # Get or create match_player_id
                        match_player_id = self.get_or_create_match_player_id(match_id, player_hex_id, team_hex_id)
                        if not match_player_id:
                            continue
                        
                        # Extract statistics from DataFrame row
                        if idx < len(df):
                            row_data = df.iloc[idx].to_dict()
                            
                            passing_stats = {
                                'match_player_passing_id': f"{match_player_id}_passing",
                                'match_player_id': match_player_id,
                                'total_completed': self._parse_numeric(row_data.get('Total_Cmp', row_data.get('Cmp'))),
                                'total_attempted': self._parse_numeric(row_data.get('Total_Att', row_data.get('Att'))),
                                'total_completion_pct': self._parse_percentage(row_data.get('Total_Cmp%', row_data.get('Cmp%'))),
                                'total_distance': self._parse_numeric(row_data.get('Total_TotDist', row_data.get('TotDist'))),
                                'progressive_distance': self._parse_numeric(row_data.get('Total_PrgDist', row_data.get('PrgDist'))),
                                'short_completed': self._parse_numeric(row_data.get('Short_Cmp')),
                                'short_attempted': self._parse_numeric(row_data.get('Short_Att')),
                                'short_completion_pct': self._parse_percentage(row_data.get('Short_Cmp%')),
                                'medium_completed': self._parse_numeric(row_data.get('Medium_Cmp')),
                                'medium_attempted': self._parse_numeric(row_data.get('Medium_Att')),
                                'medium_completion_pct': self._parse_percentage(row_data.get('Medium_Cmp%')),
                                'long_completed': self._parse_numeric(row_data.get('Long_Cmp')),
                                'long_attempted': self._parse_numeric(row_data.get('Long_Att')),
                                'long_completion_pct': self._parse_percentage(row_data.get('Long_Cmp%')),
                                'assists': self._parse_numeric(row_data.get('Ast', row_data.get('A'))),
                                'xag': self._parse_numeric(row_data.get('xAG', row_data.get('xA'))),
                                'xa': self._parse_numeric(row_data.get('xA')),
                                'key_passes': self._parse_numeric(row_data.get('KP')),
                                'final_third_passes': self._parse_numeric(row_data.get('1/3')),
                                'penalty_area_passes': self._parse_numeric(row_data.get('PPA')),
                                'cross_penalty_area_passes': self._parse_numeric(row_data.get('CrsPA')),
                                'progressive_passes': self._parse_numeric(row_data.get('PrgP'))
                            }
                            
                            passing_data.append(passing_stats)
                            
                except Exception as e:
                    logger.error(f"Error processing passing table {table_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting passing data: {e}")
        
        return passing_data
    
    def extract_possession_data(self, soup: BeautifulSoup, match_id: str) -> List[Dict]:
        """Extract possession statistics from HTML"""
        possession_data = []
        
        try:
            # Find all possession tables
            possession_tables = soup.find_all('table', id=re.compile(r'stats_[a-f0-9]{8}_possession'))
            
            for table in possession_tables:
                table_id = table.get('id', '')
                team_hex_match = re.search(r'stats_([a-f0-9]{8})_possession', table_id)
                if not team_hex_match:
                    continue
                
                team_hex_id = team_hex_match.group(1)
                
                try:
                    df = pd.read_html(StringIO(str(table)))[0]
                    
                    # Handle multi-level columns
                    if isinstance(df.columns, pd.MultiIndex):
                        new_cols = []
                        for col in df.columns.values:
                            if col[0].startswith('Unnamed'):
                                new_cols.append(col[1])
                            else:
                                new_cols.append('_'.join([str(c) for c in col if c != '']))
                        df.columns = new_cols
                    
                    # Process each row
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
                    
                    for idx, row in enumerate(rows):
                        if 'spacer' in row.get('class', []):
                            continue
                        
                        # Extract player hex ID
                        player_cell = row.find('th', {'data-append-csv': True})
                        if not player_cell:
                            continue
                        
                        player_hex_id = player_cell.get('data-append-csv')
                        if not player_hex_id:
                            continue
                        
                        # Get or create match_player_id
                        match_player_id = self.get_or_create_match_player_id(match_id, player_hex_id, team_hex_id)
                        if not match_player_id:
                            continue
                        
                        # Extract statistics from DataFrame row
                        if idx < len(df):
                            row_data = df.iloc[idx].to_dict()
                            
                            possession_stats = {
                                'match_player_possession_id': f"{match_player_id}_possession",
                                'match_player_id': match_player_id,
                                'touches': self._parse_numeric(row_data.get('Touches_Touches', row_data.get('Touches'))),
                                'touches_def_penalty_area': self._parse_numeric(row_data.get('Touches_Def Pen', row_data.get('Def Pen'))),
                                'touches_def_3rd': self._parse_numeric(row_data.get('Touches_Def 3rd', row_data.get('Def 3rd'))),
                                'touches_mid_3rd': self._parse_numeric(row_data.get('Touches_Mid 3rd', row_data.get('Mid 3rd'))),
                                'touches_att_3rd': self._parse_numeric(row_data.get('Touches_Att 3rd', row_data.get('Att 3rd'))),
                                'touches_att_penalty_area': self._parse_numeric(row_data.get('Touches_Att Pen', row_data.get('Att Pen'))),
                                'touches_live_ball': self._parse_numeric(row_data.get('Touches_Live', row_data.get('Live'))),
                                'take_ons_attempted': self._parse_numeric(row_data.get('Take-Ons_Att', row_data.get('Dribbles_Att'))),
                                'take_ons_successful': self._parse_numeric(row_data.get('Take-Ons_Succ', row_data.get('Dribbles_Succ'))),
                                'take_on_success_pct': self._parse_percentage(row_data.get('Take-Ons_Succ%', row_data.get('Dribbles_Succ%'))),
                                'times_tackled': self._parse_numeric(row_data.get('Take-Ons_Tkld', row_data.get('Dribbles_Tkld'))),
                                'times_tackled_pct': self._parse_percentage(row_data.get('Take-Ons_Tkld%', row_data.get('Dribbles_Tkld%'))),
                                'carries': self._parse_numeric(row_data.get('Carries_Carries', row_data.get('Carries'))),
                                'total_carrying_distance': self._parse_numeric(row_data.get('Carries_TotDist', row_data.get('TotDist'))),
                                'progressive_carrying_distance': self._parse_numeric(row_data.get('Carries_PrgDist', row_data.get('PrgDist'))),
                                'progressive_carries': self._parse_numeric(row_data.get('Carries_PrgC', row_data.get('PrgC'))),
                                'carries_final_third': self._parse_numeric(row_data.get('Carries_1/3', row_data.get('1/3'))),
                                'carries_penalty_area': self._parse_numeric(row_data.get('Carries_CPA', row_data.get('CPA'))),
                                'miscontrols': self._parse_numeric(row_data.get('Carries_Mis', row_data.get('Mis'))),
                                'dispossessed': self._parse_numeric(row_data.get('Carries_Dis', row_data.get('Dis'))),
                                'passes_received': self._parse_numeric(row_data.get('Receiving_Rec', row_data.get('Rec'))),
                                'progressive_passes_received': self._parse_numeric(row_data.get('Receiving_PrgR', row_data.get('PrgR')))
                            }
                            
                            possession_data.append(possession_stats)
                            
                except Exception as e:
                    logger.error(f"Error processing possession table {table_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting possession data: {e}")
        
        return possession_data
    
    def extract_defensive_data(self, soup: BeautifulSoup, match_id: str) -> List[Dict]:
        """Extract defensive actions from HTML"""
        defensive_data = []
        
        try:
            # Find all defense tables
            defense_tables = soup.find_all('table', id=re.compile(r'stats_[a-f0-9]{8}_defense'))
            
            for table in defense_tables:
                table_id = table.get('id', '')
                team_hex_match = re.search(r'stats_([a-f0-9]{8})_defense', table_id)
                if not team_hex_match:
                    continue
                
                team_hex_id = team_hex_match.group(1)
                
                try:
                    df = pd.read_html(StringIO(str(table)))[0]
                    
                    # Handle multi-level columns
                    if isinstance(df.columns, pd.MultiIndex):
                        new_cols = []
                        for col in df.columns.values:
                            if col[0].startswith('Unnamed'):
                                new_cols.append(col[1])
                            else:
                                new_cols.append('_'.join([str(c) for c in col if c != '']))
                        df.columns = new_cols
                    
                    # Process each row
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
                    
                    for idx, row in enumerate(rows):
                        if 'spacer' in row.get('class', []):
                            continue
                        
                        # Extract player hex ID
                        player_cell = row.find('th', {'data-append-csv': True})
                        if not player_cell:
                            continue
                        
                        player_hex_id = player_cell.get('data-append-csv')
                        if not player_hex_id:
                            continue
                        
                        # Get or create match_player_id
                        match_player_id = self.get_or_create_match_player_id(match_id, player_hex_id, team_hex_id)
                        if not match_player_id:
                            continue
                        
                        # Extract statistics from DataFrame row
                        if idx < len(df):
                            row_data = df.iloc[idx].to_dict()
                            
                            defensive_stats = {
                                'match_player_defensive_actions_id': f"{match_player_id}_defense",
                                'match_player_id': match_player_id,
                                'tackles': self._parse_numeric(row_data.get('Tackles_Tkl', row_data.get('Tkl'))),
                                'tackles_won': self._parse_numeric(row_data.get('Tackles_TklW', row_data.get('TklW'))),
                                'tackles_def_3rd': self._parse_numeric(row_data.get('Tackles_Def 3rd', row_data.get('Def 3rd'))),
                                'tackles_mid_3rd': self._parse_numeric(row_data.get('Tackles_Mid 3rd', row_data.get('Mid 3rd'))),
                                'tackles_att_3rd': self._parse_numeric(row_data.get('Tackles_Att 3rd', row_data.get('Att 3rd'))),
                                'challenge_tackles': self._parse_numeric(row_data.get('Challenges_Tkl', row_data.get('Drib Tkl'))),
                                'challenges_attempted': self._parse_numeric(row_data.get('Challenges_Att', row_data.get('Drib Att'))),
                                'challenge_tackle_pct': self._parse_percentage(row_data.get('Challenges_Tkl%', row_data.get('Drib Tkl%'))),
                                'challenges_lost': self._parse_numeric(row_data.get('Challenges_Lost', row_data.get('Drib Past'))),
                                'blocks': self._parse_numeric(row_data.get('Blocks_Blocks', row_data.get('Blocks'))),
                                'shots_blocked': self._parse_numeric(row_data.get('Blocks_Sh', row_data.get('Sh'))),
                                'passes_blocked': self._parse_numeric(row_data.get('Blocks_Pass', row_data.get('Pass'))),
                                'interceptions': self._parse_numeric(row_data.get('Int', row_data.get('Interceptions'))),
                                'tackles_plus_interceptions': self._parse_numeric(row_data.get('Tkl+Int', row_data.get('Tkl+Int'))),
                                'clearances': self._parse_numeric(row_data.get('Clr', row_data.get('Clearances'))),
                                'errors': self._parse_numeric(row_data.get('Err', row_data.get('Errors')))
                            }
                            
                            defensive_data.append(defensive_stats)
                            
                except Exception as e:
                    logger.error(f"Error processing defense table {table_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting defensive data: {e}")
        
        return defensive_data
    
    def extract_misc_data(self, soup: BeautifulSoup, match_id: str) -> List[Dict]:
        """Extract miscellaneous statistics from HTML"""
        misc_data = []
        
        try:
            # Find all misc tables
            misc_tables = soup.find_all('table', id=re.compile(r'stats_[a-f0-9]{8}_misc'))
            
            for table in misc_tables:
                table_id = table.get('id', '')
                team_hex_match = re.search(r'stats_([a-f0-9]{8})_misc', table_id)
                if not team_hex_match:
                    continue
                
                team_hex_id = team_hex_match.group(1)
                
                try:
                    df = pd.read_html(StringIO(str(table)))[0]
                    
                    # Handle multi-level columns
                    if isinstance(df.columns, pd.MultiIndex):
                        new_cols = []
                        for col in df.columns.values:
                            if col[0].startswith('Unnamed'):
                                new_cols.append(col[1])
                            else:
                                new_cols.append('_'.join([str(c) for c in col if c != '']))
                        df.columns = new_cols
                    
                    # Process each row
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
                    
                    for idx, row in enumerate(rows):
                        if 'spacer' in row.get('class', []):
                            continue
                        
                        # Extract player hex ID
                        player_cell = row.find('th', {'data-append-csv': True})
                        if not player_cell:
                            continue
                        
                        player_hex_id = player_cell.get('data-append-csv')
                        if not player_hex_id:
                            continue
                        
                        # Get or create match_player_id
                        match_player_id = self.get_or_create_match_player_id(match_id, player_hex_id, team_hex_id)
                        if not match_player_id:
                            continue
                        
                        # Extract statistics from DataFrame row
                        if idx < len(df):
                            row_data = df.iloc[idx].to_dict()
                            
                            misc_stats = {
                                'match_player_misc_id': f"{match_player_id}_misc",
                                'match_player_id': match_player_id,
                                'yellow_cards': self._parse_numeric(row_data.get('Performance_CrdY', row_data.get('CrdY'))),
                                'red_cards': self._parse_numeric(row_data.get('Performance_CrdR', row_data.get('CrdR'))),
                                'second_yellow_cards': self._parse_numeric(row_data.get('Performance_2CrdY', row_data.get('2CrdY'))),
                                'fouls_committed': self._parse_numeric(row_data.get('Performance_Fls', row_data.get('Fls'))),
                                'fouls_drawn': self._parse_numeric(row_data.get('Performance_Fld', row_data.get('Fld'))),
                                'offsides': self._parse_numeric(row_data.get('Performance_Off', row_data.get('Off'))),
                                'crosses': self._parse_numeric(row_data.get('Performance_Crs', row_data.get('Crs'))),
                                'tackles_won': self._parse_numeric(row_data.get('Performance_TklW', row_data.get('TklW'))),
                                'interceptions': self._parse_numeric(row_data.get('Performance_Int', row_data.get('Int'))),
                                'penalty_kicks_won': self._parse_numeric(row_data.get('Performance_PKwon', row_data.get('PKwon'))),
                                'penalty_kicks_conceded': self._parse_numeric(row_data.get('Performance_PKcon', row_data.get('PKcon'))),
                                'own_goals': self._parse_numeric(row_data.get('Performance_OG', row_data.get('OG'))),
                                'ball_recoveries': self._parse_numeric(row_data.get('Performance_Recov', row_data.get('Recov'))),
                                'aerial_duels_won': self._parse_numeric(row_data.get('Aerial Duels_Won', row_data.get('Won'))),
                                'aerial_duels_lost': self._parse_numeric(row_data.get('Aerial Duels_Lost', row_data.get('Lost'))),
                                'aerial_duel_win_pct': self._parse_percentage(row_data.get('Aerial Duels_Won%', row_data.get('Won%')))
                            }
                            
                            misc_data.append(misc_stats)
                            
                except Exception as e:
                    logger.error(f"Error processing misc table {table_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting misc data: {e}")
        
        return misc_data
    
    def extract_pass_types_data(self, soup: BeautifulSoup, match_id: str) -> List[Dict]:
        """Extract pass types statistics from HTML"""
        pass_types_data = []
        
        try:
            # Find all passing_types tables
            pass_types_tables = soup.find_all('table', id=re.compile(r'stats_[a-f0-9]{8}_passing_types'))
            
            for table in pass_types_tables:
                table_id = table.get('id', '')
                team_hex_match = re.search(r'stats_([a-f0-9]{8})_passing_types', table_id)
                if not team_hex_match:
                    continue
                
                team_hex_id = team_hex_match.group(1)
                
                try:
                    df = pd.read_html(StringIO(str(table)))[0]
                    
                    # Handle multi-level columns
                    if isinstance(df.columns, pd.MultiIndex):
                        new_cols = []
                        for col in df.columns.values:
                            if col[0].startswith('Unnamed'):
                                new_cols.append(col[1])
                            else:
                                new_cols.append('_'.join([str(c) for c in col if c != '']))
                        df.columns = new_cols
                    
                    # Process each row
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
                    
                    for idx, row in enumerate(rows):
                        if 'spacer' in row.get('class', []):
                            continue
                        
                        # Extract player hex ID
                        player_cell = row.find('th', {'data-append-csv': True})
                        if not player_cell:
                            continue
                        
                        player_hex_id = player_cell.get('data-append-csv')
                        if not player_hex_id:
                            continue
                        
                        # Get or create match_player_id
                        match_player_id = self.get_or_create_match_player_id(match_id, player_hex_id, team_hex_id)
                        if not match_player_id:
                            continue
                        
                        # Extract statistics from DataFrame row
                        if idx < len(df):
                            row_data = df.iloc[idx].to_dict()
                            
                            pass_types_stats = {
                                'match_player_pass_types_id': f"{match_player_id}_pass_types",
                                'match_player_id': match_player_id,
                                'pass_attempts': self._parse_numeric(row_data.get('Pass Types_Att', row_data.get('Att'))),
                                'live_passes': self._parse_numeric(row_data.get('Pass Types_Live', row_data.get('Live'))),
                                'dead_passes': self._parse_numeric(row_data.get('Pass Types_Dead', row_data.get('Dead'))),
                                'free_kicks': self._parse_numeric(row_data.get('Pass Types_FK', row_data.get('FK'))),
                                'through_balls': self._parse_numeric(row_data.get('Pass Types_TB', row_data.get('TB'))),
                                'switches': self._parse_numeric(row_data.get('Pass Types_Sw', row_data.get('Sw'))),
                                'crosses': self._parse_numeric(row_data.get('Pass Types_Crs', row_data.get('Crs'))),
                                'throw_ins': self._parse_numeric(row_data.get('Pass Types_TI', row_data.get('TI'))),
                                'corner_kicks': self._parse_numeric(row_data.get('Pass Types_CK', row_data.get('CK'))),
                                'corner_kicks_in': self._parse_numeric(row_data.get('Corner Kicks_In', row_data.get('In'))),
                                'corner_kicks_out': self._parse_numeric(row_data.get('Corner Kicks_Out', row_data.get('Out'))),
                                'corner_kicks_straight': self._parse_numeric(row_data.get('Corner Kicks_Str', row_data.get('Str'))),
                                'completed': self._parse_numeric(row_data.get('Outcomes_Cmp', row_data.get('Cmp'))),
                                'offsides': self._parse_numeric(row_data.get('Outcomes_Off', row_data.get('Off'))),
                                'blocked': self._parse_numeric(row_data.get('Outcomes_Blocks', row_data.get('Blocks')))
                            }
                            
                            pass_types_data.append(pass_types_stats)
                            
                except Exception as e:
                    logger.error(f"Error processing pass_types table {table_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting pass types data: {e}")
        
        return pass_types_data
    
    def bulk_insert_data(self, table_name: str, data: List[Dict], column_mapping: Dict[str, str]):
        """Bulk insert data using execute_batch for performance"""
        if not data:
            return 0
        
        inserted = 0
        try:
            # Prepare insert query with correct primary key column names
            columns = list(column_mapping.keys())
            placeholders = ', '.join(['%s'] * len(columns))
            
            # Map table names to their primary key columns
            pk_columns = {
                'match_player_passing': 'match_player_passing_id',
                'match_player_possession': 'match_player_possession_id',
                'match_player_defensive_actions': 'match_player_defensive_actions_id',
                'match_player_misc': 'match_player_misc_id',
                'match_player_pass_types': 'match_player_pass_types_id'
            }
            
            pk_column = pk_columns.get(table_name, f"{table_name}_id")
            
            query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT ({pk_column}) DO NOTHING
            """
            
            # Prepare data tuples
            values = []
            for record in data:
                value_tuple = tuple(record.get(col) for col in columns)
                values.append(value_tuple)
            
            # Execute batch insert
            execute_batch(self.cursor, query, values, page_size=BATCH_SIZE)
            self.conn.commit()
            
            inserted = len(values)
            logger.info(f"Bulk inserted {inserted} records into {table_name}")
            
        except Exception as e:
            logger.error(f"Error bulk inserting into {table_name}: {e}")
            self.conn.rollback()
        
        return inserted
    
    def process_file(self, filepath: str, filename: str) -> Dict[str, int]:
        """Process a single HTML file and extract all performance data"""
        results = {'passing': 0, 'possession': 0, 'defense': 0, 'misc': 0, 'pass_types': 0}
        
        match_id = self.extract_match_id_from_filename(filename)
        if not match_id:
            return results
        
        # Check if match exists in database
        if match_id not in self.cache['matches']:
            return results
        
        # Parse HTML
        soup = self.parse_html_file(filepath)
        if not soup:
            return results
        
        # Extract all data types
        extraction_methods = {
            'passing': (self.extract_passing_data, 'match_player_passing'),
            'possession': (self.extract_possession_data, 'match_player_possession'),
            'defense': (self.extract_defensive_data, 'match_player_defensive_actions'),
            'misc': (self.extract_misc_data, 'match_player_misc'),
            'pass_types': (self.extract_pass_types_data, 'match_player_pass_types')
        }
        
        for data_type, (extract_func, table_name) in extraction_methods.items():
            try:
                # Extract data
                data = extract_func(soup, match_id)
                if data:
                    self.stats[data_type]['extracted'] += len(data)
                    
                    # Add to batch
                    self.batch_data[data_type].extend(data)
                    
                    # If batch is full, insert
                    if len(self.batch_data[data_type]) >= BATCH_SIZE:
                        # Get column mapping from first record
                        if self.batch_data[data_type]:
                            column_mapping = {k: k for k in self.batch_data[data_type][0].keys()}
                            inserted = self.bulk_insert_data(table_name, self.batch_data[data_type], column_mapping)
                            self.stats[data_type]['inserted'] += inserted
                            results[data_type] = inserted
                            self.batch_data[data_type] = []
                
            except Exception as e:
                logger.error(f"Error processing {data_type} data for {filename}: {e}")
                self.stats[data_type]['errors'] += 1
        
        return results
    
    def flush_batches(self):
        """Flush any remaining batched data"""
        table_mapping = {
            'passing': 'match_player_passing',
            'possession': 'match_player_possession',
            'defense': 'match_player_defensive_actions',
            'misc': 'match_player_misc',
            'pass_types': 'match_player_pass_types'
        }
        
        for data_type, table_name in table_mapping.items():
            if self.batch_data[data_type]:
                logger.info(f"Flushing {len(self.batch_data[data_type])} remaining {data_type} records")
                column_mapping = {k: k for k in self.batch_data[data_type][0].keys()}
                inserted = self.bulk_insert_data(table_name, self.batch_data[data_type], column_mapping)
                self.stats[data_type]['inserted'] += inserted
                self.batch_data[data_type] = []
    
    def process_all_files(self, year_range: Tuple[int, int] = (2019, 2025)):
        """Process all HTML files for specified year range"""
        self.start_time = time.time()
        
        # Get all HTML files
        html_files = [f for f in os.listdir(HTML_DIR) if f.endswith('.html')]
        total_files = len(html_files)
        
        logger.info(f"Found {total_files} HTML files to process")
        logger.info(f"Filtering for years {year_range[0]}-{year_range[1]}")
        
        # Filter files by year
        files_to_process = []
        for filename in html_files:
            match_id = self.extract_match_id_from_filename(filename)
            if match_id and match_id in self.cache['matches']:
                # Check year
                self.cursor.execute("""
                    SELECT EXTRACT(YEAR FROM match_date) as year 
                    FROM match 
                    WHERE match_id = %s
                """, (match_id,))
                result = self.cursor.fetchone()
                
                if result and result['year']:
                    year = int(result['year'])
                    if year_range[0] <= year <= year_range[1]:
                        files_to_process.append(filename)
        
        logger.info(f"Processing {len(files_to_process)} files from {year_range[0]}-{year_range[1]}")
        
        # Process files with progress tracking
        for i, filename in enumerate(files_to_process, 1):
            if i % 50 == 0:
                elapsed = time.time() - self.start_time
                rate = i / elapsed
                remaining = (len(files_to_process) - i) / rate
                logger.info(f"Progress: {i}/{len(files_to_process)} files "
                          f"({i*100/len(files_to_process):.1f}%) - "
                          f"Est. remaining: {remaining/60:.1f} minutes")
            
            filepath = os.path.join(HTML_DIR, filename)
            results = self.process_file(filepath, filename)
            
            # Update processed counts
            for data_type in self.stats.keys():
                if data_type in ['passing', 'possession', 'defense', 'misc', 'pass_types']:
                    self.stats[data_type]['processed'] += 1
        
        # Flush any remaining batched data
        self.flush_batches()
        
        # Run ANALYZE on tables for query optimization
        logger.info("Running ANALYZE on performance tables...")
        for table in TABLE_MAPPING.values():
            self.cursor.execute(f"ANALYZE {table}")
        self.conn.commit()
    
    def generate_report(self):
        """Generate comprehensive extraction report"""
        elapsed_time = time.time() - self.start_time if self.start_time else 0
        
        report = []
        report.append("\n" + "="*80)
        report.append("TIER 2 DATA EXTRACTION REPORT - PERFORMANCE STATISTICS")
        report.append("="*80)
        report.append(f"Extraction completed at: {datetime.now()}")
        report.append(f"Total execution time: {elapsed_time/60:.2f} minutes")
        report.append("")
        
        # Summary statistics
        total_extracted = sum(s['extracted'] for s in self.stats.values())
        total_inserted = sum(s['inserted'] for s in self.stats.values())
        total_errors = sum(s['errors'] for s in self.stats.values())
        
        report.append("OVERALL SUMMARY:")
        report.append(f"  Total records extracted: {total_extracted:,}")
        report.append(f"  Total records inserted: {total_inserted:,}")
        report.append(f"  Total errors: {total_errors:,}")
        report.append(f"  Success rate: {(total_inserted/total_extracted*100 if total_extracted > 0 else 0):.2f}%")
        report.append("")
        
        # Detailed statistics per table
        table_names = {
            'passing': 'match_player_passing',
            'possession': 'match_player_possession',
            'defense': 'match_player_defensive_actions',
            'misc': 'match_player_misc',
            'pass_types': 'match_player_pass_types'
        }
        
        for data_type, table_name in table_names.items():
            stats = self.stats.get(data_type, {})
            report.append(f"{data_type.upper()} DATA ({table_name}):")
            report.append(f"  Files processed: {stats.get('processed', 0):,}")
            report.append(f"  Records extracted: {stats.get('extracted', 0):,}")
            report.append(f"  Records inserted: {stats.get('inserted', 0):,}")
            report.append(f"  Errors: {stats.get('errors', 0):,}")
            
            # Check final database count
            self.cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            final_count = self.cursor.fetchone()['count']
            report.append(f"  Final database count: {final_count:,}")
            report.append("")
        
        # Data quality checks
        report.append("DATA QUALITY VALIDATION:")
        
        # Check for orphaned records
        for data_type, table_name in table_names.items():
            self.cursor.execute(f"""
                SELECT COUNT(*) as count 
                FROM {table_name} t
                LEFT JOIN match_player mp ON t.match_player_id = mp.match_player_id
                WHERE mp.match_player_id IS NULL
            """)
            orphaned = self.cursor.fetchone()['count']
            if orphaned > 0:
                report.append(f"  WARNING: {orphaned} orphaned records in {table_name}")
        
        # Check data coverage by year
        report.append("")
        report.append("DATA COVERAGE BY YEAR:")
        self.cursor.execute("""
            SELECT 
                EXTRACT(YEAR FROM m.match_date) as year,
                COUNT(DISTINCT m.match_id) as matches,
                COUNT(DISTINCT mp.match_player_id) as match_players,
                COUNT(DISTINCT mpp.match_player_passing_id) as passing_records
            FROM match m
            LEFT JOIN match_player mp ON m.match_id = mp.match_id
            LEFT JOIN match_player_passing mpp ON mp.match_player_id = mpp.match_player_id
            WHERE EXTRACT(YEAR FROM m.match_date) BETWEEN 2019 AND 2025
            GROUP BY year
            ORDER BY year
        """)
        
        for row in self.cursor.fetchall():
            report.append(f"  {int(row['year'])}: {row['matches']} matches, "
                        f"{row['match_players']} players, {row['passing_records']} passing records")
        
        report.append("")
        report.append("PERFORMANCE METRICS:")
        if elapsed_time > 0:
            report.append(f"  Processing rate: {total_inserted/(elapsed_time/60):.0f} records/minute")
            report.append(f"  Average time per file: {elapsed_time/self.stats['passing']['processed']:.2f} seconds")
        
        report.append("="*80)
        
        return "\n".join(report)

def main():
    """Main execution function"""
    extractor = Tier2DataExtractor()
    
    try:
        # Connect to database
        if not extractor.connect_db():
            logger.error("Failed to connect to database")
            return
        
        # Load cache
        logger.info("Loading database mappings...")
        extractor.load_cache()
        
        # Process files for 2019-2025
        logger.info("Starting Tier 2 data extraction for 2019-2025...")
        extractor.process_all_files(year_range=(2019, 2025))
        
        # Generate and print report
        report = extractor.generate_report()
        print(report)
        
        # Save report to file
        with open('tier2_extraction_report.txt', 'w') as f:
            f.write(report)
        
        logger.info("Tier 2 extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error in extraction: {e}")
        traceback.print_exc()
    finally:
        extractor.close_db()

if __name__ == "__main__":
    main()