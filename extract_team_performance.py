#!/usr/bin/env python3
"""
Extract team-level performance statistics from FBref HTML files
to fill gaps in match_team_performance table.
"""

import os
import re
import json
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import traceback
import logging

# Set up logging
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

class TeamPerformanceExtractor:
    """Extract team performance statistics from FBref HTML files."""
    
    def __init__(self):
        """Initialize the extractor."""
        self.conn = None
        self.missing_matches = []
        self.team_season_map = {}
        self.extraction_stats = {
            'total_missing': 0,
            'files_found': 0,
            'successfully_extracted': 0,
            'failed_extractions': 0,
            'records_inserted': 0
        }
        
    def connect_db(self):
        """Connect to the database."""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
            
    def get_missing_matches(self):
        """Get list of matches missing team performance data."""
        query = """
        SELECT m.match_id, m.match_date, 
               m.home_team_season_id, m.away_team_season_id,
               m.season_uuid, m.match_type_id, m.match_subtype_id,
               m.home_goals, m.away_goals,
               hts.team_name_season_1 as home_team_name,
               ats.team_name_season_1 as away_team_name,
               s.season_id, s.season_year
        FROM match m
        LEFT JOIN team_season hts ON m.home_team_season_id = hts.id
        LEFT JOIN team_season ats ON m.away_team_season_id = ats.id
        LEFT JOIN season s ON m.season_uuid = s.id
        WHERE NOT EXISTS (
            SELECT 1 FROM match_team_performance mtp 
            WHERE mtp.match_id = m.match_id
        )
        ORDER BY m.match_date
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            self.missing_matches = cur.fetchall()
            
        self.extraction_stats['total_missing'] = len(self.missing_matches)
        logger.info(f"Found {len(self.missing_matches)} matches missing team performance data")
        
        # Log date ranges
        if self.missing_matches:
            dates = [m['match_date'] for m in self.missing_matches]
            logger.info(f"Date range: {min(dates)} to {max(dates)}")
            
            # Count by season
            season_counts = {}
            for m in self.missing_matches:
                season = m.get('season_year', m.get('season_id'))
                season_counts[season] = season_counts.get(season, 0) + 1
            logger.info(f"Matches by season: {season_counts}")
            
    def load_team_season_mappings(self):
        """Load team_season mappings for UUID lookups."""
        query = """
        SELECT ts.id, ts.team_id, ts.season_id, ts.team_name_season_1, ts.team_name_season_2
        FROM team_season ts
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            
        for row in results:
            # Create multiple lookup keys for flexibility
            season_id = row['season_id']
            team_id = row['team_id']
            
            # Store by fbref team ID and season
            key = f"{team_id}_{season_id}"
            self.team_season_map[key] = row['id']
            
            # Also store by team names for backup matching
            for name_field in ['team_name_season_1', 'team_name_season_2']:
                if row[name_field]:
                    name_key = f"{row[name_field].lower()}_{season_id}"
                    self.team_season_map[name_key] = row['id']
                    
        logger.info(f"Loaded {len(self.team_season_map)} team_season mappings")
        
    def find_html_file(self, match_id: str, match_date) -> Optional[str]:
        """Find HTML file for a given match."""
        # Try multiple filename patterns
        patterns = [
            f"{match_id}.html",
            f"match_{match_id}.html",
            match_id.replace('-', '_') + ".html"
        ]
        
        for pattern in patterns:
            filepath = os.path.join(HTML_DIR, pattern)
            if os.path.exists(filepath):
                return filepath
                
        # If not found by ID, try date-based search
        if match_date:
            date_str = match_date.strftime('%Y-%m-%d')
            for filename in os.listdir(HTML_DIR):
                if date_str in filename and filename.endswith('.html'):
                    filepath = os.path.join(HTML_DIR, filename)
                    # Verify it's the right match by checking content
                    if self.verify_match_file(filepath, match_id):
                        return filepath
                        
        return None
        
    def verify_match_file(self, filepath: str, match_id: str) -> bool:
        """Verify that HTML file corresponds to the expected match."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                # Check if match ID appears in the file
                return match_id in content
        except:
            return False
            
    def extract_team_stats_from_html(self, filepath: str, match_data: dict) -> List[Dict]:
        """Extract team statistics from HTML file."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
                
            team_stats = []
            
            # Find team stats tables
            # Common patterns: team_stats, team_stats_extra, stats_{team_id}_summary
            tables = soup.find_all('table')
            
            # Look for main team stats table
            team_stats_table = None
            for table in tables:
                table_id = table.get('id', '')
                if 'team_stats' in table_id:
                    team_stats_table = table
                    break
                    
            if team_stats_table:
                # Extract from team_stats table
                stats = self.parse_team_stats_table(team_stats_table, match_data)
                if stats:
                    team_stats.extend(stats)
            else:
                # Try to extract from individual team summary tables
                home_stats = self.extract_team_summary_stats(soup, match_data, is_home=True)
                away_stats = self.extract_team_summary_stats(soup, match_data, is_home=False)
                
                if home_stats:
                    team_stats.append(home_stats)
                if away_stats:
                    team_stats.append(away_stats)
                    
            # If still no stats, try extracting from matchlog tables
            if not team_stats:
                team_stats = self.extract_from_matchlogs(soup, match_data)
                
            return team_stats
            
        except Exception as e:
            logger.error(f"Error extracting from {filepath}: {e}")
            logger.error(traceback.format_exc())
            return []
            
    def parse_team_stats_table(self, table, match_data: dict) -> List[Dict]:
        """Parse the main team_stats table."""
        try:
            df = pd.read_html(str(table))[0]
            
            # Handle multi-level columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(col).strip() for col in df.columns.values]
                
            stats = []
            
            # Usually has two rows - one for each team
            for idx, row in df.iterrows():
                if idx >= 2:  # Only process first two rows
                    break
                    
                is_home = idx == 0
                team_season_id = match_data['home_team_season_id'] if is_home else match_data['away_team_season_id']
                opponent_id = match_data['away_team_season_id'] if is_home else match_data['home_team_season_id']
                
                # Extract statistics
                stat_dict = {
                    'match_id': match_data['match_id'],
                    'team_season_id': team_season_id,
                    'opponent_team_season_id': opponent_id,
                    'is_home': is_home,
                    'match_date': match_data['match_date'],
                    'season_id': match_data['season_id'],
                    'match_type_name': match_data['match_type_name'],
                    'match_subtype_name': match_data['match_subtype_name']
                }
                
                # Map column names to database fields
                column_mappings = {
                    'Possession': 'possession_pct',
                    'Poss': 'possession_pct',
                    'Passing Accuracy': 'passing_acc_pct',
                    'Pass%': 'passing_acc_pct',
                    'Shots on Target': 'sot_pct',
                    'SoT%': 'sot_pct',
                    'Saves': 'saves_pct',
                    'Save%': 'saves_pct',
                    'Goals': 'goals',
                    'Gls': 'goals',
                    'Shots': 'shots',
                    'Sh': 'shots',
                    'Touches': 'touches',
                    'Tackles': 'tackles',
                    'Tkl': 'tackles',
                    'Interceptions': 'interceptions',
                    'Int': 'interceptions',
                    'Clearances': 'clearances',
                    'Clr': 'clearances',
                    'Fouls': 'fouls',
                    'Fls': 'fouls',
                    'Corners': 'corners',
                    'CK': 'corners',
                    'Crosses': 'crosses',
                    'Crs': 'crosses',
                    'Offsides': 'offsides',
                    'Off': 'offsides',
                    'Aerials Won': 'aerials_won',
                    'Won': 'aerials_won',
                    'Yellow Cards': 'yellow_cards',
                    'CrdY': 'yellow_cards',
                    'Red Cards': 'red_cards',
                    'CrdR': 'red_cards',
                    'xG': 'xg',
                    'Expected Goals': 'xg'
                }
                
                for col in row.index:
                    col_str = str(col)
                    for pattern, field in column_mappings.items():
                        if pattern.lower() in col_str.lower():
                            value = row[col]
                            if pd.notna(value):
                                stat_dict[field] = self.parse_stat_value(value, field)
                            break
                            
                # Calculate derived fields
                if 'goals' in stat_dict:
                    # Determine result
                    opp_goals = None
                    if is_home and 'goals' in stats and len(stats) > 0:
                        opp_goals = stats[0].get('goals')
                    elif not is_home:
                        for s in stats:
                            if s.get('is_home'):
                                opp_goals = s.get('goals')
                                break
                                
                    if opp_goals is not None:
                        stat_dict['goals_against'] = opp_goals
                        if stat_dict['goals'] > opp_goals:
                            stat_dict['result'] = 'W'
                        elif stat_dict['goals'] < opp_goals:
                            stat_dict['result'] = 'L'
                        else:
                            stat_dict['result'] = 'D'
                            
                # Generate unique fbref_match_team_id
                stat_dict['fbref_match_team_id'] = f"{match_data['match_id']}_{team_season_id}"
                
                stats.append(stat_dict)
                
            return stats
            
        except Exception as e:
            logger.error(f"Error parsing team stats table: {e}")
            return []
            
    def extract_team_summary_stats(self, soup, match_data: dict, is_home: bool) -> Optional[Dict]:
        """Extract stats from individual team summary tables."""
        try:
            # Look for stats_{team_id}_summary tables
            team_name = match_data['home_team_name'] if is_home else match_data['away_team_name']
            team_season_id = match_data['home_team_season_id'] if is_home else match_data['away_team_season_id']
            opponent_id = match_data['away_team_season_id'] if is_home else match_data['home_team_season_id']
            
            # Find team-specific tables
            for table in soup.find_all('table'):
                table_id = table.get('id', '')
                
                # Check if this is a team summary table
                if 'stats_' in table_id and '_summary' in table_id:
                    # Extract team hex ID from table ID
                    team_hex = table_id.replace('stats_', '').replace('_summary', '')
                    
                    # Try to match this with our team
                    # This is simplified - you may need more sophisticated matching
                    df = pd.read_html(str(table))[0]
                    
                    if df.empty:
                        continue
                        
                    # Aggregate stats from player rows
                    stat_dict = {
                        'match_id': match_data['match_id'],
                        'team_season_id': team_season_id,
                        'opponent_team_season_id': opponent_id,
                        'is_home': is_home,
                        'match_date': match_data['match_date'],
                        'season_id': match_data['season_id'],
                        'match_type_name': match_data['match_type_name'],
                        'match_subtype_name': match_data['match_subtype_name'],
                        'fbref_match_team_id': f"{match_data['match_id']}_{team_season_id}"
                    }
                    
                    # Aggregate team totals from player stats
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    for col in numeric_cols:
                        col_lower = str(col).lower()
                        if 'gls' in col_lower or 'goals' in col_lower:
                            stat_dict['goals'] = int(df[col].sum())
                        elif 'ast' in col_lower or 'assists' in col_lower:
                            stat_dict['assists'] = int(df[col].sum())
                        elif 'sh' in col_lower and 'shot' in col_lower:
                            stat_dict['shots'] = int(df[col].sum())
                        elif 'tkl' in col_lower or 'tackle' in col_lower:
                            stat_dict['tackles'] = int(df[col].sum())
                        elif 'int' in col_lower and 'intercept' in col_lower:
                            stat_dict['interceptions'] = int(df[col].sum())
                        elif 'touches' in col_lower:
                            stat_dict['touches'] = int(df[col].sum())
                            
                    if len(stat_dict) > 10:  # If we found meaningful stats
                        return stat_dict
                        
            return None
            
        except Exception as e:
            logger.error(f"Error extracting team summary stats: {e}")
            return None
            
    def extract_from_matchlogs(self, soup, match_data: dict) -> List[Dict]:
        """Extract basic stats from matchlog tables as fallback."""
        stats = []
        
        try:
            # Look for matchlogs_for tables which often have basic stats
            for table in soup.find_all('table'):
                table_id = table.get('id', '')
                if 'matchlogs' in table_id:
                    df = pd.read_html(str(table))[0]
                    
                    if df.empty:
                        continue
                        
                    # Extract basic info from matchlog
                    # This is a simplified extraction - enhance as needed
                    for idx, row in df.iterrows():
                        if idx >= 2:  # Process first two teams
                            break
                            
                        is_home = idx == 0
                        team_season_id = match_data['home_team_season_id'] if is_home else match_data['away_team_season_id']
                        opponent_id = match_data['away_team_season_id'] if is_home else match_data['home_team_season_id']
                        
                        stat_dict = {
                            'match_id': match_data['match_id'],
                            'team_season_id': team_season_id,
                            'opponent_team_season_id': opponent_id,
                            'is_home': is_home,
                            'match_date': match_data['match_date'],
                            'season_id': match_data['season_id'],
                            'match_type_name': match_data['match_type_name'],
                            'match_subtype_name': match_data['match_subtype_name'],
                            'fbref_match_team_id': f"{match_data['match_id']}_{team_season_id}"
                        }
                        
                        # Extract available stats
                        for col in row.index:
                            col_str = str(col).lower()
                            value = row[col]
                            
                            if pd.notna(value):
                                if 'gf' in col_str or 'goals for' in col_str:
                                    stat_dict['goals'] = int(value)
                                elif 'ga' in col_str or 'goals against' in col_str:
                                    stat_dict['goals_against'] = int(value)
                                elif 'result' in col_str:
                                    if 'W' in str(value):
                                        stat_dict['result'] = 'W'
                                    elif 'L' in str(value):
                                        stat_dict['result'] = 'L'
                                    elif 'D' in str(value):
                                        stat_dict['result'] = 'D'
                                        
                        if 'goals' in stat_dict:
                            stats.append(stat_dict)
                            
                    if stats:
                        break
                        
        except Exception as e:
            logger.error(f"Error extracting from matchlogs: {e}")
            
        return stats
        
    def parse_stat_value(self, value, field_name: str):
        """Parse statistical value based on field type."""
        try:
            if pd.isna(value):
                return None
                
            value_str = str(value).strip()
            
            # Remove percentage signs
            if '%' in value_str:
                value_str = value_str.replace('%', '')
                
            # Handle percentage fields
            if '_pct' in field_name or 'percentage' in field_name:
                return int(float(value_str))
                
            # Handle decimal fields (xG)
            if field_name == 'xg':
                return float(value_str)
                
            # Handle integer fields
            return int(float(value_str))
            
        except:
            return None
            
    def insert_team_performance(self, stats: List[Dict]) -> int:
        """Insert team performance records into database."""
        inserted = 0
        
        insert_query = """
        INSERT INTO match_team_performance (
            id, match_id, team_season_id, opponent_team_season_id, is_home,
            match_date, season_id, match_type_name, match_subtype_name,
            goals, goals_against, result, possession_pct, touches,
            passing_acc_pct, crosses, long_balls, sot_pct, xg,
            saves_pct, tackles, interceptions, clearances, aerials_won,
            fouls, corners, offsides, goal_kicks, throw_ins,
            fbref_match_team_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (fbref_match_team_id) DO UPDATE SET
            goals = EXCLUDED.goals,
            goals_against = EXCLUDED.goals_against,
            result = EXCLUDED.result,
            possession_pct = EXCLUDED.possession_pct,
            touches = EXCLUDED.touches,
            passing_acc_pct = EXCLUDED.passing_acc_pct,
            crosses = EXCLUDED.crosses,
            tackles = EXCLUDED.tackles,
            interceptions = EXCLUDED.interceptions,
            xg = EXCLUDED.xg,
            updated_at = CURRENT_TIMESTAMP
        """
        
        with self.conn.cursor() as cur:
            for stat in stats:
                try:
                    # Generate UUID for ID
                    record_id = str(uuid.uuid4())
                    
                    values = (
                        record_id,
                        stat.get('match_id'),
                        stat.get('team_season_id'),
                        stat.get('opponent_team_season_id'),
                        stat.get('is_home'),
                        stat.get('match_date'),
                        stat.get('season_id'),
                        stat.get('match_type_name'),
                        stat.get('match_subtype_name'),
                        stat.get('goals'),
                        stat.get('goals_against'),
                        stat.get('result'),
                        stat.get('possession_pct'),
                        stat.get('touches'),
                        stat.get('passing_acc_pct'),
                        stat.get('crosses'),
                        stat.get('long_balls'),
                        stat.get('sot_pct'),
                        stat.get('xg'),
                        stat.get('saves_pct'),
                        stat.get('tackles'),
                        stat.get('interceptions'),
                        stat.get('clearances'),
                        stat.get('aerials_won'),
                        stat.get('fouls'),
                        stat.get('corners'),
                        stat.get('offsides'),
                        stat.get('goal_kicks'),
                        stat.get('throw_ins'),
                        stat.get('fbref_match_team_id')
                    )
                    
                    cur.execute(insert_query, values)
                    inserted += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting record: {e}")
                    logger.error(f"Data: {stat}")
                    
            self.conn.commit()
            
        return inserted
        
    def run_extraction(self):
        """Run the full extraction process."""
        logger.info("Starting team performance extraction...")
        
        # Connect to database
        self.connect_db()
        
        # Get missing matches
        self.get_missing_matches()
        
        if not self.missing_matches:
            logger.info("No missing matches found!")
            return
            
        # Load team season mappings
        self.load_team_season_mappings()
        
        # Process each missing match
        for match in self.missing_matches:
            match_id = match['match_id']
            logger.info(f"Processing match {match_id} ({match['home_team_name']} vs {match['away_team_name']})")
            
            # Find HTML file
            html_file = self.find_html_file(match_id, match['match_date'])
            
            if not html_file:
                logger.warning(f"HTML file not found for match {match_id}")
                continue
                
            self.extraction_stats['files_found'] += 1
            
            # Extract team stats
            team_stats = self.extract_team_stats_from_html(html_file, match)
            
            if team_stats:
                # Insert into database
                inserted = self.insert_team_performance(team_stats)
                
                if inserted > 0:
                    self.extraction_stats['successfully_extracted'] += 1
                    self.extraction_stats['records_inserted'] += inserted
                    logger.info(f"Inserted {inserted} team performance records for match {match_id}")
                else:
                    self.extraction_stats['failed_extractions'] += 1
                    logger.warning(f"Failed to insert records for match {match_id}")
            else:
                self.extraction_stats['failed_extractions'] += 1
                logger.warning(f"No stats extracted for match {match_id}")
                
        # Final report
        self.print_final_report()
        
        # Close connection
        if self.conn:
            self.conn.close()
            
    def print_final_report(self):
        """Print final extraction report."""
        logger.info("\n" + "="*60)
        logger.info("TEAM PERFORMANCE EXTRACTION REPORT")
        logger.info("="*60)
        
        for key, value in self.extraction_stats.items():
            logger.info(f"{key}: {value}")
            
        # Check final coverage
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM match")
            total_matches = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(DISTINCT match_id) FROM match_team_performance")
            covered_matches = cur.fetchone()[0]
            
        coverage_pct = (covered_matches / total_matches) * 100
        logger.info(f"\nFinal Coverage: {covered_matches}/{total_matches} ({coverage_pct:.2f}%)")
        
        if coverage_pct >= 100:
            logger.info("SUCCESS: 100% coverage achieved!")
        else:
            remaining = total_matches - covered_matches
            logger.info(f"Remaining gaps: {remaining} matches")
            
            
if __name__ == "__main__":
    extractor = TeamPerformanceExtractor()
    extractor.run_extraction()