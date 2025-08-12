#!/usr/bin/env python3
"""
Extract complete FBref possession data from HTML files to populate all 26 columns.
This script extracts from stats_{team_id}_possession tables in FBref HTML files.
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

class FBrefPossessionExtractor:
    """Extract complete FBref possession statistics from HTML match files."""
    
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
            'records_inserted': 0,
            'columns_populated': {},
            'errors': [],
            'missing_players': [],
            'data_quality': {
                'complete_records': 0,
                'partial_records': 0,
                'empty_records': 0
            }
        }
        
        # Complete mapping of FBref data-stat attributes to database columns
        self.field_mappings = {
            # Touches (7 columns)
            'touches': 'touches',
            'touches_def_pen_area': 'touches_def_pen',
            'touches_def_3rd': 'touches_def_3rd',
            'touches_mid_3rd': 'touches_mid_3rd',
            'touches_att_3rd': 'touches_att_3rd',
            'touches_att_pen_area': 'touches_att_pen',
            'touches_live_ball': 'touches_live',
            
            # Take-Ons (5 columns)
            'take_ons': 'take_ons_att',
            'take_ons_won': 'take_ons_succ',
            'take_ons_won_pct': 'take_ons_succ_pct',
            'take_ons_tackled': 'take_ons_tkld',
            'take_ons_tackled_pct': 'take_ons_tkld_pct',
            
            # Carries (8 columns)
            'carries': 'carries',
            'carries_distance': 'carries_total_distance',
            'carries_progressive_distance': 'carries_progressive_distance',
            'progressive_carries': 'carries_progressive',
            'carries_into_final_third': 'carries_final_third',
            'carries_into_penalty_area': 'carries_penalty_area',
            'miscontrols': 'miscontrols',
            'dispossessed': 'dispossessed',
            
            # Receiving (2 columns)
            'passes_received': 'passes_received',
            'progressive_passes_received': 'passes_received_progressive'
        }
        
    def connect_db(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            print("✓ Database connection established")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            return False
    
    def get_match_info(self, filename: str) -> Optional[Dict]:
        """Extract match ID from filename and get match info from database."""
        match_id_pattern = r'match_([a-f0-9]{8})\.html'
        match = re.search(match_id_pattern, filename)
        
        if not match:
            return None
            
        fbref_match_id = match.group(1)
        
        # Get match info from database (match_id IS the FBref ID)
        query = """
            SELECT match_id, home_team_season_id, away_team_season_id, season_uuid, match_date
            FROM match
            WHERE match_id = %s
        """
        self.cursor.execute(query, (fbref_match_id,))
        result = self.cursor.fetchone()
        
        if result:
            return dict(result)
        return None
    
    def get_or_create_match_player(self, match_id: str, fbref_player_id: str, team_season_id: str, match_info: Dict) -> Optional[str]:
        """Get or create match_player record and return its UUID."""
        # First check if match_player record exists
        query = """
            SELECT id 
            FROM match_player 
            WHERE match_id = %s AND player_id = %s
        """
        self.cursor.execute(query, (match_id, fbref_player_id))
        result = self.cursor.fetchone()
        
        if result:
            return result['id']
        
        # If not exists, we need to create it with minimal data
        # We'll need to get season_id from the season_uuid
        season_query = """
            SELECT season_year 
            FROM season 
            WHERE id = %s
        """
        self.cursor.execute(season_query, (match_info.get('season_uuid'),))
        season_result = self.cursor.fetchone()
        season_id = season_result['season_year'] if season_result else 2024  # Default to 2024 if not found
        
        # Create match_player record
        insert_query = """
            INSERT INTO match_player (match_id, player_id, team_season_id, match_date, season_id, minutes_played, started)
            VALUES (%s, %s, %s, %s, %s, 0, false)
            ON CONFLICT (match_id, player_id) DO UPDATE SET match_id = EXCLUDED.match_id
            RETURNING id
        """
        self.cursor.execute(insert_query, (
            match_id,
            fbref_player_id,
            team_season_id,
            match_info.get('match_date'),
            season_id
        ))
        result = self.cursor.fetchone()
        self.conn.commit()
        
        return result['id'] if result else None
    
    def get_team_season_id(self, fbref_team_id: str, season_uuid: str) -> Optional[str]:
        """Get team_season_id from FBref team hex ID and season."""
        # The team_id in team_season table appears to be the FBref team ID
        # Get season_year from season_uuid first
        season_query = """
            SELECT season_year 
            FROM season 
            WHERE id = %s
        """
        self.cursor.execute(season_query, (season_uuid,))
        season_result = self.cursor.fetchone()
        
        if not season_result:
            return None
        
        # Now get the team_season_id using the FBref team ID directly
        ts_query = """
            SELECT id 
            FROM team_season 
            WHERE team_id = %s AND season_id = %s
        """
        self.cursor.execute(ts_query, (fbref_team_id, season_result['season_year']))
        ts_result = self.cursor.fetchone()
        
        return ts_result['id'] if ts_result else None
    
    def extract_possession_table(self, soup: BeautifulSoup, table_id: str) -> Optional[pd.DataFrame]:
        """Extract possession data from a specific table."""
        table = soup.find('table', {'id': table_id})
        
        if not table:
            return None
        
        try:
            # Extract data manually to handle complex structure
            data = []
            headers = []
            
            # Get headers from the second header row (with actual column names)
            header_rows = table.find('thead').find_all('tr')
            if len(header_rows) >= 2:
                # Use the second row which has the actual column names
                for th in header_rows[-1].find_all(['th']):
                    stat = th.get('data-stat', '')
                    if stat:
                        headers.append(stat)
            
            # Extract player FBref IDs and data from tbody
            tbody = table.find('tbody')
            if tbody:
                for row in tbody.find_all('tr'):
                    row_data = {}
                    
                    # Get player ID from data-append-csv
                    player_cell = row.find('th', {'data-stat': 'player'})
                    if player_cell:
                        row_data['fbref_player_id'] = player_cell.get('data-append-csv', '')
                        row_data['Player'] = player_cell.get_text(strip=True)
                    
                    # Get all other cells
                    for cell in row.find_all(['th', 'td']):
                        stat = cell.get('data-stat', '')
                        if stat and stat != 'player':
                            value = cell.get_text(strip=True)
                            # Clean up the value
                            if value in ['', '—', '-']:
                                value = None
                            row_data[stat] = value
                    
                    if row_data.get('fbref_player_id'):
                        data.append(row_data)
            
            if not data:
                return None
                
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Extract team ID from table ID
            team_id_match = re.search(r'stats_([a-f0-9]+)_possession', table_id)
            if team_id_match:
                df['fbref_team_id'] = team_id_match.group(1)
            
            return df
            
        except Exception as e:
            print(f"  ✗ Error parsing table {table_id}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def process_possession_data(self, df: pd.DataFrame, match_info: Dict) -> List[Dict]:
        """Process possession DataFrame and prepare records for database."""
        records = []
        
        for _, row in df.iterrows():
            # Skip rows without player IDs
            if not row.get('fbref_player_id'):
                continue
            
            # Determine which team this player belongs to
            fbref_team_id = row.get('fbref_team_id', '')
            team_season_id = self.get_team_season_id(fbref_team_id, match_info.get('season_uuid'))
            
            if not team_season_id:
                # Try to guess based on home/away team
                team_season_id = match_info.get('home_team_season_id')
            
            # Get or create match_player record
            match_player_id = self.get_or_create_match_player(
                match_info['match_id'],
                row['fbref_player_id'],
                team_season_id,
                match_info
            )
            
            if not match_player_id:
                self.stats['missing_players'].append({
                    'fbref_id': row['fbref_player_id'],
                    'name': row.get('Player', 'Unknown'),
                    'match_id': match_info['match_id']
                })
                continue
            
            # Build record with all possession fields
            record = {
                'match_player_id': match_player_id,
                'season_id': None  # We'll set this from the season table
            }
            
            # Get season_id (year) from season_uuid
            if match_info.get('season_uuid'):
                season_query = """
                    SELECT season_year 
                    FROM season 
                    WHERE id = %s
                """
                self.cursor.execute(season_query, (match_info['season_uuid'],))
                season_result = self.cursor.fetchone()
                if season_result:
                    record['season_id'] = season_result['season_year']
            
            # Map all possession fields
            fields_populated = 0
            for fbref_field, db_field in self.field_mappings.items():
                # Get value from row using the data-stat attribute name
                value = row.get(fbref_field)
                
                # Clean and convert value
                if pd.notna(value) and value != '':
                    try:
                        # Handle percentage values
                        if 'pct' in db_field and isinstance(value, str) and '%' in str(value):
                            value = float(str(value).replace('%', ''))
                        elif 'pct' in db_field:
                            value = float(value) if value else None
                        else:
                            # Convert to appropriate type
                            value = float(value) if '.' in str(value) else int(float(value))
                        
                        record[db_field] = value
                        fields_populated += 1
                        
                        # Track column population
                        if db_field not in self.stats['columns_populated']:
                            self.stats['columns_populated'][db_field] = 0
                        self.stats['columns_populated'][db_field] += 1
                    except (ValueError, TypeError):
                        record[db_field] = None
                else:
                    record[db_field] = None
            
            # Track data quality
            if fields_populated >= 20:
                self.stats['data_quality']['complete_records'] += 1
            elif fields_populated >= 10:
                self.stats['data_quality']['partial_records'] += 1
            else:
                self.stats['data_quality']['empty_records'] += 1
            
            records.append(record)
        
        return records
    
    def upsert_possession_data(self, records: List[Dict]) -> int:
        """Insert or update possession records in the database."""
        if not records:
            return 0
        
        updated_count = 0
        inserted_count = 0
        
        for record in records:
            # Check if record exists
            check_query = """
                SELECT 1 FROM match_player_possession
                WHERE match_player_id = %s
            """
            self.cursor.execute(check_query, (record['match_player_id'],))
            exists = self.cursor.fetchone()
            
            if exists:
                # Update existing record
                update_fields = []
                update_values = []
                
                for field, value in record.items():
                    if field != 'match_player_id' and value is not None:
                        update_fields.append(f"{field} = %s")
                        update_values.append(value)
                
                if update_fields:
                    update_query = f"""
                        UPDATE match_player_possession
                        SET {', '.join(update_fields)}
                        WHERE match_player_id = %s
                    """
                    update_values.append(record['match_player_id'])
                    self.cursor.execute(update_query, update_values)
                    updated_count += 1
            else:
                # Insert new record
                fields = list(record.keys())
                placeholders = ['%s'] * len(fields)
                insert_query = f"""
                    INSERT INTO match_player_possession ({', '.join(fields)})
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT (match_player_id) DO UPDATE SET
                    {', '.join([f'{f} = EXCLUDED.{f}' for f in fields if f != 'match_player_id'])}
                """
                self.cursor.execute(insert_query, list(record.values()))
                inserted_count += 1
        
        self.conn.commit()
        self.stats['records_updated'] += updated_count
        self.stats['records_inserted'] += inserted_count
        
        return updated_count + inserted_count
    
    def process_file(self, filepath: str) -> bool:
        """Process a single HTML file."""
        filename = os.path.basename(filepath)
        
        # Get match info
        match_info = self.get_match_info(filename)
        if not match_info:
            print(f"  ⚠ No match found in database for {filename}")
            return False
        
        print(f"\n Processing {filename}")
        print(f"  Match ID: {match_info['match_id'][:8]}...")
        
        try:
            # Read and parse HTML
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Find all possession tables
            possession_tables = soup.find_all('table', id=re.compile(r'stats_[a-f0-9]+_possession'))
            
            if not possession_tables:
                print(f"  ⚠ No possession tables found")
                return False
            
            print(f"  Found {len(possession_tables)} possession table(s)")
            self.stats['tables_found'] += len(possession_tables)
            
            all_records = []
            
            for table in possession_tables:
                table_id = table.get('id')
                print(f"  Processing table: {table_id}")
                
                # Extract data from table
                df = self.extract_possession_table(soup, table_id)
                
                if df is not None and not df.empty:
                    # Process the data
                    records = self.process_possession_data(df, match_info)
                    all_records.extend(records)
                    print(f"    Extracted {len(records)} player records")
                    self.stats['players_extracted'] += len(records)
            
            # Upsert all records
            if all_records:
                count = self.upsert_possession_data(all_records)
                print(f"  ✓ Updated/Inserted {count} possession records")
            
            return True
            
        except Exception as e:
            error_msg = f"Error processing {filename}: {str(e)}"
            print(f"  ✗ {error_msg}")
            self.stats['errors'].append(error_msg)
            return False
    
    def process_all_files(self, html_dir: str):
        """Process all HTML files in the directory."""
        html_files = [f for f in os.listdir(html_dir) if f.startswith('match_') and f.endswith('.html')]
        total_files = len(html_files)
        
        print(f"\nFound {total_files} match HTML files to process")
        print("=" * 60)
        
        for i, filename in enumerate(html_files, 1):
            filepath = os.path.join(html_dir, filename)
            print(f"\n[{i}/{total_files}] Processing {filename}")
            
            if self.process_file(filepath):
                self.stats['files_processed'] += 1
        
        print("\n" + "=" * 60)
        print("Processing complete!")
    
    def generate_report(self) -> str:
        """Generate extraction report."""
        report = []
        report.append("FBref Possession Data Extraction Report")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Summary statistics
        report.append("SUMMARY STATISTICS:")
        report.append(f"  Files processed: {self.stats['files_processed']}")
        report.append(f"  Tables found: {self.stats['tables_found']}")
        report.append(f"  Players extracted: {self.stats['players_extracted']}")
        report.append(f"  Records updated: {self.stats['records_updated']}")
        report.append(f"  Records inserted: {self.stats['records_inserted']}")
        report.append("")
        
        # Data quality
        report.append("DATA QUALITY:")
        report.append(f"  Complete records (20+ fields): {self.stats['data_quality']['complete_records']}")
        report.append(f"  Partial records (10-19 fields): {self.stats['data_quality']['partial_records']}")
        report.append(f"  Sparse records (<10 fields): {self.stats['data_quality']['empty_records']}")
        report.append("")
        
        # Column population
        report.append("COLUMN POPULATION (Top populated fields):")
        sorted_columns = sorted(self.stats['columns_populated'].items(), key=lambda x: x[1], reverse=True)
        for col, count in sorted_columns[:15]:
            report.append(f"  {col}: {count} records")
        
        if len(sorted_columns) > 15:
            report.append(f"  ... and {len(sorted_columns) - 15} more fields")
        report.append("")
        
        # Missing fields (fields never populated)
        all_db_fields = set(self.field_mappings.values())
        populated_fields = set(self.stats['columns_populated'].keys())
        missing_fields = all_db_fields - populated_fields
        
        if missing_fields:
            report.append("UNPOPULATED FIELDS:")
            for field in sorted(missing_fields):
                report.append(f"  - {field}")
            report.append("")
        
        # Missing players
        if self.stats['missing_players']:
            report.append(f"MISSING PLAYERS ({len(self.stats['missing_players'])} total):")
            # Show first 10
            for player in self.stats['missing_players'][:10]:
                report.append(f"  - {player['name']} (FBref: {player['fbref_id']})")
            if len(self.stats['missing_players']) > 10:
                report.append(f"  ... and {len(self.stats['missing_players']) - 10} more")
            report.append("")
        
        # Errors
        if self.stats['errors']:
            report.append(f"ERRORS ({len(self.stats['errors'])} total):")
            for error in self.stats['errors'][:10]:
                report.append(f"  - {error}")
            if len(self.stats['errors']) > 10:
                report.append(f"  ... and {len(self.stats['errors']) - 10} more")
        
        return "\n".join(report)
    
    def test_single_file(self, filepath: str):
        """Test extraction with a single file."""
        print(f"\nTesting extraction with: {os.path.basename(filepath)}")
        print("=" * 60)
        
        if self.process_file(filepath):
            print("\n✓ Test successful!")
        else:
            print("\n✗ Test failed!")
        
        # Generate mini-report
        print("\nTest Results:")
        print(f"  Tables found: {self.stats['tables_found']}")
        print(f"  Players extracted: {self.stats['players_extracted']}")
        print(f"  Records updated: {self.stats['records_updated']}")
        print(f"  Records inserted: {self.stats['records_inserted']}")
        
        if self.stats['columns_populated']:
            print("\nFields populated:")
            for field, count in sorted(self.stats['columns_populated'].items()):
                print(f"  - {field}: {count}")
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Database connection closed")


def main():
    """Main execution function."""
    # Initialize extractor
    extractor = FBrefPossessionExtractor(DB_CONFIG)
    
    if not extractor.connect_db():
        sys.exit(1)
    
    try:
        # HTML files directory
        html_dir = "/Users/thomasmcmillan/projects/nwsl_db_migration/html_files"
        
        # Test mode or full extraction
        if len(sys.argv) > 1 and sys.argv[1] == '--test':
            # Test with the known file
            test_file = os.path.join(html_dir, "match_07c68416.html")
            extractor.test_single_file(test_file)
        else:
            # Process all files
            extractor.process_all_files(html_dir)
            
            # Generate and save report
            report = extractor.generate_report()
            print("\n" + report)
            
            # Save report to file
            report_filename = f"possession_extraction_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(report_filename, 'w') as f:
                f.write(report)
            print(f"\n✓ Report saved to {report_filename}")
            
            # Also save stats as JSON
            stats_filename = f"possession_extraction_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(stats_filename, 'w') as f:
                json.dump(extractor.stats, f, indent=2, default=str)
            print(f"✓ Statistics saved to {stats_filename}")
    
    except KeyboardInterrupt:
        print("\n\n⚠ Extraction interrupted by user")
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        traceback.print_exc()
    finally:
        extractor.close()


if __name__ == "__main__":
    main()