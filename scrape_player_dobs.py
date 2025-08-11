#!/usr/bin/env python3
"""
Scrape Player DOBs from FBref
Following the guide in parse_fbref_instructions.md EXACTLY to avoid being blocked
"""

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import logging
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('player_dob_scraping.log'),
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

# CRITICAL: Following guide's header recommendations to avoid being blocked
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

class PlayerDOBScraper:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.driver = None
        self.scraped_count = 0
        self.found_dobs = 0
        self.errors = 0
        
    def connect_db(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def setup_selenium(self):
        """Setup Selenium driver following guide recommendations"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Following guide's headless approach
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            
            # Use system Chrome - no need to specify chromedriver path if it's in PATH
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("Selenium driver setup complete")
            return True
        except Exception as e:
            logger.error(f"Selenium setup failed: {e}")
            return False
    
    def get_players_missing_dob(self, limit=200):
        """Get players missing DOB data"""
        try:
            # Get players missing DOB data, ordered alphabetically for consistent processing
            self.cursor.execute("""
                SELECT player_id, player_name 
                FROM player 
                WHERE dob IS NULL 
                ORDER BY player_name 
                LIMIT %s
            """, (limit,))
            
            players = self.cursor.fetchall()
            logger.info(f"Found {len(players)} players missing DOB data")
            return players
        except Exception as e:
            logger.error(f"Error fetching players: {e}")
            return []
    
    def scrape_player_dob(self, player_id, player_name):
        """
        Scrape DOB for a single player
        Following guide: Try BeautifulSoup first, Selenium as fallback
        """
        logger.info(f"Scraping DOB for {player_name} (ID: {player_id})")
        
        # Construct FBref player URL - need full format with player name
        player_url = f"https://fbref.com/en/players/{player_id}/{player_name.replace(' ', '-')}"
        
        # STEP 1: Try BeautifulSoup first (following guide methodology)
        try:
            logger.info(f"Trying BeautifulSoup for {player_name}")
            response = requests.get(player_url, headers=headers)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                dob = self.extract_dob_from_soup(soup)
                
                if dob:
                    logger.info(f"Found DOB with BeautifulSoup: {dob}")
                    return dob
                else:
                    logger.info(f"DOB not found in static HTML, trying Selenium...")
            else:
                logger.warning(f"HTTP {response.status_code} for {player_name}")
                
        except Exception as e:
            logger.error(f"BeautifulSoup failed for {player_name}: {e}")
        
        # STEP 2: Use Selenium as fallback (following guide methodology)
        if self.driver:
            try:
                logger.info(f"Using Selenium for {player_name}")
                self.driver.get(player_url)
                
                # Wait for page to load (following guide's wait strategy)
                wait = WebDriverWait(self.driver, 10)
                # Wait for any content to load
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                # Get page source and parse with BeautifulSoup
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                dob = self.extract_dob_from_soup(soup)
                
                if dob:
                    logger.info(f"Found DOB with Selenium: {dob}")
                    return dob
                    
            except Exception as e:
                logger.error(f"Selenium failed for {player_name}: {e}")
        
        logger.warning(f"Could not find DOB for {player_name}")
        return None
    
    def extract_dob_from_soup(self, soup):
        """Extract DOB and other player info from BeautifulSoup object"""
        try:
            player_info = {}
            
            # Method 1: Look for span with data-birth attribute (most reliable)
            birth_span = soup.find('span', {'data-birth': True})
            if birth_span:
                # Get ISO format date from data-birth attribute
                iso_date = birth_span.get('data-birth')
                if iso_date:
                    player_info['dob'] = iso_date
                    logger.info(f"Found DOB in data-birth attribute: {iso_date}")
            
            # Method 2: Look for span with id="necro-birth" as backup
            if 'dob' not in player_info:
                necro_birth = soup.find('span', {'id': 'necro-birth'})
                if necro_birth:
                    birth_text = necro_birth.get_text().strip()
                    if birth_text:
                        player_info['dob_text'] = birth_text
                        logger.info(f"Found DOB text: {birth_text}")
            
            # Extract position
            position_elem = soup.find('strong', string='Position:')
            if position_elem and position_elem.parent:
                position_text = position_elem.parent.get_text()
                position = position_text.replace('Position:', '').strip()
                if position:
                    player_info['position'] = position
                    logger.info(f"Found position: {position}")
            
            # Extract nationality from National Team link
            nat_team_elem = soup.find('strong', string='National Team:')
            if nat_team_elem and nat_team_elem.parent:
                nat_link = nat_team_elem.parent.find('a')
                if nat_link:
                    nationality = nat_link.get_text().strip()
                    if nationality:
                        player_info['nationality'] = nationality
                        logger.info(f"Found nationality: {nationality}")
            
            # Extract current club
            club_elem = soup.find('strong', string='Club:')
            if club_elem and club_elem.parent:
                club_link = club_elem.parent.find('a')
                if club_link:
                    club = club_link.get_text().strip()
                    if club:
                        player_info['current_club'] = club
                        logger.info(f"Found club: {club}")
            
            # Return the ISO date if we found it, otherwise return the collected info
            if 'dob' in player_info:
                return player_info['dob']
            elif 'dob_text' in player_info:
                return player_info['dob_text']
            
        except Exception as e:
            logger.error(f"Error extracting player info: {e}")
        
        return None
    
    def update_player_dob(self, player_id, dob):
        """Update player DOB in database"""
        try:
            # Parse the DOB into a date object
            parsed_date = None
            
            if dob:
                # If it's already in ISO format (YYYY-MM-DD), use it directly
                if len(dob) == 10 and dob.count('-') == 2:
                    try:
                        # Validate it's a proper date
                        datetime.strptime(dob, '%Y-%m-%d')
                        parsed_date = dob
                    except ValueError:
                        logger.warning(f"Invalid ISO date format: {dob}")
                else:
                    # Try to parse text formats like "April 28, 1995"
                    try:
                        # Try common date formats
                        formats = ['%B %d, %Y', '%b %d, %Y', '%m/%d/%Y', '%d/%m/%Y']
                        for fmt in formats:
                            try:
                                parsed_dt = datetime.strptime(dob, fmt)
                                parsed_date = parsed_dt.strftime('%Y-%m-%d')
                                break
                            except ValueError:
                                continue
                    except Exception as e:
                        logger.warning(f"Could not parse date '{dob}': {e}")
            
            if parsed_date:
                # Update the database
                self.cursor.execute(
                    "UPDATE player SET dob = %s WHERE player_id = %s", 
                    (parsed_date, player_id)
                )
                self.conn.commit()
                logger.info(f"✅ Updated player {player_id} with DOB: {parsed_date}")
                return True
            else:
                logger.warning(f"Could not parse DOB '{dob}' for player {player_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating DOB for player {player_id}: {e}")
            return False
    
    def run_scraping(self, limit=200):
        """
        Main scraping function
        CRITICAL: Following guide's rate limiting (6 second delays)
        """
        logger.info(f"Starting DOB scraping for up to {limit} players")
        
        players = self.get_players_missing_dob(limit)
        if not players:
            logger.error("No players found to scrape")
            return
        
        for i, player in enumerate(players):
            try:
                player_id = player['player_id']
                player_name = player['player_name']
                
                logger.info(f"Processing {i+1}/{len(players)}: {player_name}")
                
                dob = self.scrape_player_dob(player_id, player_name)
                
                if dob:
                    if self.update_player_dob(player_id, dob):
                        self.found_dobs += 1
                
                self.scraped_count += 1
                
                # Log progress every 10 players
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i+1}/{len(players)} processed, {self.found_dobs} DOBs found so far")
                
                # CRITICAL: Following guide's 6-second delay to avoid being blocked
                if i < len(players) - 1:  # Don't delay after the last player
                    logger.info("Waiting 6 seconds to be respectful to FBref...")
                    time.sleep(6)
                    
            except Exception as e:
                logger.error(f"Error processing {player.get('player_name', 'Unknown')}: {e}")
                self.errors += 1
        
        # Summary
        logger.info(f"Scraping complete:")
        logger.info(f"  Players processed: {self.scraped_count}")
        logger.info(f"  DOBs found: {self.found_dobs}")  
        logger.info(f"  Errors: {self.errors}")
    
    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
            logger.info("Selenium driver closed")
        
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """
    Main function - TEST WITH SMALL SAMPLE FIRST
    """
    scraper = PlayerDOBScraper()
    
    try:
        if not scraper.connect_db():
            return
        
        if not scraper.setup_selenium():
            logger.warning("Selenium setup failed, will only use BeautifulSoup")
        
        # Process larger batch - 200 players takes ~20 minutes with 6-second delays
        scraper.run_scraping(limit=200)  # Process 200 players (~20 minutes with delays)
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        traceback.print_exc()
    finally:
        scraper.cleanup()

if __name__ == "__main__":
    main()