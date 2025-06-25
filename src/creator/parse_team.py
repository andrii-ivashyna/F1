# parse_team.py

import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from io import StringIO
import config
from fetch_api import log, get_country_code

def parse_team_wiki():
    """Parses Wikipedia for F1 constructor data and updates the database."""
    log("Starting Wikipedia constructor parsing...")
    url = "https://en.wikipedia.org/wiki/List_of_Formula_One_constructors"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the first table with constructor data
        table = soup.find('table', {'class': 'wikitable'})
        if not table:
            log("ERROR: Could not find constructor table on Wikipedia page")
            return
            
        # Read the table with pandas
        constructors_df = pd.read_html(StringIO(str(table)))[0]
        
        # Check if 'Constructor' column exists, if not check other possible column names
        constructor_column = None
        for col in constructors_df.columns:
            if 'constructor' in str(col).lower():
                constructor_column = col
                break
        
        if constructor_column is None:
            log(f"ERROR: Could not find Constructor column. Available columns: {list(constructors_df.columns)}")
            return

        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()

        # Get current teams from database
        db_teams = cursor.execute("SELECT team_id, team_name FROM team").fetchall()

        for index, row in constructors_df.iterrows():
            constructor_name = str(row[constructor_column]).strip()
            licensed_in = str(row.get('Licensed in', '')).strip()
            
            # Remove brackets and content inside them
            constructor_name = re.sub(r'\[.*?\]', '', constructor_name).strip()
            licensed_in = re.sub(r'\[.*?\]', '', licensed_in).strip()
            
            # Skip if constructor_name is NaN or empty
            if constructor_name in ['nan', '', 'NaN']:
                continue
                
            # Handle multi-line constructor names (separated by / and newlines)
            constructor_variants = []
            # Split by both / and newlines, then clean up
            parts = re.split(r'[/\n]+', constructor_name)
            for part in parts:
                clean_part = part.strip()
                if clean_part and clean_part not in ['nan', '', 'NaN']:
                    constructor_variants.append(clean_part)
            
            # Get country code from "Licensed in" column
            country_code = None
            if licensed_in and licensed_in not in ['nan', '', 'NaN']:
                country_code = get_country_code(licensed_in)
                if country_code:
                    # Add country to database if it doesn't exist
                    cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", 
                                 (country_code, licensed_in))
            
            # Match with database teams
            for team_id, team_name in db_teams:
                if not team_name:
                    continue
                    
                # Get first word of team name from database
                db_first_word = team_name.split()[0].lower()
                
                # Check if any constructor variant starts with the same word
                for constructor_variant in constructor_variants:
                    constructor_first_word = constructor_variant.split()[0].lower()
                    
                    if db_first_word == constructor_first_word:
                        log(f"  Updating team '{team_name}' with constructor name '{constructor_variant}' and country '{licensed_in}'")
                        
                        # Update team with constructor name and country
                        cursor.execute("""
                            UPDATE team 
                            SET team_name = ?, country_fk = ?
                            WHERE team_id = ?
                        """, (constructor_variant, country_code, team_id))
                        break

        conn.commit()
        conn.close()
        log("Wikipedia constructor parsing finished.")

    except Exception as e:
        log(f"ERROR: Could not parse constructor data from Wikipedia. Reason: {e}")

def parse_team_f1():
    """Parses F1 official site for team data and updates the database."""
    log("Starting F1 official site team parsing...")
    
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()

        # Get current teams from database
        db_teams = cursor.execute("SELECT team_id, team_name FROM team").fetchall()
        
        for team_id, team_name in db_teams:
            if not team_name:
                continue
                
            # Convert team name to URL format (lowercase, spaces to dashes)
            team_url_name = team_name.lower().replace(' ', '-').replace('&', 'and')
            
            # Special cases for team URL names
            team_url_mappings = {
                'rb': 'rb-f1-team',
                'visa-cashapp-rb': 'rb-f1-team',
                'haas-f1-team': 'haas-f1-team',
                'kick-sauber': 'kick-sauber',
                'williams': 'williams'
            }
            
            # Apply mappings if team name matches
            if team_url_name in team_url_mappings:
                team_url_name = team_url_mappings[team_url_name]
            
            # Construct F1 team URL
            f1_team_url = f"https://www.formula1.com/en/teams/{team_url_name}"
            
            log(f"  Parsing F1 data for team '{team_name}' from {f1_team_url}")
            
            try:
                response = requests.get(f1_team_url, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Initialize team data
                team_official_name = None
                power_unit = None
                chassis = None
                team_logo_url = None
                team_car_url = None
                
                # Find Full Team Name
                full_name_element = soup.find(text=re.compile(r'Full Team Name', re.IGNORECASE))
                if full_name_element:
                    # Look for the next text element or sibling that contains the team name
                    parent = full_name_element.parent
                    if parent:
                        # Try to find the value in various ways
                        next_element = parent.find_next()
                        if next_element:
                            team_official_name = next_element.get_text(strip=True)
                
                # Find Power Unit
                power_unit_element = soup.find(text=re.compile(r'Power Unit', re.IGNORECASE))
                if power_unit_element:
                    parent = power_unit_element.parent
                    if parent:
                        next_element = parent.find_next()
                        if next_element:
                            power_unit = next_element.get_text(strip=True)
                
                # Find Chassis
                chassis_element = soup.find(text=re.compile(r'Chassis', re.IGNORECASE))
                if chassis_element:
                    parent = chassis_element.parent
                    if parent:
                        next_element = parent.find_next()
                        if next_element:
                            chassis = next_element.get_text(strip=True)
                
                # Find team logo (look for images with team logo keywords)
                logo_img = soup.find('img', alt=re.compile(r'logo', re.IGNORECASE))
                if not logo_img:
                    # Try different selectors for logo
                    logo_img = soup.find('img', src=re.compile(r'logo', re.IGNORECASE))
                if logo_img:
                    logo_src = logo_img.get('src', '')
                    if logo_src:
                        if logo_src.startswith('//'):
                            team_logo_url = 'https:' + logo_src
                        elif logo_src.startswith('/'):
                            team_logo_url = 'https://www.formula1.com' + logo_src
                        elif logo_src.startswith('http'):
                            team_logo_url = logo_src
                
                # Find team car image (look for images with car keywords)
                car_img = soup.find('img', alt=re.compile(r'car', re.IGNORECASE))
                if not car_img:
                    # Try different selectors for car
                    car_img = soup.find('img', src=re.compile(r'car', re.IGNORECASE))
                if car_img:
                    car_src = car_img.get('src', '')
                    if car_src:
                        if car_src.startswith('//'):
                            team_car_url = 'https:' + car_src
                        elif car_src.startswith('/'):
                            team_car_url = 'https://www.formula1.com' + car_src
                        elif car_src.startswith('http'):
                            team_car_url = car_src
                
                # Update database if we found any data
                if any([team_official_name, power_unit, chassis, team_logo_url, team_car_url]):
                    log(f"  Found data for '{team_name}': official_name={team_official_name}, power_unit={power_unit}, chassis={chassis}")
                    cursor.execute("""
                        UPDATE team
                        SET team_official_name = COALESCE(?, team_official_name),
                            power_unit = COALESCE(?, power_unit),
                            chassis = COALESCE(?, chassis),
                            team_logo_url = COALESCE(?, team_logo_url),
                            team_car_url = COALESCE(?, team_car_url)
                        WHERE team_id = ?
                    """, (team_official_name, power_unit, chassis, team_logo_url, team_car_url, team_id))
                else:
                    log(f"  No team data found for '{team_name}' at {f1_team_url}")
                    
            except requests.exceptions.RequestException as e:
                log(f"  ERROR: Could not fetch data from {f1_team_url}. Reason: {e}")
                continue
            except Exception as e:
                log(f"  ERROR: Could not parse data from {f1_team_url}. Reason: {e}")
                continue
        
        conn.commit()
        conn.close()
        log("F1 official site team parsing finished.")

    except Exception as e:
        log(f"ERROR: Could not parse team data from F1 official site. Reason: {e}")

def run_team_parsers():
    """Runs all team parsers."""
    log("--- Starting Team Data Enrichment ---")
    parse_team_wiki()
    parse_team_f1()
    log("--- Team Data Enrichment Finished ---")
