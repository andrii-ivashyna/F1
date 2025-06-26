# parse_team.py

import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from io import StringIO
import config
from config import log
from fetch_api import get_country_code

def run_team_parsers():
    """Runs all team parsers."""
    log("Enriching Team Data", 'HEADING')
    parse_team_wiki()
    parse_team_f1()

def parse_team_wiki():
    """Parses Wikipedia for F1 constructor data and updates the database."""
    log("Parsing from Wikipedia", 'SUBHEADING')
    url = "https://en.wikipedia.org/wiki/List_of_Formula_One_constructors"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table', {'class': 'wikitable'})
        constructors_df = pd.read_html(StringIO(str(table)))[0]
        
        col = next((c for c in constructors_df.columns if 'constructor' in str(c).lower()), None)
        if not col:
            log("Could not find 'Constructor' column.", 'ERROR', data={'available': list(constructors_df.columns)})
            return

        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        db_teams = cursor.execute("SELECT team_id, team_name FROM team").fetchall()

        for _, row in constructors_df.iterrows():
            name = re.sub(r'\[.*?\]', '', str(row[col])).strip()
            country = re.sub(r'\[.*?\]', '', str(row.get('Licensed in', ''))).strip()
            if name in ['nan', '', 'NaN']: continue
            
            variants = [p.strip() for p in re.split(r'[/\n]+', name) if p.strip()]
            code = get_country_code(country) if country not in ['nan', '', 'NaN'] else None
            if code: cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, country))
            
            for team_id, team_name in db_teams:
                if team_name and any(v.split()[0].lower() == team_name.split()[0].lower() for v in variants):
                    variant = next(v for v in variants if v.split()[0].lower() == team_name.split()[0].lower())
                    update_data = {'team_name': variant, 'country_fk': code}
                    log(f"Updating '{team_name}'", 'DATA', indent=1, data=update_data)
                    cursor.execute("UPDATE team SET team_name = ?, country_fk = COALESCE(?, country_fk) WHERE team_id = ?", (variant, code, team_id))
                    break # Move to the next row in DataFrame
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse constructor data from Wikipedia.", 'ERROR', data={'reason': str(e)})

def parse_team_f1():
    """Parses F1 official site for team data."""
    log("Parsing from Formula1.com", 'SUBHEADING')
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        db_teams = cursor.execute("SELECT team_id, team_name FROM team").fetchall()
        
        for team_id, team_name in db_teams:
            if not team_name: continue

            # Generate URL slug from team name (lowercase with dashes)
            url_name = team_name.lower().replace(' ', '-')
            f1_url = f"https://www.formula1.com/en/teams/{url_name}"
            
            log(f"Requesting data for '{team_name}' from {f1_url}", 'INFO', indent=1)
            
            try:
                response = requests.get(f1_url, timeout=15)
                if response.status_code == 404:
                    log("Page not found (404), skipping.", 'WARNING', indent=2)
                    continue
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                
                team_official_name = None
                power_unit = None
                chassis = None
                logo_image_url = None
                car_image_url = None
                
                # Find Full Team Name
                full_name_elem = soup.find(string=re.compile(r'Full Team Name', re.I))
                if full_name_elem:
                    parent = full_name_elem.find_parent()
                    if parent:
                        # Look for next sibling or nearby element containing the team name
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            team_official_name = next_elem.get_text(strip=True)
                
                # Find Power Unit
                power_unit_elem = soup.find(string=re.compile(r'Power Unit', re.I))
                if power_unit_elem:
                    parent = power_unit_elem.find_parent()
                    if parent:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            power_unit = next_elem.get_text(strip=True)
                
                # Find Chassis
                chassis_elem = soup.find(string=re.compile(r'Chassis', re.I))
                if chassis_elem:
                    parent = chassis_elem.find_parent()
                    if parent:
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            chassis = next_elem.get_text(strip=True)
                
                # Find team logo URL - updated selector based on provided HTML
                logo_img = soup.find('img', class_='relative z-40 h-px-32')
                if not logo_img:
                    # Fallback: try to find img with class containing 'z-40' and 'h-px-32'
                    logo_img = soup.find('img', class_=re.compile(r'.*z-40.*h-px-32.*'))
                if not logo_img:
                    # Another fallback: look for img with src containing team logo pattern
                    logo_img = soup.find('img', src=re.compile(r'.*logo.*', re.I))
                
                if logo_img and (src := logo_img.get('src')):
                    logo_image_url = src if src.startswith('http') else 'https://www.formula1.com' + src
                
                # Find team car URL - updated selector based on provided HTML
                car_img = soup.find('img', class_=re.compile(r'.*z-40.*max-w-full.*max-h-\[90px\].*'))
                if not car_img:
                    # Fallback: try to find img with class containing car-related patterns
                    car_img = soup.find('img', class_=re.compile(r'.*max-w-full.*max-h-.*'))
                if not car_img:
                    # Another fallback: look for img with src containing car pattern
                    car_img = soup.find('img', src=re.compile(r'.*car.*', re.I))
                
                if car_img and (src := car_img.get('src')):
                    car_image_url = src if src.startswith('http') else 'https://www.formula1.com' + src
                
                update_data = {}
                if team_official_name:
                    update_data['team_official_name'] = team_official_name
                if power_unit:
                    update_data['power_unit'] = power_unit
                if chassis:
                    update_data['chassis'] = chassis
                if logo_image_url:
                    update_data['logo_image_url'] = logo_image_url
                if car_image_url:
                    update_data['car_image_url'] = car_image_url
                
                if update_data:
                    log("Found data", 'DATA', indent=2, data=update_data)
                    cursor.execute("""
                        UPDATE team SET 
                        team_official_name = COALESCE(?, team_official_name), 
                        power_unit = COALESCE(?, power_unit), 
                        chassis = COALESCE(?, chassis), 
                        logo_image_url = COALESCE(?, logo_image_url), 
                        car_image_url = COALESCE(?, car_image_url)
                        WHERE team_id = ?
                    """, (team_official_name, power_unit, chassis, logo_image_url, car_image_url, team_id))
                else:
                    log("No new data found on page.", 'WARNING', indent=2)
            except Exception as e:
                log("Could not fetch or parse data.", 'ERROR', indent=2, data={'error': str(e)})
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse team data from F1 site.", 'ERROR', data={'reason': str(e)})
