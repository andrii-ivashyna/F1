# parse_team.py

import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from io import StringIO
import config
from config import log, show_progress_bar
from fetch_api import get_country_code
import time

def run_team_parsers():
    """Runs all team parsers."""
    parse_team_wiki()
    parse_team_f1()

def parse_team_wiki():
    """Parses Wikipedia for F1 constructor data and updates the database."""
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

        total_constructors = len(constructors_df)
        start_time_wiki_teams = time.time()
        for i, row in constructors_df.iterrows():
            show_progress_bar(i + 1, total_constructors, prefix_text='Wikipedia:    Team', start_time=start_time_wiki_teams)
            name = re.sub(r'\[.*?\]', '', str(row[col])).strip()
            country = re.sub(r'\[.*?\]', '', str(row.get('Licensed in', ''))).strip()
            if name in ['nan', '', 'NaN']: continue
            
            variants = [p.strip() for p in re.split(r'[/\n]+', name) if p.strip()]
            code = get_country_code(country) if country not in ['nan', '', 'NaN'] else None
            if code: cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, country))
            
            for team_id, team_name in db_teams:
                if team_name and any(v.split()[0].lower() == team_name.split()[0].lower() for v in variants):
                    variant = next(v for v in variants if v.split()[0].lower() == team_name.split()[0].lower())
                    cursor.execute("UPDATE team SET team_name = ?, country_fk = COALESCE(?, country_fk) WHERE team_id = ?", (variant, code, team_id))
                    break
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse constructor data from Wikipedia.", 'ERROR', data={'reason': str(e)})

def parse_team_f1():
    """Parses F1 official site for team data."""
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        db_teams = cursor.execute("SELECT team_id, team_name FROM team").fetchall()
        
        total_teams = len(db_teams)
        start_time_f1_teams = time.time()
        for i, (team_id, team_name) in enumerate(db_teams):
            show_progress_bar(i + 1, total_teams, prefix_text='Formula1.com: Team', start_time=start_time_f1_teams)
            if not team_name: continue

            url_name = team_name.lower().replace(' ', '-')
            f1_url = f"https://www.formula1.com/en/teams/{url_name}"
            
            try:
                response = requests.get(f1_url, timeout=15)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                
                team_official_name, power_unit, chassis, logo_image_url, car_image_url = (None,)*5
                
                if (elem := soup.find(string=re.compile(r'Full Team Name', re.I))) and (p := elem.find_parent()) and (n := p.find_next_sibling()): team_official_name = n.get_text(strip=True)
                if (elem := soup.find(string=re.compile(r'Power Unit', re.I))) and (p := elem.find_parent()) and (n := p.find_next_sibling()): power_unit = n.get_text(strip=True)
                if (elem := soup.find(string=re.compile(r'Chassis', re.I))) and (p := elem.find_parent()) and (n := p.find_next_sibling()): chassis = n.get_text(strip=True)
                if (img := soup.find('img', class_=re.compile(r'.*z-40.*h-px-32.*'))) and (src := img.get('src')): logo_image_url = src if src.startswith('http') else 'https://www.formula1.com' + src
                if (img := soup.find('img', class_=re.compile(r'.*z-40.*max-w-full.*'))) and (src := img.get('src')): car_image_url = src if src.startswith('http') else 'https://www.formula1.com' + src
                
                update_data = {k: v for k, v in locals().items() if k in ['team_official_name', 'power_unit', 'chassis', 'logo_image_url', 'car_image_url'] and v}
                
                if update_data:
                    cursor.execute("""
                        UPDATE team SET 
                        team_official_name = COALESCE(?, team_official_name), power_unit = COALESCE(?, power_unit), 
                        chassis = COALESCE(?, chassis), logo_image_url = COALESCE(?, logo_image_url), 
                        car_image_url = COALESCE(?, car_image_url)
                        WHERE team_id = ?
                    """, (team_official_name, power_unit, chassis, logo_image_url, car_image_url, team_id))
            except Exception as e:
                log("Could not fetch or parse data.", 'ERROR', indent=2, data={'error': str(e)})
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse team data from F1 site.", 'ERROR', data={'reason': str(e)})
