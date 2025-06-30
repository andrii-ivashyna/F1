# parse_circuit.py

import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from io import StringIO
import config
from config import log, show_progress_bar
import time

def parse_circuit_wiki():
    """Parses Wikipedia for F1 circuit data and updates the database."""
    try:
        # Get total circuits count from database first
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        total_circuits_in_db = cursor.execute("SELECT COUNT(*) FROM circuit").fetchone()[0]
        
        response = requests.get("https://en.wikipedia.org/wiki/List_of_Formula_One_circuits", timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        
        table = soup.find('table', {'class': 'wikitable sortable'})
        circuits_df = pd.read_html(StringIO(str(table)))[0]

        active_circuits_df = circuits_df[circuits_df['Circuit'].str.strip().str.endswith('*')].copy()

        db_circuits = cursor.execute("SELECT circuit_key, circuit_name FROM circuit").fetchall()

        start_time_wiki_circuits = time.time()
        for i, (_, row) in enumerate(active_circuits_df.iterrows()):
            show_progress_bar(i + 1, len(active_circuits_df), prefix_text=f'Wiki | Circuit | {total_circuits_in_db}', start_time=start_time_wiki_circuits)
            for key, name in db_circuits:
                if name and (name.lower() in str(row['Location']).lower() or name.lower() in str(row['Circuit']).lower()):
                    update_data = {
                        'official_name': str(row['Circuit']).strip().rstrip('*').strip(),
                        'location': str(row['Location']),
                        'type': 'street' if 'street' in str(row['Type']).lower() else 'race',
                        'direction': 'anti-clockwise' if 'anti' in str(row['Direction']).lower() else 'clockwise',
                        'turns': (m.group(0) if (m := re.search(r'\d+', str(row.get('Turns')))) else None)
                    }
                    cursor.execute("""
                        UPDATE circuit SET circuit_official_name=?, location=?, type=?, direction=?, turns=? 
                        WHERE circuit_key=?
                    """, (
                        update_data['official_name'], update_data['location'], update_data['type'], 
                        update_data['direction'], update_data['turns'], key
                    ))
                    break
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse circuit data from Wikipedia.", 'ERROR', data={'reason': str(e)})

def parse_circuit_f1():
    """Parses F1 official site for circuit data and updates the database."""
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        
        # Get total circuits count from database first
        total_circuits_in_db = cursor.execute("SELECT COUNT(*) FROM circuit").fetchone()[0]
        
        circuit_data = cursor.execute("""
            SELECT DISTINCT c.circuit_key, c.circuit_name, co.country_name, m.date_start
            FROM circuit c JOIN meeting m ON c.circuit_key = m.circuit_fk JOIN country co ON c.country_fk = co.country_code
            WHERE m.date_start IS NOT NULL ORDER BY m.date_start DESC
        """).fetchall()

        start_time_f1_circuits = time.time()
        for i, (key, name, country, date) in enumerate(circuit_data):
            show_progress_bar(i + 1, len(circuit_data), prefix_text=f'F1.com | Circuit | {total_circuits_in_db}', start_time=start_time_f1_circuits)
            if not all([key, country, date]): continue
            country_url = country.lower().replace(' ', '-')
            country_map = {'united-kingdom': 'great-britain', 'united-arab-emirates': 'abudhabi', 'usa': 'united-states'}
            country_url = country_map.get(country_url, country_url)
            f1_url = f"https://www.formula1.com/en/racing/{date[:4]}/{country_url}"
            
            try:
                response = requests.get(f1_url, timeout=15)
                if response.status_code == 404:
                    log("Page not found (404), skipping.", 'WARNING', indent=2); continue
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                
                laps = None
                length_km = None
                map_image_url = None

                laps_dt = soup.find('dt', class_=re.compile(r'typography-module_body-xs-semibold'), string=re.compile(r'Number of Laps', re.I))
                if laps_dt:
                    laps_dd = laps_dt.find_next_sibling('dd', class_=re.compile(r'typography-module_display-l-bold'))
                    if laps_dd and (match := re.search(r'(\d+)', laps_dd.get_text(strip=True))):
                        laps = int(match.group(1))
                
                length_dt = soup.find('dt', class_=re.compile(r'typography-module_body-s-compact-semibold'), string=re.compile(r'Circuit Length', re.I))
                if length_dt:
                    length_dd = length_dt.find_next_sibling('dd', class_=re.compile(r'typography-module_desktop-headline-small-bold'))
                    if length_dd and (match := re.search(r'(\d+\.\d+)', length_dd.get_text(strip=True))):
                        length_km = float(match.group(1))
                
                circuit_img = soup.find('img', class_='w-full h-full object-contain')
                if circuit_img and (src := circuit_img.get('src')):
                    map_image_url = src if src.startswith('http') else 'https://www.formula1.com' + src
                
                if laps or length_km or map_image_url:
                    cursor.execute("UPDATE circuit SET laps=COALESCE(?,laps), length_km=COALESCE(?,length_km), map_image_url=COALESCE(?,map_image_url) WHERE circuit_key=?", (laps, length_km, map_image_url, key))
                else:
                    log("No specific data (laps, length, map) found on page.", 'WARNING', indent=2)
            except Exception as e:
                log("Could not fetch or parse data.", 'ERROR', indent=2, data={'url': config.Style.url(f1_url), 'error': str(e)})
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse circuit data from F1 site.", 'ERROR', data={'reason': str(e)})
