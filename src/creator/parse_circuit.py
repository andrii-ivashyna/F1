# parse_circuit.py

import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from io import StringIO
import config
from config import log

def run_circuit_parsers():
    """Runs all circuit parsers."""
    log("Enriching Circuit Data", 'HEADING')
    parse_circuit_wiki()
    parse_circuit_f1()

def parse_circuit_wiki():
    """Parses Wikipedia for F1 circuit data and updates the database."""
    log("Parsing from Wikipedia", 'SUBHEADING')
    url = "https://en.wikipedia.org/wiki/List_of_Formula_One_circuits"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        
        table = soup.find('table', {'class': 'wikitable sortable'})
        circuits_df = pd.read_html(StringIO(str(table)))[0]

        active_circuits_df = circuits_df[circuits_df['Circuit'].str.strip().str.endswith('*')].copy()
        log(f"Found {len(active_circuits_df)} active circuits on Wikipedia.", 'SUCCESS', indent=1)

        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        db_circuits = cursor.execute("SELECT circuit_key, circuit_name FROM circuit").fetchall()

        for _, row in active_circuits_df.iterrows():
            for key, name in db_circuits:
                if name and (name.lower() in str(row['Location']).lower() or name.lower() in str(row['Circuit']).lower()):
                    update_data = {
                        'official_name': str(row['Circuit']).strip().rstrip('*').strip(),
                        'location': str(row['Location']),
                        'type': 'street' if 'street' in str(row['Type']).lower() else 'race',
                        'direction': 'anti-clockwise' if 'anti' in str(row['Direction']).lower() else 'clockwise',
                        'length_km': (m.group(1) if (m := re.search(r"(\d+\.\d+)", str(row.get('Last length used')))) else None),
                        'turns': (m.group(0) if (m := re.search(r'\d+', str(row.get('Turns')))) else None)
                    }
                    log(f"Updating '{name}'", 'DATA', data=update_data, indent=2)
                    cursor.execute("""
                        UPDATE circuit SET circuit_official_name=?, location=?, type=?, direction=?, length_km=?, turns=? 
                        WHERE circuit_key=?
                    """, (
                        update_data['official_name'], update_data['location'], update_data['type'], 
                        update_data['direction'], update_data['length_km'], update_data['turns'], key
                    ))
                    break # Move to the next row in DataFrame
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse circuit data from Wikipedia.", 'ERROR', data={'reason': str(e)})

def parse_circuit_f1():
    """Parses F1 official site for circuit data and updates the database."""
    log("Parsing from Formula1.com", 'SUBHEADING')
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        circuit_data = cursor.execute("""
            SELECT DISTINCT c.circuit_key, c.circuit_name, co.country_name, m.date_start
            FROM circuit c JOIN meeting m ON c.circuit_key = m.circuit_fk JOIN country co ON c.country_fk = co.country_code
            WHERE m.date_start IS NOT NULL ORDER BY m.date_start DESC
        """).fetchall()

        for key, name, country, date in circuit_data:
            if not all([key, country, date]): continue
            country_url = country.lower().replace(' ', '-')
            country_map = {'united-kingdom': 'great-britain', 'united-arab-emirates': 'abudhabi', 'usa': 'united-states'}
            country_url = country_map.get(country_url, country_url)
            f1_url = f"https://www.formula1.com/en/racing/{date[:4]}/{country_url}"
            
            log(f"Requesting data for '{name}' from {f1_url}", 'INFO', indent=1)
            
            try:
                response = requests.get(f1_url, timeout=15)
                if response.status_code == 404:
                    log("Page not found (404), skipping.", 'WARNING', indent=2); continue
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')
                
                laps = None
                circuit_map_url = None

                # Parse Laps from the specific structure
                laps_dt = soup.find('dt', class_=re.compile(r'typography-module_body-xs-semibold'), string=re.compile(r'Number of Laps', re.I))
                if laps_dt:
                    laps_dd = laps_dt.find_next_sibling('dd', class_=re.compile(r'typography-module_display-l-bold'))
                    if laps_dd and (match := re.search(r'(\d+)', laps_dd.get_text(strip=True))):
                        laps = int(match.group(1))
                
                # Parse Circuit Map URL from img with specific class
                circuit_img = soup.find('img', class_='w-full h-full object-contain')
                if circuit_img and (src := circuit_img.get('src')):
                    if src.startswith('http'):
                        circuit_map_url = src
                    else:
                        circuit_map_url = 'https://www.formula1.com' + src
                
                if laps or circuit_map_url:
                    log("Found data", 'DATA', data={'laps': laps, 'circuit_map_url': circuit_map_url}, indent=2)
                    cursor.execute("UPDATE circuit SET laps=COALESCE(?,laps), circuit_map_url=COALESCE(?,circuit_map_url) WHERE circuit_key=?", (laps, circuit_map_url, key))
                else:
                    log("No specific data (laps, map) found on page.", 'WARNING', indent=2)
            except Exception as e:
                log("Could not fetch or parse data.", 'ERROR', indent=2, data={'url': config.Style.url(f1_url), 'error': str(e)})
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse circuit data from F1 site.", 'ERROR', data={'reason': str(e)})
