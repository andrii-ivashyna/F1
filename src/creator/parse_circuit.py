# parse_circuit.py

import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from io import StringIO
import config
from fetch_api import log

def parse_circuit_wiki():
    """Parses Wikipedia for F1 circuit data and updates the database."""
    log("Starting Wikipedia circuit parsing...")
    url = "https://en.wikipedia.org/wiki/List_of_Formula_One_circuits"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        table = soup.find('table', {'class': 'wikitable sortable'})
        # Use StringIO to fix pandas FutureWarning
        circuits_df = pd.read_html(StringIO(str(table)))[0]

        # 1. Get data only from rows that have * in the end of Circuit column
        active_circuits_df = circuits_df[circuits_df['Circuit'].str.strip().str.endswith('*')].copy()

        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()

        db_circuits = cursor.execute("SELECT circuit_key, circuit_name, location FROM circuit").fetchall()

        for index, row in active_circuits_df.iterrows():
            wiki_circuit_name = str(row['Circuit'])
            wiki_location = str(row['Location'])
            
            # 2. Check logic for parsing data about circuits
            for circuit_key, db_circuit_name, db_location in db_circuits:
                # Match if db_name is in location, otherwise if it's in the circuit name
                match = False
                if db_circuit_name and db_circuit_name.lower() in wiki_location.lower():
                    match = True
                elif db_circuit_name and db_circuit_name.lower() in wiki_circuit_name.lower():
                    match = True
                
                if match:
                    # 3. Save circuit_official_name without trailing *
                    official_name = wiki_circuit_name.strip().rstrip('*').strip()
                    log(f"  Updating circuit '{db_circuit_name}' with data for '{official_name}'")
                    
                    # 4. Change logic for direction parsing
                    dir_str = str(row['Direction']).lower().strip()
                    if dir_str == 'clockwise':
                        direction = 'clockwise'
                    elif dir_str == 'anti-clockwise':
                        direction = 'anti-clockwise'
                    else:
                        direction = 'both'
                        
                    # 5. Get data from "Last length used"
                    length_str = str(row.get('Last length used', ''))
                    length_km_match = re.search(r"(\d+\.\d+)", length_str)
                    length_km = float(length_km_match.group(1)) if length_km_match else None

                    # Type
                    type_str = str(row['Type']).lower()
                    circuit_type = 'street' if 'street' in type_str else 'race' if 'race' in type_str else None

                    # Turns
                    turns_match = re.search(r'\d+', str(row.get('Turns', '')))
                    turns = int(turns_match.group(0)) if turns_match else None

                    cursor.execute("""
                        UPDATE circuit
                        SET circuit_official_name = ?, location = ?, type = ?, direction = ?, length_km = ?, turns = ?
                        WHERE circuit_key = ?
                    """, (
                        official_name, row['Location'], circuit_type, direction, length_km, turns, circuit_key
                    ))
                    break 

        conn.commit()
        conn.close()
        log("Wikipedia circuit parsing finished.")

    except Exception as e:
        log(f"ERROR: Could not parse circuit data from Wikipedia. Reason: {e}")

def parse_circuit_f1():
    """Parses F1 official site for circuit data and updates the database."""
    log("Starting F1 official site circuit parsing...")
    
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()

        # Get circuits with their country names and meeting dates
        circuit_data = cursor.execute("""
            SELECT DISTINCT c.circuit_key, c.circuit_name, co.country_name, m.date_start
            FROM circuit c
            JOIN meeting m ON c.circuit_key = m.circuit_fk
            JOIN country co ON c.country_fk = co.country_code
            WHERE m.date_start IS NOT NULL
            ORDER BY m.date_start DESC
        """).fetchall()

        for circuit_key, circuit_name, country_name, date_start in circuit_data:
            if not all([circuit_key, country_name, date_start]):
                continue
                
            # Extract year from date_start (ISO 8601 format)
            year = date_start[:4] if len(date_start) >= 4 else None
            if not year:
                continue
                
            # Convert country name to lowercase and replace spaces with hyphens for URL
            country_url = country_name.lower().replace(' ', '-').replace('united-states', 'united-states').replace('united-kingdom', 'great-britain')
            
            # Special cases for country names in F1 URLs
            country_mappings = {
                'united-kingdom': 'great-britain',
                'united-arab-emirates': 'abu-dhabi'
            }
            
            if country_url in country_mappings:
                country_url = country_mappings[country_url]
            
            # Generate F1 official site URL
            f1_url = f"https://www.formula1.com/en/racing/{year}/{country_url}"
            
            log(f"  Parsing F1 data for circuit '{circuit_name}' from {f1_url}")
            
            try:
                response = requests.get(f1_url, timeout=15)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for circuit section
                laps = None
                circuit_map_url = None
                
                # Find laps information using specific HTML structure
                # Look for dt element with "Number of Laps" text, then find the corresponding dd element
                dt_elements = soup.find_all('dt')
                for dt in dt_elements:
                    if 'number of laps' in dt.get_text().lower():
                        # Find the next dd sibling element
                        dd = dt.find_next_sibling('dd')
                        if dd:
                            laps_text = dd.get_text(strip=True)
                            laps_match = re.search(r'(\d+)', laps_text)
                            if laps_match:
                                laps = int(laps_match.group(1))
                                break
                
                # Find circuit map image in the same context/div as laps
                # Look for images with circuit-related keywords and specific class structure
                img_tags = soup.find_all('img', class_=re.compile(r'w-full.*h-full.*object-contain'))
                for img in img_tags:
                    src = img.get('src', '')
                    alt = img.get('alt', '').lower()
                    
                    # Check if image is likely a circuit map based on src pattern and alt text
                    if ('circuit' in src.lower() and 'maps' in src.lower()) or \
                       ('circuit' in alt and '.png' in alt):
                        # Ensure full URL
                        if src.startswith('//'):
                            circuit_map_url = 'https:' + src
                        elif src.startswith('/'):
                            circuit_map_url = 'https://www.formula1.com' + src
                        elif src.startswith('http'):
                            circuit_map_url = src
                        
                        if circuit_map_url:
                            break
                
                # Update database if we found data
                if laps or circuit_map_url:
                    # Extract filename from URL for shortened display
                    map_display = "..." + circuit_map_url.split('/')[-1] if circuit_map_url else None
                    log(f"  Found data for '{circuit_name}': laps={laps}, map_url={map_display}")
                    cursor.execute("""
                        UPDATE circuit
                        SET laps = ?, circuit_map_url = ?
                        WHERE circuit_key = ?
                    """, (laps, circuit_map_url, circuit_key))
                else:
                    log(f"  No circuit data found for '{circuit_name}' at {f1_url}")
                    
            except requests.exceptions.RequestException as e:
                log(f"  ERROR: Could not fetch data from {f1_url}. Reason: {e}")
                continue
            except Exception as e:
                log(f"  ERROR: Could not parse data from {f1_url}. Reason: {e}")
                continue

        conn.commit()
        conn.close()
        log("F1 official site circuit parsing finished.")

    except Exception as e:
        log(f"ERROR: Could not parse circuit data from F1 official site. Reason: {e}")

def run_circuit_parsers():
    """Runs all circuit parsers."""
    log("--- Starting Circuit Data Enrichment ---")
    parse_circuit_wiki()
    parse_circuit_f1()
    log("--- Circuit Data Enrichment Finished ---")
