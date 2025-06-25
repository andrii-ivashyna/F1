# parse_wiki.py

import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from io import StringIO
import config
from fetch_api import log

def parse_circuits():
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
                    log(f"  Updating circuit '{db_circuit_name}' with data for '{wiki_circuit_name.strip()}'")
                    
                    # 3. Save circuit_official_name without trailing *
                    official_name = wiki_circuit_name.strip().rstrip('*').strip()
                    
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

def run_wiki_parsers():
    """Runs all Wikipedia parsers."""
    log("--- Starting Wikipedia Data Enrichment ---")
    # Constructor parsing is now removed
    parse_circuits()
    log("--- Wikipedia Data Enrichment Finished ---")
