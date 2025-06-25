# parse_wiki.py

import sqlite3
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from io import StringIO
import config
from fetch_api import log, get_country_code

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

def parse_constructors():
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

def run_wiki_parsers():
    """Runs all Wikipedia parsers."""
    log("--- Starting Wikipedia Data Enrichment ---")
    parse_circuits()
    parse_constructors()
    log("--- Wikipedia Data Enrichment Finished ---")
