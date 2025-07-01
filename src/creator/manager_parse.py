# manager_parse.py
"""
Manages all data parsing and enrichment operations from external sources like Wikipedia and F1.com.
Consolidates parsing for circuits, teams, and drivers into a single, optimized module.
"""

# --- Imports ---
import sqlite3
import time
import re
from io import StringIO
import requests
import pandas as pd
from bs4 import BeautifulSoup
import config
from config import log, show_progress_bar
from manager_db import get_country_code

# --- Generic F1.com Parsing Logic ---

def _execute_f1_parse_loop(entity_name, table_name, db_select_query, url_builder_func, data_extractor_func, db_update_func):
    """
    A generic helper to parse data from F1.com for different entities.
    Handles DB connection, iteration, progress bar, requests, and error logging.
    """
    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()

        total_in_db = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        items_to_parse = cursor.execute(db_select_query).fetchall()
        items_to_process_count = len(items_to_parse)

        if items_to_process_count == 0:
            log(f"No {entity_name.lower()}s found to parse from F1.com.", 'WARNING')
            conn.close()
            return

        start_time = time.time()
        for i, item_data in enumerate(items_to_parse):
            prefix_text = f'F1.com | {entity_name.capitalize():<7} | {total_in_db}'
            show_progress_bar(i + 1, items_to_process_count, prefix_text=prefix_text, start_time=start_time)

            f1_url = url_builder_func(item_data)
            if not f1_url:
                continue

            try:
                response = requests.get(f1_url, timeout=15)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'lxml')

                parsed_data = data_extractor_func(soup)
                if parsed_data:
                    db_update_func(cursor, item_data, parsed_data)

            except requests.exceptions.RequestException as e:
                log(f"Could not fetch data for item {item_data[0]}.", 'ERROR', indent=2, data={'url': config.Style.url(f1_url), 'error': str(e)})

        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        log(f"Database error during F1.com {entity_name.lower()} parsing.", 'ERROR', data={'reason': str(e)})
    except Exception as e:
        import traceback
        log(f"An unexpected error occurred during F1.com {entity_name.lower()} parsing.", 'ERROR', data={'reason': str(e)})
        traceback.print_exc()


# --- Driver Parsing (F1.com) ---

def _build_driver_url_f1(driver_data):
    """Builds the F1.com URL for a specific driver."""
    _, driver_name = driver_data
    return f"https://www.formula1.com/en/drivers/{driver_name.lower().replace(' ', '-')}" if driver_name else None

def _extract_driver_data_f1(soup):
    """Extracts image URLs and country from a driver's F1.com page soup."""
    country_name = (elem.get_text(strip=True) if (elem := soup.find('p', class_=re.compile(r'typography-module_body-xs-semibold'))) else None)
    
    # --- FIX START ---
    # Refactored the image URL extraction to prevent UnboundLocalError.
    driver_image_url = None
    if (img := soup.find('img', class_=re.compile(r'absolute.*w-\[222px\].*'))) and (src := img.get('src')):
        driver_image_url = src if src.startswith('http') else f'https://www.formula1.com{src}'

    number_image_url = None
    if (div := soup.find('div', style=re.compile(r'mask-image:\s*url\('))) and \
       (style := div.get('style', '')) and \
       (m := re.search(r'url\((?:&quot;|")?([^"&)]+)(?:&quot;|")?\)', style)):
        url_part = m.group(1)
        number_image_url = url_part if url_part.startswith('http') else f'https://www.formula1.com{url_part}'
    # --- FIX END ---
    
    return {'country_name': country_name, 'driver_image_url': driver_image_url, 'number_image_url': number_image_url}

def _update_driver_db_f1(cursor, driver_data, parsed_data):
    """Updates the driver table with the parsed F1.com data."""
    driver_code, _ = driver_data
    country_code = None
    if parsed_data.get('country_name') and (country_code := get_country_code(parsed_data['country_name'])):
        cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (country_code, parsed_data['country_name']))

    update_values = (country_code, parsed_data.get('driver_image_url'), parsed_data.get('number_image_url'), driver_code)
    if any(val for val in update_values[:-1]):
        cursor.execute("""
            UPDATE driver SET country_fk = COALESCE(?, country_fk), 
            driver_image_url = COALESCE(?, driver_image_url), number_image_url = COALESCE(?, number_image_url)
            WHERE driver_code = ? """, update_values)

def parse_driver_f1():
    """Parses F1 official site for driver data and updates the database."""
    _execute_f1_parse_loop(
        entity_name="Driver",
        table_name="driver",
        db_select_query="SELECT driver_code, driver_name FROM driver WHERE driver_name IS NOT NULL",
        url_builder_func=_build_driver_url_f1,
        data_extractor_func=_extract_driver_data_f1,
        db_update_func=_update_driver_db_f1
    )


# --- Team Parsing (F1.com & Wikipedia) ---

def _build_team_url_f1(team_data):
    """Builds the F1.com URL for a specific team."""
    _, team_name = team_data
    return f"https://www.formula1.com/en/teams/{team_name.lower().replace(' ', '-')}" if team_name else None

def _extract_team_data_f1(soup):
    """Extracts details from a team's F1.com page soup."""
    data = {}
    details_map = {'Full Team Name': 'team_official_name', 'Power Unit': 'power_unit', 'Chassis': 'chassis'}
    for key, value in details_map.items():
        if (elem := soup.find(string=re.compile(key, re.I))) and (p := elem.find_parent()) and (n := p.find_next_sibling()):
            data[value] = n.get_text(strip=True)
    
    if (img := soup.find('img', class_=re.compile(r'.*z-40.*h-px-32.*'))) and (src := img.get('src')):
        data['logo_image_url'] = src if src.startswith('http') else 'https://www.formula1.com' + src
    if (img := soup.find('img', class_=re.compile(r'.*z-40.*max-w-full.*'))) and (src := img.get('src')):
        data['car_image_url'] = src if src.startswith('http') else 'https://www.formula1.com' + src
    return data

def _update_team_db_f1(cursor, team_data, parsed_data):
    """Updates the team table with the parsed F1.com data."""
    team_id, _ = team_data
    update_values = (
        parsed_data.get('team_official_name'), parsed_data.get('power_unit'), parsed_data.get('chassis'),
        parsed_data.get('logo_image_url'), parsed_data.get('car_image_url'), team_id
    )
    if any(val for val in update_values[:-1]):
        cursor.execute("""
            UPDATE team SET team_official_name = COALESCE(?, team_official_name), 
            power_unit = COALESCE(?, power_unit), chassis = COALESCE(?, chassis), 
            logo_image_url = COALESCE(?, logo_image_url), car_image_url = COALESCE(?, car_image_url)
            WHERE team_id = ? """, update_values)

def parse_team_f1():
    """Parses F1 official site for team data."""
    _execute_f1_parse_loop(
        entity_name="Team",
        table_name="team",
        db_select_query="SELECT team_id, team_name FROM team WHERE team_name IS NOT NULL",
        url_builder_func=_build_team_url_f1,
        data_extractor_func=_extract_team_data_f1,
        db_update_func=_update_team_db_f1
    )

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
        db_team_count = len(db_teams)
        
        teams_to_process_count = len(constructors_df)
        start_time = time.time()
        for i, row in constructors_df.iterrows():
            show_progress_bar(i + 1, teams_to_process_count, prefix_text=f'Wiki | Team | {db_team_count}', start_time=start_time)
            name = re.sub(r'\[.*?\]', '', str(row[col])).strip()
            country = re.sub(r'\[.*?\]', '', str(row.get('Licensed in', '')).strip() if 'Licensed in' in row else '')
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
        log("Could not parse constructor data from Wikipedia.", 'ERROR', data={'url': url, 'reason': str(e)})


# --- Circuit Parsing (F1.com & Wikipedia) ---

def _build_circuit_url_f1(circuit_data):
    """Builds the F1.com URL for a specific circuit race."""
    _, _, country, date = circuit_data
    if not all([country, date]): return None
    country_url = country.lower().replace(' ', '-')
    country_map = {'united-kingdom': 'great-britain', 'united-arab-emirates': 'abudhabi', 'usa': 'united-states'}
    country_url = country_map.get(country_url, country_url)
    return f"https://www.formula1.com/en/racing/{date[:4]}/{country_url}"

def _extract_circuit_data_f1(soup):
    """Extracts details from a circuit's F1.com page soup."""
    data = {}
    if (dt := soup.find('dt', string=re.compile(r'Number of Laps', re.I))) and (dd := dt.find_next_sibling('dd')) and (m := re.search(r'(\d+)', dd.get_text(strip=True))):
        data['laps'] = int(m.group(1))
    if (dt := soup.find('dt', string=re.compile(r'Circuit Length', re.I))) and (dd := dt.find_next_sibling('dd')) and (m := re.search(r'(\d+\.\d+)', dd.get_text(strip=True))):
        data['length_km'] = float(m.group(1))
    if (img := soup.find('img', class_='w-full h-full object-contain')) and (src := img.get('src')):
        data['map_image_url'] = src if src.startswith('http') else 'https://www.formula1.com' + src
    if not data:
        log("No specific data (laps, length, map) found on page.", 'WARNING', indent=2)
    return data

def _update_circuit_db_f1(cursor, circuit_data, parsed_data):
    """Updates the circuit table with the parsed F1.com data."""
    circuit_key, _, _, _ = circuit_data
    update_values = (parsed_data.get('laps'), parsed_data.get('length_km'), parsed_data.get('map_image_url'), circuit_key)
    if any(val for val in update_values[:-1]):
        cursor.execute("UPDATE circuit SET laps=COALESCE(?,laps), length_km=COALESCE(?,length_km), map_image_url=COALESCE(?,map_image_url) WHERE circuit_key=?", update_values)

def parse_circuit_f1():
    """Parses F1 official site for circuit data."""
    _execute_f1_parse_loop(
        entity_name="Circuit",
        table_name="circuit",
        db_select_query="""
            SELECT DISTINCT c.circuit_key, c.circuit_name, co.country_name, m.timestamp_utc
            FROM circuit c JOIN meeting m ON c.circuit_key = m.circuit_fk JOIN country co ON c.country_fk = co.country_code
            WHERE m.timestamp_utc IS NOT NULL ORDER BY m.timestamp_utc DESC """,
        url_builder_func=_build_circuit_url_f1,
        data_extractor_func=_extract_circuit_data_f1,
        db_update_func=_update_circuit_db_f1
    )

def parse_circuit_wiki():
    """Parses Wikipedia for F1 circuit data and updates the database."""
    url = "https://en.wikipedia.org/wiki/List_of_Formula_One_circuits"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table', {'class': 'wikitable sortable'})
        circuits_df = pd.read_html(StringIO(str(table)))[0]

        active_circuits_df = circuits_df[circuits_df['Circuit'].str.strip().str.endswith('*')].copy()

        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        db_circuits = cursor.execute("SELECT circuit_key, circuit_name FROM circuit").fetchall()
        db_circuit_count = len(db_circuits)

        circuits_to_process_count = len(active_circuits_df)
        start_time = time.time()
        for i, (_, row) in enumerate(active_circuits_df.iterrows()):
            show_progress_bar(i + 1, circuits_to_process_count, prefix_text=f'Wiki | Circuit | {db_circuit_count}', start_time=start_time)
            for key, name in db_circuits:
                location_match = name and isinstance(row.get('Location'), str) and name.lower() in row['Location'].lower()
                circuit_match = name and isinstance(row.get('Circuit'), str) and name.lower() in row['Circuit'].lower()

                if location_match or circuit_match:
                    update_data = {
                        'official_name': str(row['Circuit']).strip().rstrip('*').strip(),
                        'location': str(row['Location']),
                        'type': 'street' if 'street' in str(row['Type']).lower() else 'race',
                        'direction': 'anti-clockwise' if 'anti' in str(row['Direction']).lower() else 'clockwise',
                        'turns': (m.group(0) if (m := re.search(r'\d+', str(row.get('Turns')))) else None)
                    }
                    cursor.execute("""
                        UPDATE circuit SET circuit_official_name=?, location=?, type=?, direction=?, turns=? 
                        WHERE circuit_key=? """,
                        (update_data['official_name'], update_data['location'], update_data['type'], 
                         update_data['direction'], update_data['turns'], key)
                    )
                    break
        conn.commit()
        conn.close()
    except Exception as e:
        log("Could not parse circuit data from Wikipedia.", 'ERROR', data={'url': url, 'reason': str(e)})


# --- Parser Runners ---

def run_all_parsers():
    """Runs all parsing operations in sequence."""
    log("Enriching data from external sources", 'SUBHEADING')
    run_circuit_parsers()
    run_team_parsers()
    run_driver_parsers()

def run_circuit_parsers():
    """Runs all circuit-related parsers."""
    parse_circuit_wiki()
    parse_circuit_f1()

def run_team_parsers():
    """Runs all team-related parsers."""
    parse_team_wiki()
    parse_team_f1()

def run_driver_parsers():
    """Runs all driver-related parsers."""
    parse_driver_f1()
