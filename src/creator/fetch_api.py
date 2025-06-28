# fetch_api.py

import sqlite3
import time
import requests
import pycountry
import config
import sys
from config import log, show_progress_bar

# --- Formatting Helpers ---
def format_int_date(date_str):
    if isinstance(date_str, str) and len(date_str) > 19: return date_str[:19]
    return date_str

def format_real_date(date_str):
    """Formats weather date string to YYYY-MM-DDTHH:MM:SS.sss"""
    if isinstance(date_str, str) and len(date_str) > 23: return date_str[:23]
    return date_str

def format_gmt_offset(gmt_str):
    if isinstance(gmt_str, str) and len(gmt_str) > 5: return gmt_str[:6] if gmt_str.startswith('-') else gmt_str[:5]
    return gmt_str

def get_country_code(country_name):
    if not country_name: return None
    try:
        return pycountry.countries.search_fuzzy(country_name)[0].alpha_3
    except LookupError:
        # This warning is suppressed for cleaner bulk processing logs
        # log(f"Could not find a country code for '{country_name}'", 'WARNING', indent=2)
        return None

# --- API Helper ---
def get_api_data_bulk(endpoint, params=None, max_retries=5):
    """Fetches data from an API endpoint with a clean progress message."""
    retries = 0
    
    # Progress indicator start
    sys.stdout.write(f"{config.Style.CYAN}  ▸ Fetching {endpoint:<10}... {config.Style.RESET}")
    sys.stdout.flush()

    while retries <= max_retries:
        try:
            response = requests.get(f"{config.API_BASE_URL}/{endpoint}", params=params, timeout=30)
            response.raise_for_status()
            
            # Progress indicator success
            sys.stdout.write(f"\r{config.Style.GREEN}  ▸ Fetched {endpoint:<10} successfully. ({len(response.json())} records){config.Style.RESET}\n")
            sys.stdout.flush()
            return response.json()
            
        except requests.exceptions.Timeout:
            if retries < max_retries:
                time.sleep(2**(retries)); retries += 1
            else:
                sys.stdout.write(f"\r{config.Style.RED}  ▸ Fetching {endpoint:<10} FAILED (Timeout){config.Style.RESET}\n")
                log("Request timed out. Max retries reached.", 'ERROR', indent=2, data={'endpoint': endpoint})
                return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [429, 503] and retries < max_retries:
                time.sleep(2**(retries)); retries += 1
            else:
                sys.stdout.write(f"\r{config.Style.RED}  ▸ Fetching {endpoint:<10} FAILED (HTTP Error){config.Style.RESET}\n")
                log(f"HTTP Error ({e.response.status_code}). Max retries reached.", 'ERROR', indent=2, data={'endpoint': endpoint})
                return None
        except requests.exceptions.RequestException as e:
            sys.stdout.write(f"\r{config.Style.RED}  ▸ Fetching {endpoint:<10} FAILED (Request Error){config.Style.RESET}\n")
            log("An unexpected request error occurred.", 'ERROR', indent=2, data={'endpoint': endpoint, 'error': str(e)})
            return None
    return None

# --- Data Population ---
def populate_database():
    """Fetches API data in bulk and populates the database efficiently."""
    log("Populating database from OpenF1 API", 'HEADING')
    
    # --- Step 1: Bulk Fetch All Data ---
    log("Starting bulk data fetch from API", 'SUBHEADING')
    meetings = get_api_data_bulk('meetings', {'year': config.YEAR})
    if not meetings:
        log("Halting process: could not fetch meetings data.", 'ERROR')
        return

    min_meeting_key = min(m['meeting_key'] for m in meetings)
    
    sessions = get_api_data_bulk('sessions', {'meeting_key>': min_meeting_key})
    drivers = get_api_data_bulk('drivers', {'meeting_key>': min_meeting_key})
    weather = get_api_data_bulk('weather', {'meeting_key>': min_meeting_key})

    if not all([sessions, drivers, weather]):
        log("Halting process: one or more subsequent API calls failed.", 'ERROR')
        return

    # --- Step 2: Process and Insert Data ---
    log("Processing and inserting API data into the database", 'HEADING')
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    # Create a lookup map for session_key -> meeting_key
    session_to_meeting_map = {s['session_key']: s['meeting_key'] for s in sessions}

    # Process Countries, Circuits, and Meetings
    unique_countries = {m['country_name']: get_country_code(m['country_name']) for m in meetings if m.get('country_name')}
    for name, code in unique_countries.items():
        if name and code: cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, name))
    
    total_meetings = len(meetings)
    for i, meeting in enumerate(meetings):
        show_progress_bar(i + 1, total_meetings, prefix='Meetings & Circuits: ', length=40)
        cursor.execute("INSERT OR IGNORE INTO circuit (circuit_key, circuit_name, location, gmt_offset, country_fk) VALUES (?, ?, ?, ?, ?)",
            (meeting.get('circuit_key'), meeting.get('circuit_short_name'), meeting.get('location'), 
             format_gmt_offset(meeting.get('gmt_offset')), unique_countries.get(meeting.get('country_name'))))
        cursor.execute("INSERT OR IGNORE INTO meeting (meeting_key, meeting_name, meeting_official_name, date_start, circuit_fk) VALUES (?, ?, ?, ?, ?)",
            (meeting.get('meeting_key'), meeting.get('meeting_name'), meeting.get('meeting_official_name'),
             format_int_date(meeting.get('date_start')), meeting.get('circuit_key')))
    conn.commit()

    # Process Sessions
    total_sessions = len(sessions)
    for i, session in enumerate(sessions):
        show_progress_bar(i + 1, total_sessions, prefix='Sessions:            ', length=40)
        cursor.execute("INSERT OR IGNORE INTO session (session_key, session_name, session_type, date_start, date_end, meeting_fk) VALUES (?, ?, ?, ?, ?, ?)",
            (session.get('session_key'), session.get('session_name'), session.get('session_type'),
             format_int_date(session.get('date_start')), format_int_date(session.get('date_end')), session.get('meeting_key')))
    conn.commit()

    # Process Drivers and Teams
    unique_teams_by_name = {}
    latest_driver_data = {}
    
    # Also collect country data from drivers
    for driver in drivers:
        if (team_name := driver.get('team_name')): unique_teams_by_name[team_name] = True
        if (country_name := driver.get('country_name')): unique_countries[country_name] = driver.get('country_code')
        if not (code := driver.get('name_acronym')): continue
        
        session_date = driver.get('date', '1970-01-01T00:00:00')
        if code not in latest_driver_data or session_date > latest_driver_data[code]['session_date']:
            latest_driver_data[code] = {'data': driver, 'session_date': session_date}

    for name, code in unique_countries.items():
        if name and code: cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, name))

    team_name_to_id = {}
    for name in unique_teams_by_name:
        cursor.execute("INSERT OR IGNORE INTO team (team_name) VALUES (?)", (name,))
        if res := cursor.execute("SELECT team_id FROM team WHERE team_name = ?", (name,)).fetchone():
            team_name_to_id[name] = res[0]

    for i, (code, rec) in enumerate(latest_driver_data.items()):
        show_progress_bar(i + 1, len(latest_driver_data), prefix='Unique Drivers:      ', length=40)
        info = rec['data']
        cursor.execute("INSERT OR REPLACE INTO driver (driver_code, driver_name, driver_number, country_fk) VALUES (?, ?, ?, ?)",
            (code, info.get('full_name'), info.get('driver_number'), info.get('country_code')))
    conn.commit()

    # Process Junction Tables (Meeting/Session Drivers)
    processed_meeting_drivers = set()
    total_drivers = len(drivers)
    for i, driver in enumerate(drivers):
        show_progress_bar(i + 1, total_drivers, prefix='Driver Links:        ', length=40)
        code = driver.get('name_acronym')
        session_key = driver.get('session_key')
        meeting_key = session_to_meeting_map.get(session_key)
        team_id = team_name_to_id.get(driver.get('team_name'))

        if not all([code, session_key, meeting_key, team_id]): continue
        
        if (meeting_key, code) not in processed_meeting_drivers:
            cursor.execute("INSERT OR IGNORE INTO meeting_driver (meeting_fk, driver_fk, team_fk, driver_number) VALUES (?, ?, ?, ?)",
                (meeting_key, code, team_id, driver.get('driver_number')))
            processed_meeting_drivers.add((meeting_key, code))
            
        cursor.execute("INSERT OR IGNORE INTO session_driver (session_fk, driver_fk) VALUES (?, ?)", (session_key, code))
    conn.commit()

    # Process Weather
    total_weather = len(weather)
    for i, w in enumerate(weather):
        show_progress_bar(i + 1, total_weather, prefix='Weather Data:        ', length=40)
        cursor.execute("INSERT INTO weather (air_temperature, track_temperature, humidity, air_pressure, wind_direction, wind_speed, is_raining, date, session_fk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (w.get('air_temperature'), w.get('track_temperature'), w.get('humidity'), w.get('pressure'),
             w.get('wind_direction'), w.get('wind_speed'), w.get('rainfall'), 
             format_real_date(w.get('date')), w.get('session_key')))
    conn.commit()

    conn.close()
    log("API data population complete.", 'SUCCESS')
