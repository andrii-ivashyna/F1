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
    """Formats weather date string to ISO-MM-DDTHH:MM:SS.sss"""
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
        return None

# --- API Helper ---
def get_api_data_bulk(endpoint, params=None, max_tries=5):
    """Fetches data from an API endpoint with a clean progress message."""
    tries = 1
    start_time = time.time()
    
    while tries <= max_tries:
        try:
            # Show progress during the request
            show_progress_bar(tries, max_tries, prefix_text=f'API | {endpoint.capitalize()} | {tries}', start_time=start_time)
            
            response = requests.get(f"{config.API_BASE_URL}/{endpoint}", params=params, timeout=30)
            response.raise_for_status()
            
            # Show completion
            show_progress_bar(tries, tries, prefix_text=f'API | {endpoint.capitalize()} | {tries}', start_time=start_time)
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if tries < max_tries:
                tries += 1
                time.sleep(2**(tries))
            else:
                # Show failure
                show_progress_bar(0, max_tries, prefix_text=f'API | {endpoint.capitalize()} FAILED | {max_tries}', start_time=start_time)
                return None
    return None

# --- Data Population ---
def populate_database():
    """Fetches API data in bulk and populates the database efficiently."""
    log("Fetching data from OpenF1 API", 'SUBHEADING')
    
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

    log("Processing and inserting data in database", 'SUBHEADING')
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    # --- Process Meetings, Circuits & Countries ---
    unique_countries = {m['country_name']: get_country_code(m['country_name']) for m in meetings if m.get('country_name')}
    
    # Insert meetings
    start_time_meetings = time.time()
    for i, meeting in enumerate(meetings):
        show_progress_bar(i + 1, len(meetings), prefix_text=f'DB | Meeting | {len(meetings)}', start_time=start_time_meetings)
        cursor.execute("INSERT OR IGNORE INTO meeting (meeting_key, meeting_name, meeting_official_name, date_start, circuit_fk) VALUES (?, ?, ?, ?, ?)",
            (meeting.get('meeting_key'), meeting.get('meeting_name'), meeting.get('meeting_official_name'),
             format_int_date(meeting.get('date_start')), meeting.get('circuit_key')))
    conn.commit()

    # Insert circuits
    start_time_circuits = time.time()
    for i, meeting in enumerate(meetings):
        show_progress_bar(i + 1, len(meetings), prefix_text=f'DB | Circuit | {len(meetings)}', start_time=start_time_circuits)
        cursor.execute("INSERT OR IGNORE INTO circuit (circuit_key, circuit_name, location, gmt_offset, country_fk) VALUES (?, ?, ?, ?, ?)",
            (meeting.get('circuit_key'), meeting.get('circuit_short_name'), meeting.get('location'), 
             format_gmt_offset(meeting.get('gmt_offset')), unique_countries.get(meeting.get('country_name'))))
    conn.commit()

    # Insert countries
    country_data = [(code, name) for name, code in unique_countries.items() if name and code]
    start_time_countries = time.time()
    for i, (code, name) in enumerate(country_data):
        show_progress_bar(i + 1, len(country_data), prefix_text=f'DB | Country | {len(country_data)}', start_time=start_time_countries)
        cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, name))
    conn.commit()

    # --- Process Sessions ---
    start_time_sessions = time.time()
    for i, session in enumerate(sessions):
        show_progress_bar(i + 1, len(sessions), prefix_text=f'DB | Session | {len(sessions)}', start_time=start_time_sessions)
        cursor.execute("INSERT OR IGNORE INTO session (session_key, session_name, session_type, date_start, date_end, meeting_fk) VALUES (?, ?, ?, ?, ?, ?)",
            (session.get('session_key'), session.get('session_name'), session.get('session_type'),
             format_int_date(session.get('date_start')), format_int_date(session.get('date_end')), session.get('meeting_key')))
    conn.commit()

    # --- Process Drivers & Teams ---
    session_to_meeting_map = {s['session_key']: s['meeting_key'] for s in sessions}
    latest_driver_records = {}
    driver_meeting_keys = {}

    for record in drivers:
        code = record.get('name_acronym')
        meeting_key = session_to_meeting_map.get(record.get('session_key'))
        if not all([code, meeting_key]):
            continue
        
        # If driver is new or this record is from a later meeting, update it
        if code not in latest_driver_records or meeting_key > driver_meeting_keys.get(code, -1):
            latest_driver_records[code] = record
            driver_meeting_keys[code] = meeting_key
    
    final_drivers = list(latest_driver_records.values())

    unique_teams = sorted(list(set(d.get('team_name') for d in final_drivers if d.get('team_name'))))
    driver_countries = {d.get('country_name'): d.get('country_code') for d in final_drivers if d.get('country_name')}
    unique_countries.update(driver_countries) # Update unique_countries with driver-specific countries if any

    # Insert drivers
    start_time_drivers = time.time()
    for i, driver in enumerate(final_drivers):
        show_progress_bar(i + 1, len(final_drivers), prefix_text=f'DB | Driver | {len(final_drivers)}', start_time=start_time_drivers)
        code = driver.get('name_acronym')
        cursor.execute("INSERT OR REPLACE INTO driver (driver_code, driver_name, driver_number, country_fk) VALUES (?, ?, ?, ?)",
                       (code, driver.get('full_name'), driver.get('driver_number'), driver.get('country_code')))
    conn.commit()

    # Insert teams
    start_time_teams = time.time()
    team_data = [(name,) for name in unique_teams]
    for i, name_tuple in enumerate(team_data):
        show_progress_bar(i + 1, len(team_data), prefix_text=f'DB | Team | {len(team_data)}', start_time=start_time_teams)
        cursor.execute("INSERT OR IGNORE INTO team (team_name) VALUES (?)", name_tuple)
    conn.commit()

    team_name_to_id = {
        row[1]: row[0] for row in cursor.execute(f"SELECT team_id, team_name FROM team WHERE team_name IN ({','.join('?'*len(unique_teams))})", list(unique_teams))
    }

    # --- Process Join Tables ---
    # Insert meeting_driver links
    meeting_driver_data = []
    for i, driver in enumerate(final_drivers):
        code = driver.get('name_acronym')
        team_id = team_name_to_id.get(driver.get('team_name'))
        meeting_key = driver_meeting_keys.get(code)
        if all([meeting_key, code, team_id]):
            meeting_driver_data.append((meeting_key, code, team_id, driver.get('driver_number')))
    
    start_time_meeting_driver = time.time()
    for i, entry in enumerate(meeting_driver_data):
        show_progress_bar(i + 1, len(meeting_driver_data), prefix_text=f'DB | Meeting-Driver | {len(meeting_driver_data)}', start_time=start_time_meeting_driver)
        cursor.execute("INSERT OR IGNORE INTO meeting_driver (meeting_fk, driver_fk, team_fk, driver_number) VALUES (?, ?, ?, ?)", entry)
    conn.commit()

    # Insert session_driver links
    valid_driver_codes = set(latest_driver_records.keys())
    session_driver_pairs = {(d.get('session_key'), d.get('name_acronym'))
                            for d in drivers if d.get('name_acronym') in valid_driver_codes and d.get('session_key')}
    
    session_driver_data = list(session_driver_pairs)
    start_time_session_driver = time.time()
    for i, pair in enumerate(session_driver_data):
        show_progress_bar(i + 1, len(session_driver_data), prefix_text=f'DB | Session-Driver | {len(session_driver_data)}', start_time=start_time_session_driver)
        cursor.execute("INSERT OR IGNORE INTO session_driver (session_fk, driver_fk) VALUES (?, ?)", pair)
    conn.commit()

    # --- Process Weather Data ---
    weather_data = [
        (w.get('air_temperature'), w.get('track_temperature'), w.get('humidity'), w.get('pressure'),
         w.get('wind_direction'), w.get('wind_speed'), w.get('rainfall'), 
         format_real_date(w.get('date')), w.get('session_key'))
        for w in weather
    ]
    
    start_time_weather = time.time()
    for i, entry in enumerate(weather_data):
        show_progress_bar(i + 1, len(weather_data), prefix_text=f'DB | Weather | {len(weather_data)}', start_time=start_time_weather)
        cursor.execute("INSERT INTO weather (air_temperature, track_temperature, humidity, air_pressure, wind_direction, wind_speed, is_raining, date, session_fk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", entry)
    conn.commit()

    conn.close()
