# fetch_api.py

import sqlite3
import time
import requests
import config
import pycountry
from datetime import datetime

# --- Logging and Formatting Helpers ---

def log(message):
    """Prints a message with a timestamp."""
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def format_api_date(date_str):
    """Removes timezone offset from an ISO 8601 date string."""
    if isinstance(date_str, str) and len(date_str) > 19:
        return date_str[:19]
    return date_str

def format_gmt_offset(gmt_str):
    """Formats GMT offset from HH:MM:SS to HH:MM."""
    if isinstance(gmt_str, str) and len(gmt_str) > 5:
        return gmt_str[:6] if gmt_str.startswith('-') else gmt_str[:5]
    return gmt_str

def get_country_code(country_name):
    """Finds the 3-letter ISO code for a given country name."""
    if not country_name:
        return None
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_3
    except LookupError:
        log(f"Warning: Could not find a country code for '{country_name}'.")
        return None

# --- API Helper ---
def get_api_data(endpoint, params=None, max_retries=5):
    """Fetches data from the OpenF1 API with timeout and retry logic."""
    retries = 0
    while retries <= max_retries:
        try:
            response = requests.get(f"{config.API_BASE_URL}/{endpoint}", params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            if retries < max_retries:
                log(f"Request to {endpoint} timed out. Retrying in {2**(retries+1)} seconds... (Attempt {retries + 1}/{max_retries})")
                time.sleep(2**(retries)); retries += 1
            else:
                log(f"Error fetching data from {endpoint}: Request timed out. Max retries reached. Skipping.")
                return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and retries < max_retries:
                log(f"Too Many Requests for {endpoint}. Retrying in {2**(retries+1)} seconds... (Attempt {retries + 1}/{max_retries})")
                time.sleep(2**(retries)); retries += 1
            else:
                log(f"Error fetching data from {endpoint}: {e}. Max retries reached. Skipping.")
                return None
        except requests.exceptions.RequestException as e:
            log(f"An unexpected error occurred while fetching data from {endpoint}: {e}")
            return None
    return None

# --- Data Population ---
def populate_database():
    """Fetches API data and populates the database."""
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    unique_countries = {}
    unique_teams_by_name = {}
    latest_driver_data = {}

    all_meetings, all_sessions = [], []
    for year in config.YEARS:
        log(f"--- Fetching meeting data for {year} ---")
        meetings_data = get_api_data('meetings', {'year': year})
        if meetings_data:
            all_meetings.extend(meetings_data)
            for meeting in meetings_data:
                log(f"  Fetching sessions for meeting: {meeting.get('meeting_name')}")
                sessions = get_api_data('sessions', {'meeting_key': meeting.get('meeting_key')})
                if sessions: all_sessions.extend(sessions)
                time.sleep(0.5)
        time.sleep(1)

    log("--- Processing all meetings and circuits ---")
    for meeting in all_meetings:
        if meeting.get('country_name'):
            unique_countries[meeting['country_name']] = get_country_code(meeting['country_name'])
        
        cursor.execute("""
            INSERT OR IGNORE INTO circuit (circuit_key, circuit_name, location, gmt_offset, country_fk)
            VALUES (?, ?, ?, ?, ?)
        """, (
            meeting.get('circuit_key'), meeting.get('circuit_short_name'),
            meeting.get('location'), format_gmt_offset(meeting.get('gmt_offset')),
            unique_countries.get(meeting.get('country_name'))
        ))
        cursor.execute("""
            INSERT OR IGNORE INTO meeting (meeting_key, meeting_name, meeting_official_name, date_start, circuit_fk)
            VALUES (?, ?, ?, ?, ?)
        """, (
            meeting.get('meeting_key'), meeting.get('meeting_name'),
            meeting.get('meeting_official_name'), format_api_date(meeting.get('date_start')),
            meeting.get('circuit_key')
        ))
    conn.commit()

    log("--- Fetching and collecting latest driver and team data from all sessions ---")
    for session in all_sessions:
        log(f"  Processing session: {session.get('session_name')} (Key: {session.get('session_key')})")
        session_date = format_api_date(session.get('date_start'))
        
        cursor.execute("""
            INSERT OR IGNORE INTO session (session_key, session_name, session_type, date_start, date_end, meeting_fk)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            session.get('session_key'), session.get('session_name'), session.get('session_type'),
            session_date, format_api_date(session.get('date_end')), session.get('meeting_key')
        ))

        drivers_in_session = get_api_data('drivers', {'session_key': session.get('session_key')})
        if drivers_in_session:
            for driver_data in drivers_in_session:
                driver_code = driver_data.get('name_acronym')
                if not driver_code: continue

                if driver_code not in latest_driver_data or session_date > latest_driver_data[driver_code]['session_date']:
                    latest_driver_data[driver_code] = {
                        'data': driver_data,
                        'session_date': session_date
                    }

                if driver_data.get('team_name'):
                    unique_teams_by_name[driver_data['team_name']] = driver_data
                if driver_data.get('country_name'):
                     unique_countries[driver_data['country_name']] = get_country_code(driver_data['country_name'])
        time.sleep(0.5)

    log("Inserting unique countries and teams into database...")
    for name, code in unique_countries.items():
        if name and code:
            cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, name))

    team_name_to_id = {}
    for team_name in unique_teams_by_name.keys():
        cursor.execute("INSERT OR IGNORE INTO team (team_name) VALUES (?)", (team_name,))
        res = cursor.execute("SELECT team_id FROM team WHERE team_name = ?", (team_name,)).fetchone()
        if res: team_name_to_id[team_name] = res[0]

    log("Inserting latest driver data into database...")
    for driver_code, record in latest_driver_data.items():
        driver_info = record['data']
        country_code = unique_countries.get(driver_info.get('country_name'))
        
        cursor.execute("""
            INSERT OR REPLACE INTO driver (driver_code, driver_name, driver_number, country_fk)
            VALUES (?, ?, ?, ?)
        """, (
            driver_code,
            driver_info.get('full_name'),
            driver_info.get('driver_number'),
            country_code
        ))
    conn.commit()
    
    log("Inserting relational data (meeting_driver, session_driver)...")
    processed_meeting_drivers = set()
    # CORRECTED LOGIC: Iterate through each session again to get the right context for joins.
    for session in all_sessions:
        drivers_in_session = get_api_data('drivers', {'session_key': session.get('session_key')})
        if drivers_in_session:
            for driver_data in drivers_in_session:
                driver_code = driver_data.get('name_acronym')
                team_id = team_name_to_id.get(driver_data.get('team_name'))
                meeting_key = session.get('meeting_key')
                
                if not all([driver_code, team_id, meeting_key]):
                    continue

                # Insert into meeting_driver, ensuring one entry per driver per meeting
                if (meeting_key, driver_code) not in processed_meeting_drivers:
                    cursor.execute("""
                        INSERT OR IGNORE INTO meeting_driver (meeting_fk, driver_fk, team_fk, driver_number)
                        VALUES (?, ?, ?, ?)
                    """, (meeting_key, driver_code, team_id, driver_data.get('driver_number')))
                    processed_meeting_drivers.add((meeting_key, driver_code))
                
                # Insert into session_driver
                cursor.execute("""
                    INSERT OR IGNORE INTO session_driver (session_fk, driver_fk)
                    VALUES (?, ?)
                """, (session.get('session_key'), driver_code))
        time.sleep(0.5) # Be respectful to the API

    conn.commit()
    conn.close()
    log("API data population complete.")
