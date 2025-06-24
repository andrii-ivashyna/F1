# fetch_api.py

import os
import sqlite3
import time
import requests
import config
from datetime import datetime

# --- Logging and Formatting Helpers ---

def log(message):
    """Prints a message with a timestamp."""
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

def format_api_date(date_str):
    """Removes timezone offset from an ISO 8601 date string to save space."""
    if isinstance(date_str, str) and len(date_str) > 19:
        return date_str[:19]
    return date_str

# --- Database Setup ---

def create_database():
    """Creates the database and tables using the schema from config.py."""
    log("Initializing database...")
    if not os.path.exists('data'):
        os.makedirs('data')

    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    log("Dropping existing tables...")
    for table_name in config.TABLES_TO_DROP:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    log("Creating tables with the new structure...")
    for create_statement in config.CREATE_TABLE_STATEMENTS:
        cursor.execute(create_statement)

    conn.commit()
    conn.close()
    log("Database and tables created successfully.")


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
                time.sleep(2**(retries))
                retries += 1
            else:
                log(f"Error fetching data from {endpoint}: Request timed out. Max retries reached. Skipping.")
                return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and retries < max_retries:
                log(f"Too Many Requests for {endpoint}. Retrying in {2**(retries+1)} seconds... (Attempt {retries + 1}/{max_retries})")
                time.sleep(2**(retries))
                retries += 1
            else:
                log(f"Error fetching data from {endpoint}: {e}. Max retries reached. Skipping.")
                return None
        except requests.exceptions.RequestException as e:
            log(f"An unexpected error occurred while fetching data from {endpoint}: {e}")
            return None
    return None

# --- Data Population ---

def populate_database():
    """Fetches API data and populates the database using the new schema."""
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    unique_countries = {}
    unique_drivers = {}
    unique_teams_by_name = {}
    all_meetings = []
    all_sessions = []

    for year in config.YEARS:
        log(f"--- Fetching meeting data for {year} ---")
        meetings_data = get_api_data('meetings', {'year': year})
        if meetings_data:
            all_meetings.extend(meetings_data)
            for meeting in meetings_data:
                log(f"  Fetching sessions for meeting: {meeting.get('meeting_name')}")
                sessions = get_api_data('sessions', {'meeting_key': meeting.get('meeting_key')})
                if sessions:
                    all_sessions.extend(sessions)
                time.sleep(0.5)
        time.sleep(1)

    log("--- Processing all meetings and sessions ---")
    for meeting in all_meetings:
        country_code = meeting.get('country_code')
        if country_code and meeting.get('country_name'):
            unique_countries[country_code] = meeting.get('country_name')

        cursor.execute("""
            INSERT OR IGNORE INTO circuit (circuit_key, circuit_name, circuit_official_name, location, type, direction, length_km, turns, gmt_offset, country_fk)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            meeting.get('circuit_key'), meeting.get('circuit_short_name'),
            meeting.get('circuit_official_name'), meeting.get('location'),
            None, None, None, meeting.get('number_of_turns'),
            meeting.get('gmt_offset'), country_code
        ))

        cursor.execute("""
            INSERT OR IGNORE INTO meeting (meeting_key, meeting_name, meeting_official_name, date_start, circuit_fk)
            VALUES (?, ?, ?, ?, ?)
        """, (
            meeting.get('meeting_key'), meeting.get('meeting_name'),
            meeting.get('meeting_official_name'),
            format_api_date(meeting.get('date_start')), # Format date
            meeting.get('circuit_key')
        ))
    conn.commit()

    log("--- Fetching and processing driver and team data per session ---")
    for session in all_sessions:
        log(f"  Processing session: {session.get('session_name')} (Key: {session.get('session_key')})")

        cursor.execute("""
            INSERT OR IGNORE INTO session (session_key, session_name, session_type, date_start, date_end, meeting_fk)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            session.get('session_key'), session.get('session_name'),
            session.get('session_type'),
            format_api_date(session.get('date_start')), # Format date
            format_api_date(session.get('date_end')),   # Format date
            session.get('meeting_key')
        ))

        drivers_in_session = get_api_data('drivers', {'session_key': session.get('session_key')})
        if drivers_in_session:
            for driver_data in drivers_in_session:
                if driver_data.get('full_name'):
                    unique_drivers[driver_data['full_name']] = driver_data
                if driver_data.get('team_name'):
                    unique_teams_by_name[driver_data['team_name']] = {'team_name': driver_data['team_name']}
                if driver_data.get('country_code'):
                    unique_countries[driver_data['country_code']] = driver_data.get('country_name')
        time.sleep(0.5)

    log("Inserting unique countries, teams, and drivers into database...")
    for code, name in unique_countries.items():
        cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, name))

    team_name_to_id = {}
    for team_name, team_info in unique_teams_by_name.items():
        cursor.execute("""
            INSERT OR IGNORE INTO team (team_name, team_official_name, power_unit, chassis, country_fk)
            VALUES (?, ?, ?, ?, ?)
        """, (team_info.get('team_name'), None, None, None, None))
        res = cursor.execute("SELECT team_id FROM team WHERE team_name = ?", (team_info.get('team_name'),)).fetchone()
        if res:
            team_name_to_id[team_name] = res[0]

    driver_name_to_id = {}
    for full_name, driver_info in unique_drivers.items():
        cursor.execute("""
            INSERT OR IGNORE INTO driver (driver_acronym, driver_name, country_fk)
            VALUES (?, ?, ?)
        """, (driver_info.get('name_acronym'), full_name, driver_info.get('country_code')))
        res = cursor.execute("SELECT driver_id FROM driver WHERE driver_name = ?", (full_name,)).fetchone()
        if res:
            driver_name_to_id[full_name] = res[0]
    conn.commit()

    log("Inserting relational data (meeting_driver, session_driver)...")
    for session in all_sessions:
        drivers_in_session = get_api_data('drivers', {'session_key': session.get('session_key')})
        if drivers_in_session:
            for driver_data in drivers_in_session:
                driver_id = driver_name_to_id.get(driver_data.get('full_name'))
                team_id = team_name_to_id.get(driver_data.get('team_name'))

                if driver_id:
                    cursor.execute("""
                        INSERT OR IGNORE INTO meeting_driver (meeting_fk, driver_fk, team_fk, driver_number)
                        VALUES (?, ?, ?, ?)
                    """, (session.get('meeting_key'), driver_id, team_id, driver_data.get('driver_number')))

                    cursor.execute("""
                        INSERT OR IGNORE INTO session_driver (session_fk, driver_fk)
                        VALUES (?, ?)
                    """, (session.get('session_key'), driver_id))
        time.sleep(0.5)

    conn.commit()
    conn.close()
    log("Database population complete.")
