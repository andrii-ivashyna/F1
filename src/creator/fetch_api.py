# fetch_api.py

import sqlite3
import time
import requests
import pycountry
import config
import sys
from config import log

# --- Formatting Helpers ---
def format_api_date(date_str):
    if isinstance(date_str, str) and len(date_str) > 19: return date_str[:19]
    return date_str

def format_gmt_offset(gmt_str):
    if isinstance(gmt_str, str) and len(gmt_str) > 5: return gmt_str[:6] if gmt_str.startswith('-') else gmt_str[:5]
    return gmt_str

def get_country_code(country_name):
    if not country_name: return None
    try:
        return pycountry.countries.search_fuzzy(country_name)[0].alpha_3
    except LookupError:
        log(f"Could not find a country code for '{country_name}'", 'WARNING', indent=2)
        return None

# --- API Helper ---
def get_api_data(endpoint, params=None, max_retries=5):
    retries = 0
    while retries <= max_retries:
        try:
            response = requests.get(f"{config.API_BASE_URL}/{endpoint}", params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            if retries < max_retries:
                log(f"Request to {endpoint} timed out. Retrying...", 'WARNING', indent=3)
                time.sleep(2**(retries)); retries += 1
            else:
                log("Request timed out. Max retries reached.", 'ERROR', indent=3, data={'endpoint': endpoint})
                return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [429, 503] and retries < max_retries:
                log(f"API rate limited ({e.response.status_code}). Retrying...", 'WARNING', indent=3)
                time.sleep(2**(retries)); retries += 1
            else:
                log("HTTP Error. Max retries reached.", 'ERROR', indent=3, data={'endpoint': endpoint, 'error': str(e)})
                return None
        except requests.exceptions.RequestException as e:
            log("An unexpected request error occurred.", 'ERROR', indent=3, data={'endpoint': endpoint, 'error': str(e)})
            return None
    return None

# --- Data Population ---
def populate_database():
    """Fetches API data and populates the database."""
    log("Populating database from OpenF1 API", 'HEADING')
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    unique_countries, unique_teams_by_name, latest_driver_data = {}, {}, {}
    all_meetings, all_sessions = [], []

    for year in config.YEARS:
        log(f"Fetching meeting and session data for {year}", 'INFO')
        meetings_data = get_api_data('meetings', {'year': year})
        if meetings_data:
            all_meetings.extend(meetings_data)
            total_meetings = len(meetings_data)
            for i, meeting in enumerate(meetings_data):
                progress = f"[{i+1}/{total_meetings}]"
                sys.stdout.write(f"\r{config.Style.CYAN}  ▸ Fetching sessions for: {meeting.get('meeting_name', 'N/A'):<30} {progress}{config.Style.RESET}")
                sys.stdout.flush()
                
                sessions = get_api_data('sessions', {'meeting_key': meeting.get('meeting_key')})
                if sessions: all_sessions.extend(sessions)
                time.sleep(0.1)
        print() # Newline after progress bar
        time.sleep(0.5)

    log("Processing and inserting API data", 'INFO')
    for meeting in all_meetings:
        if meeting.get('country_name'): unique_countries[meeting['country_name']] = get_country_code(meeting['country_name'])
        cursor.execute("INSERT OR IGNORE INTO circuit (circuit_key, circuit_name, location, gmt_offset, country_fk) VALUES (?, ?, ?, ?, ?)",
            (meeting.get('circuit_key'), meeting.get('circuit_short_name'), meeting.get('location'), 
             format_gmt_offset(meeting.get('gmt_offset')), unique_countries.get(meeting.get('country_name'))))
        cursor.execute("INSERT OR IGNORE INTO meeting (meeting_key, meeting_name, meeting_official_name, date_start, circuit_fk) VALUES (?, ?, ?, ?, ?)",
            (meeting.get('meeting_key'), meeting.get('meeting_name'), meeting.get('meeting_official_name'),
             format_api_date(meeting.get('date_start')), meeting.get('circuit_key')))
    conn.commit()

    total_sessions = len(all_sessions)
    for i, session in enumerate(all_sessions):
        progress = f"[{i+1}/{total_sessions}]"
        sys.stdout.write(f"\r{config.Style.CYAN}  ▸ Collecting driver/team data from session: {session.get('session_name', 'N/A'):<20} {progress}{config.Style.RESET}")
        sys.stdout.flush()
        
        session_date = format_api_date(session.get('date_start'))
        cursor.execute("INSERT OR IGNORE INTO session (session_key, session_name, session_type, date_start, date_end, meeting_fk) VALUES (?, ?, ?, ?, ?, ?)",
            (session.get('session_key'), session.get('session_name'), session.get('session_type'),
             session_date, format_api_date(session.get('date_end')), session.get('meeting_key')))

        drivers_in_session = get_api_data('drivers', {'session_key': session.get('session_key')})
        if drivers_in_session:
            for driver in drivers_in_session:
                if not (code := driver.get('name_acronym')): continue
                if code not in latest_driver_data or session_date > latest_driver_data[code]['session_date']:
                    latest_driver_data[code] = {'data': driver, 'session_date': session_date}
                if (team_name := driver.get('team_name')): unique_teams_by_name[team_name] = driver
                if (country_code := driver.get('country_code')): unique_countries[driver['country_name']] = country_code
        time.sleep(0.1)
    print() # Newline after progress bar

    log("Finalizing database entries", 'INFO')
    for name, code in unique_countries.items():
        if name and code: cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, name))

    team_name_to_id = {}
    for name in unique_teams_by_name:
        cursor.execute("INSERT OR IGNORE INTO team (team_name) VALUES (?)", (name,))
        if res := cursor.execute("SELECT team_id FROM team WHERE team_name = ?", (name,)).fetchone():
            team_name_to_id[name] = res[0]
    log(f"Processed {len(team_name_to_id)} unique teams.", 'SUCCESS', indent=1)

    for code, rec in latest_driver_data.items():
        info = rec['data']
        cursor.execute("INSERT OR REPLACE INTO driver (driver_code, driver_name, driver_number, country_fk) VALUES (?, ?, ?, ?)",
            (code, info.get('full_name'), info.get('driver_number'), unique_countries.get(info.get('country_name'))))
    log(f"Processed {len(latest_driver_data)} unique drivers.", 'SUCCESS', indent=1)
    conn.commit()
    
    processed_meeting_drivers = set()
    for session in all_sessions:
        if drivers := get_api_data('drivers', {'session_key': session.get('session_key')}):
            for driver in drivers:
                code = driver.get('name_acronym')
                team_id = team_name_to_id.get(driver.get('team_name'))
                meeting_key = session.get('meeting_key')
                if not all([code, team_id, meeting_key]): continue
                if (meeting_key, code) not in processed_meeting_drivers:
                    cursor.execute("INSERT OR IGNORE INTO meeting_driver (meeting_fk, driver_fk, team_fk, driver_number) VALUES (?, ?, ?, ?)",
                        (meeting_key, code, team_id, driver.get('driver_number')))
                    processed_meeting_drivers.add((meeting_key, code))
                cursor.execute("INSERT OR IGNORE INTO session_driver (session_fk, driver_fk) VALUES (?, ?)", (session.get('session_key'), code))
        time.sleep(0.1)

    conn.commit()
    conn.close()
    log("API data population complete.", 'SUCCESS')
