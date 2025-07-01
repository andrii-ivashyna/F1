# manager_api.py
"""
API client for fetching Formula 1 data from OpenF1 API and populating the database.
"""

import sqlite3
import time
from datetime import datetime
import requests
import config
from config import log, show_progress_bar
from manager_db import format_timestamp, format_gmt_offset, get_country_code

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
    pits = get_api_data_bulk('pit', {'meeting_key>': min_meeting_key})
    stints = get_api_data_bulk('stints', {'meeting_key>': min_meeting_key})
    team_radio = get_api_data_bulk('team_radio', {'meeting_key>': min_meeting_key})
    race_control = get_api_data_bulk('race_control', {'meeting_key>': min_meeting_key})


    if not all([sessions, drivers, weather, pits, stints, team_radio, race_control]):
        log("Halting process: one or more subsequent API calls failed.", 'ERROR')
        return

    log("Processing and inserting data in database", 'SUBHEADING')
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    # --- Process Meetings, Circuits & Countries ---
    # Insert meetings
    start_time_meetings = time.time()
    for i, meeting in enumerate(meetings):
        show_progress_bar(i + 1, len(meetings), prefix_text=f'DB | Meeting | {len(meetings)}', start_time=start_time_meetings)
        cursor.execute("INSERT OR IGNORE INTO meeting (meeting_key, meeting_name, meeting_official_name, timestamp_utc, circuit_fk) VALUES (?, ?, ?, ?, ?)",
            (meeting.get('meeting_key'), meeting.get('meeting_name'), meeting.get('meeting_official_name'),
            format_timestamp(meeting.get('date_start'), 'int'), meeting.get('circuit_key')))
    conn.commit()

    # Insert countries
    unique_countries = {m['country_name']: get_country_code(m['country_name']) for m in meetings if m.get('country_name')}
    country_data = [(code, name) for name, code in unique_countries.items() if name and code]
    start_time_countries = time.time()
    for i, (code, name) in enumerate(country_data):
        show_progress_bar(i + 1, len(country_data), prefix_text=f'DB | Country | {len(country_data)}', start_time=start_time_countries)
        cursor.execute("INSERT OR IGNORE INTO country (country_code, country_name) VALUES (?, ?)", (code, name))
    conn.commit()

    # Insert circuits
    unique_circuits = {
        m['circuit_key']: {
            'circuit_key': m['circuit_key'],
            'circuit_short_name': m.get('circuit_short_name'),
            'location': m.get('location'),
            'gmt_offset': m.get('gmt_offset'),
            'country_name': m.get('country_name')
        }
        for m in meetings
        if (ck := m.get('circuit_key')) and ck not in set() and not set().add(ck)
    }

    start_time_circuits = time.time()
    circuit_data = list(unique_circuits.values())
    for i, circuit in enumerate(circuit_data):
        show_progress_bar(i + 1, len(circuit_data), prefix_text=f'DB | Circuit | {len(circuit_data)}', start_time=start_time_circuits)
        cursor.execute("INSERT OR IGNORE INTO circuit (circuit_key, circuit_name, location, gmt_offset, country_fk) VALUES (?, ?, ?, ?, ?)",
            (circuit['circuit_key'], circuit['circuit_short_name'], circuit['location'],
            format_gmt_offset(circuit['gmt_offset']), unique_countries.get(circuit['country_name'])))
    conn.commit()

    # --- Process Sessions ---
    start_time_sessions = time.time()
    for i, session in enumerate(sessions):
        show_progress_bar(i + 1, len(sessions), prefix_text=f'DB | Session | {len(sessions)}', start_time=start_time_sessions)

        duration_iso = None
        start_str = session.get('date_start')
        end_str = session.get('date_end')

        if start_str and end_str:
            try:
                start_dt = datetime.fromisoformat(start_str)
                end_dt = datetime.fromisoformat(end_str)
                delta = end_dt - start_dt

                total_seconds = delta.total_seconds()
                hours = int(total_seconds // 3600)
                minutes = int((total_seconds % 3600) // 60)
                seconds = int(total_seconds % 60)
                duration_iso = f"{hours:02}:{minutes:02}:{seconds:02}"
            except (ValueError, TypeError):
                duration_iso = None

        cursor.execute("INSERT OR IGNORE INTO session (session_key, session_name, session_type, duration_utc, timestamp_utc, meeting_fk) VALUES (?, ?, ?, ?, ?, ?)",
            (session.get('session_key'), session.get('session_name'), session.get('session_type'),
             duration_iso, format_timestamp(start_str, 'int'), session.get('meeting_key')))
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
    # Insert meeting_driver links - use ALL driver records, not just latest
    meeting_driver_data = []
    meeting_driver_seen = set()  # To avoid duplicates

    for driver in drivers:
        code = driver.get('name_acronym')
        session_key = driver.get('session_key')
        meeting_key = session_to_meeting_map.get(session_key)
        team_name = driver.get('team_name')
        team_id = team_name_to_id.get(team_name)
        driver_number = driver.get('driver_number')

        if all([meeting_key, code, team_id, driver_number]):
            # Create unique key to avoid duplicates
            unique_key = (meeting_key, code, team_id, driver_number)
            if unique_key not in meeting_driver_seen:
                meeting_driver_data.append(unique_key)
                meeting_driver_seen.add(unique_key)

    # Sort by meeting_key, then driver_code
    meeting_driver_data.sort(key=lambda x: (x[0], x[1]))

    start_time_meeting_driver = time.time()
    for i, entry in enumerate(meeting_driver_data):
        show_progress_bar(i + 1, len(meeting_driver_data), prefix_text=f'DB | Meeting-Driver | {len(meeting_driver_data)}', start_time=start_time_meeting_driver)
        cursor.execute("INSERT OR IGNORE INTO meeting_driver (meeting_fk, driver_fk, team_fk, driver_number) VALUES (?, ?, ?, ?)", entry)
    conn.commit()

    # Insert session_driver links - use ALL driver records with proper sorting
    session_driver_data = []
    session_driver_seen = set()  # To avoid duplicates

    for driver in drivers:
        code = driver.get('name_acronym')
        session_key = driver.get('session_key')

        if code and session_key:
            # Create unique key to avoid duplicates
            unique_key = (session_key, code)
            if unique_key not in session_driver_seen:
                session_driver_data.append(unique_key)
                session_driver_seen.add(unique_key)

    # Sort by session_key, then driver_code
    session_driver_data.sort(key=lambda x: (x[0], x[1]))

    start_time_session_driver = time.time()
    for i, pair in enumerate(session_driver_data):
        show_progress_bar(i + 1, len(session_driver_data), prefix_text=f'DB | Session-Driver | {len(session_driver_data)}', start_time=start_time_session_driver)
        cursor.execute("INSERT OR IGNORE INTO session_driver (session_fk, driver_fk) VALUES (?, ?)", pair)
    conn.commit()

    # --- Process Weather Data ---
    weather_data = [
        (w.get('air_temperature'), w.get('track_temperature'), w.get('humidity'), w.get('pressure'),
         w.get('wind_direction'), w.get('wind_speed'), w.get('rainfall'),
         format_timestamp(w.get('date'), 'real'), w.get('session_key'))
        for w in weather
    ]

    start_time_weather = time.time()
    for i, entry in enumerate(weather_data):
        show_progress_bar(i + 1, len(weather_data), prefix_text=f'DB | Weather | {len(weather_data)}', start_time=start_time_weather)
        cursor.execute("INSERT INTO weather (air_temp_C, track_temp_C, rel_humidity_pct, air_pressure_mbar, wind_direction_deg, wind_speed_mps, is_raining, timestamp_utc, session_fk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", entry)
    conn.commit()

    # --- Build helper map for session/driver lookups ---
    session_driver_number_to_code_map = {
        (d['session_key'], d['driver_number']): d['name_acronym']
        for d in drivers if all(k in d for k in ['session_key', 'driver_number', 'name_acronym'])
    }

    # --- Process Pit Data ---
    pit_data = []
    for p in pits:
        session_key = p.get('session_key')
        driver_number = p.get('driver_number')
        driver_code = session_driver_number_to_code_map.get((session_key, driver_number))
        if driver_code:
            pit_data.append((
                p.get('lap_number'),
                p.get('pit_duration'),
                session_key,
                driver_code
            ))

    start_time_pits = time.time()
    for i, entry in enumerate(pit_data):
        show_progress_bar(i + 1, len(pit_data), prefix_text=f'DB | Pit | {len(pit_data)}', start_time=start_time_pits)
        cursor.execute("INSERT INTO pit (lap_num, duration_s, session_fk, driver_fk) VALUES (?, ?, ?, ?)", entry)
    conn.commit()

    # --- Process Stint Data ---
    stint_data = []
    for s in stints:
        session_key = s.get('session_key')
        driver_number = s.get('driver_number')
        driver_code = session_driver_number_to_code_map.get((session_key, driver_number))
        if driver_code:
            stint_data.append((
                s.get('stint_number'),
                s.get('compound'),
                s.get('lap_start'),
                s.get('lap_end'),
                s.get('tyre_age_at_start'),
                session_key,
                driver_code
            ))

    start_time_stints = time.time()
    for i, entry in enumerate(stint_data):
        show_progress_bar(i + 1, len(stint_data), prefix_text=f'DB | Stint | {len(stint_data)}', start_time=start_time_stints)
        cursor.execute("INSERT INTO stint (stint_num, tyre_compound, lap_num_start, lap_num_end, tyre_age_laps, session_fk, driver_fk) VALUES (?, ?, ?, ?, ?, ?, ?)", entry)
    conn.commit()

    # --- Process Radio Data ---
    radio_data = []
    for r in team_radio:
        session_key = r.get('session_key')
        driver_number = r.get('driver_number')
        driver_code = session_driver_number_to_code_map.get((session_key, driver_number))
        if driver_code:
            radio_data.append((
                format_timestamp(r.get('date'), 'real'),
                session_key,
                driver_code,
                r.get('recording_url')
            ))

    start_time_radio = time.time()
    for i, entry in enumerate(radio_data):
        show_progress_bar(i + 1, len(radio_data), prefix_text=f'DB | Radio | {len(radio_data)}', start_time=start_time_radio)
        cursor.execute("INSERT INTO radio (timestamp_utc, session_fk, driver_fk, radio_url) VALUES (?, ?, ?, ?)", entry)
    conn.commit()

    # --- Process Event Data ---
    event_data = []
    for ev in race_control:
        session_key = ev.get('session_key')
        driver_number = ev.get('driver_number')
        driver_code = session_driver_number_to_code_map.get((session_key, driver_number)) if driver_number else None

        event_data.append((
            ev.get('category'),
            ev.get('flag'),
            ev.get('scope'),
            ev.get('message'),
            ev.get('lap_number'),
            ev.get('sector'), # This will be inserted into sector_num
            format_timestamp(ev.get('date'), 'int'),
            session_key,
            driver_code
        ))

    start_time_event = time.time()
    for i, entry in enumerate(event_data):
        show_progress_bar(i + 1, len(event_data), prefix_text=f'DB | Event | {len(event_data)}', start_time=start_time_event)
        cursor.execute("INSERT INTO event (category, flag, scope, message, lap_num, sector_num, timestamp_utc, session_fk, driver_fk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", entry)
    conn.commit()

    conn.close()
