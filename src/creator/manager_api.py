# manager_api.py
"""
API client for fetching Formula 1 data from OpenF1 API and populating the database
"""

import sqlite3
import time
from datetime import datetime
import requests
import config
from config import log, show_progress_bar
from manager_db import format_timestamp, format_gmt_offset, get_country_code

def get_api_data_bulk(endpoint, params=None, max_tries=5):
    """
    Fetches data from an API endpoint with a retry mechanism
    This function does NOT display a progress bar; progress is handled by the caller.
    Params must be a dictionary.
    - If a value is a single item (e.g., {'key': value}), it forms 'key=value'.
    - If a value is a two-item list (e.g., {'key': [val1, val2]}), it forms 'key>=val1&key<=val2'.
    - Multiple parameters are joined by '&'.
    """
    if params is None:
        params = {}

    query_parts = []
    for key, value in params.items():
        if isinstance(value, list) and len(value) == 2:
            query_parts.append(f"{key}>={value[0]}&{key}<={value[1]}")
        else:
            query_parts.append(f"{key}={value}")

    query_string = '&'.join(query_parts)
    url = f"{config.API_BASE_URL}/{endpoint}"
    if query_string:
        url += f"?{query_string}"

    tries = 1
    while tries <= max_tries:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if tries < max_tries:
                tries += 1
                time.sleep(2**(tries))
            else:
                log(f"Failed to fetch {endpoint} data after {max_tries} tries: {e}", 'ERROR')
                return None
    return None

def populate_database():
    """
    Coordinates the fetching of all F1 data for a given year and populates the database
    """
    log("Fetching data from OpenF1 API", 'SUBHEADING')
    api_data = {}

    # --- Initial Data Fetch (Meetings) ---
    # Fetch meetings first, as their keys are required to query other endpoints accurately
    start_time_meetings = time.time()
    api_data['meetings'] = get_api_data_bulk('meetings', {'year': config.YEAR})
    if not api_data.get('meetings'):
        return log("Halting process: could not fetch meetings data. Check the configured YEAR", 'ERROR')
    show_progress_bar(1, 1, prefix_text=f'API | Meetings | 1', start_time=start_time_meetings)

    # --- API Data Fetching ---
    # Define which endpoints can be fetched in a single bulk request vs. per meeting
    bulk_endpoints_ordered = ['sessions', 'drivers', 'pit', 'stints', 'weather', 'race_control', 'team_radio']
    per_meeting_endpoints_ordered = ['position', 'laps', 'intervals']
    per_session_driver_endpoints = ['car_data', 'location']

    # Determine the range of meeting keys for the specified year to scope the queries
    meeting_keys = [m['meeting_key'] for m in api_data['meetings']]
    min_meeting_key = min(meeting_keys)
    max_meeting_key = max(meeting_keys)

    for endpoint in bulk_endpoints_ordered:
        start_time_endpoint = time.time()
        params = {'meeting_key': [min_meeting_key, max_meeting_key]}
        api_data[endpoint] = get_api_data_bulk(endpoint, params)
        if api_data[endpoint] is None:
            log(f"Could not fetch {endpoint} data. This section will be skipped", 'WARNING')
            api_data[endpoint] = []
        show_progress_bar(1, 1, prefix_text=f'API | {endpoint.capitalize():<12} | 1', start_time=start_time_endpoint)

    for endpoint in per_meeting_endpoints_ordered:
        api_data[endpoint] = []
        endpoint_fetch_start_time = time.time()
        no_data_meetings = []
        
        if not meeting_keys:
            log(f"No meetings found to fetch {endpoint.capitalize()} data", 'INFO')
            continue

        for i, meeting_key in enumerate(meeting_keys):
            data = get_api_data_bulk(endpoint, {'meeting_key': meeting_key})
            if data:
                api_data[endpoint].extend(data)
            else:
                no_data_meetings.append(f"meeting_key={meeting_key}")
            show_progress_bar(i + 1, len(meeting_keys), prefix_text=f'API | {endpoint.capitalize():<12} | {len(meeting_keys)}', start_time=endpoint_fetch_start_time)
        
        if no_data_meetings:
            log(f"No data returned for {endpoint.capitalize()} with params: {', '.join(no_data_meetings)}", 'INFO')

    log("API data fetching complete", 'SUCCESS') # New success message

    # --- Database Processing & Insertion ---
    log("Processing and inserting data into database", 'SUBHEADING')
    conn = sqlite3.connect(config.DB_FILE)
    cursor = conn.cursor()

    def insert_records(table, columns, data, msg_prefix):
        """Helper function to bulk insert records into a specified table"""
        if not data:
            log(f"Skipping DB insert for {msg_prefix} as no data was provided", 'WARNING')
            return
        start_time = time.time()
        cols_str = ', '.join(columns)
        placeholders = ', '.join(['?'] * len(columns))
        statement = "INSERT OR REPLACE" if table == "driver" else "INSERT OR IGNORE"
        cursor.executemany(f"{statement} INTO {table} ({cols_str}) VALUES ({placeholders})", data)
        conn.commit()
        show_progress_bar(len(data), len(data), prefix_text=f'DB | {msg_prefix:<15} | {len(data)}', start_time=start_time)

    # Insert foundational data (meetings, countries, circuits)
    meetings = api_data.get('meetings', [])
    insert_records('meeting', ['meeting_key', 'meeting_name', 'meeting_official_name', 'timestamp_utc', 'circuit_fk'], 
                [(m.get('meeting_key'), m.get('meeting_name'), m.get('meeting_official_name'), format_timestamp(m.get('date_start'), 'int'), m.get('circuit_key')) for m in meetings], 'Meeting')

    unique_countries = {m['country_name']: get_country_code(m['country_name']) for m in meetings if m.get('country_name')}
    insert_records('country', ['country_code', 'country_name'], [(code, name) for name, code in unique_countries.items() if name and code], 'Country')

    circuits = {m['circuit_key']: m for m in meetings}.values()
    insert_records('circuit', ['circuit_key', 'circuit_name', 'location', 'gmt_offset', 'country_fk'],
                [(c.get('circuit_key'), c.get('circuit_short_name'), c.get('location'), format_gmt_offset(c.get('gmt_offset')), unique_countries.get(c.get('country_name'))) for c in circuits], 'Circuit')

    # Insert session data and create a helper function to calculate duration
    def get_duration(start, end):
        try:
            delta = datetime.fromisoformat(end) - datetime.fromisoformat(start)
            h, rem = divmod(delta.total_seconds(), 3600)
            m, s = divmod(rem, 60)
            return f"{int(h):02}:{int(m):02}:{int(s):02}"
        except (TypeError, ValueError):
            return None
    
    sessions = api_data.get('sessions', [])
    insert_records('session', ['session_key', 'session_name', 'session_type', 'duration_utc', 'timestamp_utc', 'meeting_fk'],
                [(s.get('session_key'), s.get('session_name'), s.get('session_type'), get_duration(s.get('date_start'), s.get('date_end')), format_timestamp(s.get('date_start'), 'int'), s.get('meeting_key')) for s in sessions], 'Session')

    # Process and insert driver and team data
    drivers = api_data.get('drivers', [])
    session_to_meeting = {s['session_key']: s['meeting_key'] for s in sessions}
    # Get the latest driver details by sorting by meeting key to ensure we have the most recent info
    latest_drivers = {d['name_acronym']: d for d in sorted(drivers, key=lambda x: session_to_meeting.get(x.get('session_key'), 0)) if d.get('name_acronym')}.values()
    
    # Remove driver_number from driver table insertion
    insert_records('driver', ['driver_code', 'driver_name', 'country_fk'],
                [(d.get('name_acronym'), d.get('full_name'), d.get('country_code')) for d in latest_drivers], 'Driver')

    team_names = sorted({d.get('team_name') for d in latest_drivers if d.get('team_name')})
    insert_records('team', ['team_name'], [(name,) for name in team_names], 'Team')
    
    # Create a map from team name to team_id for use in foreign keys
    team_name_to_id = {row[1]: row[0] for row in cursor.execute(f"SELECT team_id, team_name FROM team")}

    # Insert data into session_driver join table with team_fk and driver_number
    # Sort by session_fk based on session_key order, then by driver_fk alphabetically
    session_key_order = {s['session_key']: i for i, s in enumerate(sessions)}
    session_drivers_raw = []
    for d in drivers:
        if all(k in d for k in ['session_key', 'name_acronym', 'team_name', 'driver_number']):
            session_drivers_raw.append({
                'session_fk': d['session_key'],
                'driver_fk': d['name_acronym'],
                'team_fk': team_name_to_id.get(d['team_name']),
                'driver_number': d.get('driver_number')
            })

    session_drivers_sorted = sorted(session_drivers_raw, 
                                    key=lambda x: (session_key_order.get(x['session_fk'], float('inf')), x['driver_fk']))

    session_drivers_to_insert = [(sd['session_fk'], sd['driver_fk'], sd['team_fk'], sd['driver_number']) for sd in session_drivers_sorted]
    insert_records('session_driver', ['session_fk', 'driver_fk', 'team_fk', 'driver_number'], session_drivers_to_insert, 'Session-Driver')
    
    # Process and insert telemetry and event data
    # Create a map for quick lookup of a driver's code from session_key and driver_number
    session_driver_map = {(d['session_key'], d['driver_number']): d['name_acronym'] for d in drivers if all(k in d for k in ['session_key', 'driver_number', 'name_acronym'])}

    # Stint
    weather_data = [(w.get('air_temperature'), w.get('track_temperature'), w.get('humidity'), w.get('pressure'), w.get('wind_direction'), w.get('wind_speed'), w.get('rainfall'), format_timestamp(w.get('date'), 'real'), w.get('session_key')) for w in api_data.get('weather', [])]
    insert_records('weather', ['air_temp_C', 'track_temp_C', 'rel_humidity_pct', 'air_pressure_mbar', 'wind_direction_deg', 'wind_speed_mps', 'is_raining', 'timestamp_utc', 'session_fk'], weather_data, 'Weather')

    # Combine Stint and Pit data for a more complete picture of race strategy
    session_type_map = {s['session_key']: s.get('session_type') for s in sessions}
    stints_by_driver, pits_by_driver = {}, {}
    for stint in api_data.get('stints', []):
        if driver_code := session_driver_map.get((stint.get('session_key'), stint.get('driver_number'))):
            stints_by_driver.setdefault((stint.get('session_key'), driver_code), []).append(stint)
    for pit in api_data.get('pit', []):
        if driver_code := session_driver_map.get((pit.get('session_key'), pit.get('driver_number'))):
            pits_by_driver.setdefault((pit.get('session_key'), driver_code), []).append(pit)

    combined_stint_data = []
    for key, stints in stints_by_driver.items():
        session_key, driver_code = key
        pits = sorted(pits_by_driver.get(key, []), key=lambda x: x.get('lap_number', 0))
        stints.sort(key=lambda x: x.get('stint_number', 0))
        
        if session_type_map.get(session_key) == 'Race':
            max_lap = stints[-1].get('lap_end', 0) if stints else 0
            for i, stint in enumerate(stints):
                if not stint.get('lap_start'): continue
                lap_start = 1 if i == 0 else (pits[i-1].get('lap_number', 0) + 1 if i-1 < len(pits) else None)
                if lap_start is None: continue
                pit_duration = None if i == 0 else (pits[i-1].get('pit_duration') if i-1 < len(pits) else None)
                lap_end = pits[i].get('lap_number') if i < len(pits) else max_lap
                combined_stint_data.append((stint.get('stint_number'), stint.get('compound'), lap_start, lap_end or max_lap, pit_duration, stint.get('tyre_age_at_start'), session_key, driver_code))
        else:
            pit_map = {p.get('lap_number'): p.get('pit_duration') for p in pits}
            for stint in stints:
                lap_start = stint.get('lap_start')
                combined_stint_data.append((stint.get('stint_number'), stint.get('compound'), lap_start, stint.get('lap_end'), pit_map.get(lap_start), stint.get('tyre_age_at_start'), session_key, driver_code))
    insert_records('stint', ['stint_num', 'tyre_compound', 'lap_num_start', 'lap_num_end', 'pit_duration_s', 'tyre_age_laps', 'session_fk', 'driver_fk'], combined_stint_data, 'Stint')

    # Event
    event_data = [(ev.get('category'), ev.get('flag'), ev.get('scope'), ev.get('message'), ev.get('lap_number'), ev.get('sector'), format_timestamp(ev.get('date'), 'int'), ev.get('session_key'), session_driver_map.get((ev.get('session_key'), ev.get('driver_number')))) for ev in api_data.get('race_control', [])]
    insert_records('event', ['category', 'flag', 'scope', 'message', 'lap_num', 'sector_num', 'timestamp_utc', 'session_fk', 'driver_fk'], event_data, 'Event')

    # Radio
    radio_data = [(format_timestamp(r.get('date'), 'real'), r.get('session_key'), session_driver_map.get((r.get('session_key'), r.get('driver_number'))), r.get('recording_url')) for r in api_data.get('team_radio', []) if session_driver_map.get((r.get('session_key'), r.get('driver_number')))]
    insert_records('radio', ['timestamp_utc', 'session_fk', 'driver_fk', 'radio_url'], radio_data, 'Radio')

    # Position
    position_to_insert = [(p.get('position'), format_timestamp(p.get('date'), 'real'), p.get('session_key'), session_driver_map.get((p.get('session_key'), p.get('driver_number')))) for p in api_data.get('position', []) if session_driver_map.get((p.get('session_key'), p.get('driver_number')))]
    insert_records('position', ['position', 'timestamp_utc', 'session_fk', 'driver_fk'], position_to_insert, 'Position')

    # Lap
    lap_data_to_insert = [(lap.get('lap_number'), lap.get('lap_duration'), lap.get('duration_sector_1'), lap.get('duration_sector_2'), lap.get('duration_sector_3'), lap.get('st_speed'), lap.get('i1_speed'), lap.get('i2_speed'), lap.get('is_pit_out_lap'), format_timestamp(lap.get('date_start'), 'real'), str(lap.get('segments_sector_1')), str(lap.get('segments_sector_2')), str(lap.get('segments_sector_3')), lap.get('session_key'), session_driver_map.get((lap.get('session_key'), lap.get('driver_number')))) for lap in api_data.get('laps', []) if session_driver_map.get((lap.get('session_key'), lap.get('driver_number')))]
    insert_records('lap', ['lap_number', 'lap_duration', 'duration_sector_1', 'duration_sector_2', 'duration_sector_3', 'st_speed', 'i1_speed', 'i2_speed', 'is_pit_out_lap', 'timestamp_utc', 'segments_sector_1', 'segments_sector_2', 'segments_sector_3', 'session_fk', 'driver_fk'], lap_data_to_insert, 'Lap')

    # Interval
    interval_to_insert = [(i.get('gap_to_leader'), i.get('interval'), format_timestamp(i.get('date'), 'real'), i.get('session_key'), session_driver_map.get((i.get('session_key'), i.get('driver_number')))) for i in api_data.get('intervals', []) if session_driver_map.get((i.get('session_key'), i.get('driver_number')))]
    insert_records('interval', ['gap_to_leader', 'interval', 'timestamp_utc', 'session_fk', 'driver_fk'], interval_to_insert, 'Interval')

    # --- Fetching per session_driver for large datasets (Car Data, Location) ---
    log("Fetching large datasets per session and driver", 'SUBHEADING')
    
    # Retrieve all session-driver combinations from the session_driver table
    cursor.execute("SELECT session_fk, driver_fk, driver_number FROM session_driver ORDER BY session_fk DESC") # Order by session_fk descending
    session_driver_combinations = cursor.fetchall()

    # Limit to the last 3 sessions for testing purposes
    limited_session_driver_combinations = []
    processed_sessions_count = 0
    last_session_fk = None

    for session_fk, driver_fk, driver_number in session_driver_combinations:
        if last_session_fk is None or session_fk != last_session_fk:
            if processed_sessions_count >= 3:
                break
            processed_sessions_count += 1
            last_session_fk = session_fk
        limited_session_driver_combinations.append((session_fk, driver_fk, driver_number))
    
    total_combinations = len(limited_session_driver_combinations)

    for endpoint in per_session_driver_endpoints:
        all_endpoint_data = []
        endpoint_fetch_start_time = time.time()

        for i, (session_fk, driver_fk, driver_number) in enumerate(limited_session_driver_combinations):
            params = {'session_key': session_fk, 'driver_number': driver_number}
            data = get_api_data_bulk(endpoint, params)
            if data:
                # Add session_fk and driver_fk to each record
                for record in data:
                    record['session_fk'] = session_fk
                    record['driver_fk'] = driver_fk
                all_endpoint_data.extend(data)
            
            show_progress_bar(i + 1, total_combinations, prefix_text=f'API | {endpoint.capitalize():<12} | {total_combinations}', start_time=endpoint_fetch_start_time)
        
        if endpoint == 'car_data':
            car_data_to_insert = [(
                c.get('rpm'), c.get('speed'), c.get('n_gear'), c.get('throttle'),
                c.get('brake'), c.get('drs'), format_timestamp(c.get('date'), 'real'),
                c.get('session_fk'), c.get('driver_fk')
            ) for c in all_endpoint_data]
            insert_records('car', ['rpm', 'speed', 'n_gear', 'throttle', 'brake', 'drs', 'timestamp_utc', 'session_fk', 'driver_fk'], car_data_to_insert, 'Car Data')
        elif endpoint == 'location':
            loc_data_to_insert = [(
                l.get('x'), l.get('y'), l.get('z'), format_timestamp(l.get('date'), 'real'),
                l.get('session_fk'), l.get('driver_fk')
            ) for l in all_endpoint_data]
            insert_records('loc', ['x', 'y', 'z', 'timestamp_utc', 'session_fk', 'driver_fk'], loc_data_to_insert, 'Location Data')

    log("Database processing and insertion complete", 'SUCCESS')

    # --- Finalization ---
    conn.close()
