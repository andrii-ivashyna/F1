# manager_db.py
"""
Database management utilities including schema creation and helper functions
"""

import os
import sqlite3
import time
import pycountry
from config import DB_FILE, log, show_progress_bar

def format_timestamp(timestamp_str, precision='int'):
    """
    Formats a timestamp string to a specified precision for database storage
    'int' for ISO-MM-DDTHH:MM:SS (length 19), 'real' for ISO-MM-DDTHH:MM:SS.sss (length 23)
    """
    if not isinstance(timestamp_str, str):
        return timestamp_str

    if precision == 'int' and len(timestamp_str) > 19:
        return timestamp_str[:19]
    elif precision == 'real' and len(timestamp_str) > 23:
        return timestamp_str[:23]
    return timestamp_str

def format_gmt_offset(gmt_str):
    """Formats GMT offset string to proper length."""
    if isinstance(gmt_str, str) and len(gmt_str) > 5:
        return gmt_str[:6] if gmt_str.startswith('-') else gmt_str[:5]
    return gmt_str

def get_country_code(country_name):
    """Converts country name to ISO 3-letter country code."""
    if not country_name:
        return None
    try:
        return pycountry.countries.search_fuzzy(country_name)[0].alpha_3
    except LookupError:
        return None

def create_database():
    """Creates the database and tables using the predefined schema."""
    log("Initializing database", 'SUBHEADING')

    os.makedirs('data', exist_ok=True)

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()

        tables = [row[0] for row in cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()]

        if tables:
            start_time = time.time()
            for i, table in enumerate(tables):
                show_progress_bar(i + 1, len(tables), prefix_text=f'DB | Drop tables | {len(tables)}', start_time=start_time)
                cursor.execute(f"DROP TABLE IF EXISTS {table}")

        table_count = SCHEMA.count('CREATE TABLE')
        start_time = time.time()
        cursor.executescript(SCHEMA)
        show_progress_bar(table_count, table_count, prefix_text=f'DB | Create tables | {table_count}', start_time=start_time)

# --- Optimized Database Schema ---
SCHEMA = """
CREATE TABLE country (
    country_code TEXT PRIMARY KEY,
    country_name TEXT UNIQUE
);

CREATE TABLE circuit (
    circuit_key SMALLINT PRIMARY KEY,
    circuit_name TEXT,
    circuit_official_name TEXT,
    location TEXT,
    type TEXT CHECK(type IN ('street', 'race')),
    direction TEXT CHECK(direction IN ('clockwise', 'anti-clockwise', 'both')),
    length_km REAL,
    laps SMALLINT,
    turns SMALLINT,
    gmt_offset TEXT,
    country_fk TEXT,
    map_image_url TEXT,
    FOREIGN KEY (country_fk) REFERENCES country(country_code)
);

CREATE TABLE meeting (
    meeting_key SMALLINT PRIMARY KEY,
    meeting_name TEXT,
    meeting_official_name TEXT,
    timestamp_utc TEXT,
    circuit_fk SMALLINT,
    FOREIGN KEY (circuit_fk) REFERENCES circuit(circuit_key)
);

CREATE TABLE session (
    session_key SMALLINT PRIMARY KEY,
    session_name TEXT,
    session_type TEXT,
    duration_utc TEXT,
    timestamp_utc TEXT,
    meeting_fk SMALLINT,
    FOREIGN KEY (meeting_fk) REFERENCES meeting(meeting_key)
);

CREATE TABLE team (
    team_id INTEGER PRIMARY KEY,
    team_name TEXT UNIQUE,
    team_official_name TEXT,
    power_unit TEXT,
    chassis TEXT,
    country_fk TEXT,
    logo_image_url TEXT,
    car_image_url TEXT,
    FOREIGN KEY (country_fk) REFERENCES country(country_code)
);

CREATE TABLE driver (
    driver_code TEXT PRIMARY KEY,
    driver_name TEXT,
    country_fk TEXT,
    driver_image_url TEXT,
    number_image_url TEXT,
    FOREIGN KEY (country_fk) REFERENCES country(country_code)
);

CREATE TABLE session_driver (
    session_fk SMALLINT,
    driver_fk TEXT,
    team_fk INTEGER,
    driver_number SMALLINT,
    PRIMARY KEY (session_fk, driver_fk),
    FOREIGN KEY (session_fk) REFERENCES session(session_key),
    FOREIGN KEY (driver_fk) REFERENCES driver(driver_code),
    FOREIGN KEY (team_fk) REFERENCES team(team_id)
);

CREATE TABLE weather (
    weather_id INTEGER PRIMARY KEY,
    air_temp_C REAL,
    track_temp_C REAL,
    rel_humidity_pct SMALLINT,
    air_pressure_mbar SMALLINT,
    wind_direction_deg SMALLINT,
    wind_speed_mps REAL,
    is_raining BOOLEAN,
    timestamp_utc TEXT,
    session_fk SMALLINT,
    FOREIGN KEY (session_fk) REFERENCES session(session_key)
);

CREATE TABLE stint (
    stint_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stint_num SMALLINT,
    tyre_compound TEXT,
    lap_num_start SMALLINT,
    lap_num_end SMALLINT,
    pit_duration_s REAL,
    tyre_age_laps SMALLINT,
    session_fk SMALLINT,
    driver_fk TEXT,
    FOREIGN KEY (session_fk, driver_fk) REFERENCES session_driver(session_fk, driver_fk)
);

CREATE TABLE radio (
    radio_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT,
    session_fk SMALLINT,
    driver_fk TEXT,
    radio_url TEXT,
    FOREIGN KEY (session_fk, driver_fk) REFERENCES session_driver(session_fk, driver_fk)
);

CREATE TABLE event (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT,
    flag TEXT,
    scope TEXT,
    message TEXT,
    lap_num SMALLINT,
    sector_num SMALLINT,
    timestamp_utc TEXT,
    session_fk SMALLINT,
    driver_fk TEXT,
    FOREIGN KEY (session_fk, driver_fk) REFERENCES session_driver(session_fk, driver_fk)
);

CREATE TABLE position (
    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
    position SMALLINT,
    timestamp_utc TEXT,
    session_fk SMALLINT,
    driver_fk TEXT,
    FOREIGN KEY (session_fk, driver_fk) REFERENCES session_driver(session_fk, driver_fk)
);

CREATE TABLE interval (
    interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
    gap_to_leader TEXT,
    interval TEXT,
    timestamp_utc TEXT,
    session_fk SMALLINT,
    driver_fk TEXT,
    FOREIGN KEY (session_fk, driver_fk) REFERENCES session_driver(session_fk, driver_fk)
);

CREATE TABLE lap (
    lap_id INTEGER PRIMARY KEY AUTOINCREMENT,
    lap_number SMALLINT,
    lap_duration REAL,
    duration_sector_1 REAL,
    duration_sector_2 REAL,
    duration_sector_3 REAL,
    st_speed SMALLINT,
    i1_speed SMALLINT,
    i2_speed SMALLINT,
    is_pit_out_lap BOOLEAN,
    timestamp_utc TEXT,
    segments_sector_1 TEXT,
    segments_sector_2 TEXT,
    segments_sector_3 TEXT,
    session_fk SMALLINT,
    driver_fk TEXT,
    FOREIGN KEY (session_fk, driver_fk) REFERENCES session_driver(session_fk, driver_fk)
);

CREATE TABLE car (
    car_id INTEGER PRIMARY KEY AUTOINCREMENT,
    rpm SMALLINT,
    speed SMALLINT,
    n_gear SMALLINT,
    throttle SMALLINT,
    brake SMALLINT,
    drs SMALLINT,
    timestamp_utc TEXT,
    session_fk SMALLINT,
    driver_fk TEXT,
    FOREIGN KEY (session_fk, driver_fk) REFERENCES session_driver(session_fk, driver_fk)
);

CREATE TABLE loc (
    loc_id INTEGER PRIMARY KEY AUTOINCREMENT,
    x SMALLINT,
    y SMALLINT,
    z SMALLINT,
    timestamp_utc TEXT,
    session_fk SMALLINT,
    driver_fk TEXT,
    FOREIGN KEY (session_fk, driver_fk) REFERENCES session_driver(session_fk, driver_fk)
);
"""
