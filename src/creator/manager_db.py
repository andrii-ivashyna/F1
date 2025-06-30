# manager_db.py
"""
Database management utilities including schema creation and helper functions.
"""

import os
import sqlite3
import time
import pycountry
from config import DB_FILE, log, show_progress_bar

def format_int_date(date_str):
    """Formats date string to proper length for database storage."""
    if isinstance(date_str, str) and len(date_str) > 19: 
        return date_str[:19]
    return date_str

def format_real_date(date_str):
    """Formats weather date string to ISO-MM-DDTHH:MM:SS.sss"""
    if isinstance(date_str, str) and len(date_str) > 23: 
        return date_str[:23]
    return date_str

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
    country_code CHAR(3) PRIMARY KEY, 
    country_name VARCHAR(50) UNIQUE
);

CREATE TABLE circuit (
    circuit_key SMALLINT PRIMARY KEY, 
    circuit_name VARCHAR(30), 
    circuit_official_name VARCHAR(100),
    location VARCHAR(50), 
    type TEXT CHECK(type IN ('street', 'race')),
    direction TEXT CHECK(direction IN ('clockwise', 'anti-clockwise', 'both')),
    length_km REAL, 
    laps SMALLINT, 
    turns SMALLINT, 
    gmt_offset VARCHAR(6),
    country_fk CHAR(3), 
    map_image_url VARCHAR(255),
    FOREIGN KEY (country_fk) REFERENCES country(country_code)
);

CREATE TABLE meeting (
    meeting_key SMALLINT PRIMARY KEY, 
    meeting_name VARCHAR(50), 
    meeting_official_name VARCHAR(100),
    date_start TEXT, 
    circuit_fk SMALLINT,
    FOREIGN KEY (circuit_fk) REFERENCES circuit(circuit_key)
);

CREATE TABLE session (
    session_key SMALLINT PRIMARY KEY, 
    session_name VARCHAR(30), 
    session_type VARCHAR(30),
    date_start TEXT, 
    date_end TEXT, 
    meeting_fk SMALLINT,
    FOREIGN KEY (meeting_fk) REFERENCES meeting(meeting_key)
);

CREATE TABLE team (
    team_id INTEGER PRIMARY KEY, 
    team_name VARCHAR(30) UNIQUE, 
    team_official_name VARCHAR(100),
    power_unit VARCHAR(30), 
    chassis VARCHAR(30), 
    country_fk CHAR(3),
    logo_image_url VARCHAR(255), 
    car_image_url VARCHAR(255),
    FOREIGN KEY (country_fk) REFERENCES country(country_code)
);

CREATE TABLE driver (
    driver_code CHAR(3) PRIMARY KEY, 
    driver_name VARCHAR(60), 
    driver_number SMALLINT, 
    country_fk CHAR(3),
    driver_image_url VARCHAR(255), 
    number_image_url VARCHAR(255),
    FOREIGN KEY (country_fk) REFERENCES country(country_code)
);

CREATE TABLE meeting_driver (
    meeting_fk SMALLINT, 
    driver_fk CHAR(3), 
    team_fk INTEGER, 
    driver_number SMALLINT,
    PRIMARY KEY (meeting_fk, driver_fk), 
    FOREIGN KEY (meeting_fk) REFERENCES meeting(meeting_key),
    FOREIGN KEY (driver_fk) REFERENCES driver(driver_code), 
    FOREIGN KEY (team_fk) REFERENCES team(team_id)
);

CREATE TABLE session_driver (
    session_fk SMALLINT, 
    driver_fk CHAR(3), 
    PRIMARY KEY (session_fk, driver_fk),
    FOREIGN KEY (session_fk) REFERENCES session(session_key), 
    FOREIGN KEY (driver_fk) REFERENCES driver(driver_code)
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
    date TEXT,
    session_fk SMALLINT,
    FOREIGN KEY (session_fk) REFERENCES session(session_key)
);
"""
