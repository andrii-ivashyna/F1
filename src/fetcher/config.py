"""
Configuration settings for OpenF1 API Data Fetcher
"""

import os

# Database and file paths
DB_FILENAME = "f1_data.db"
DATA_FOLDER = "data"
FETCHER_FOLDER = "src/fetcher"
LOG_FILENAME = "fetcher_log.log"
PROGRESS_FILENAME = "fetcher_progress.json"

# API Configuration
BASE_URL = "https://api.openf1.org/v1"
USER_AGENT = "OpenF1-Data-Fetcher/2.0"
REQUEST_TIMEOUT = 60
RATE_LIMIT_DELAY = 0.2

# Ensure folders exist
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(FETCHER_FOLDER, exist_ok=True)

# Table schemas
TABLE_SCHEMAS = {
    'meetings': '''
        CREATE TABLE IF NOT EXISTS meetings (
            circuit_key INTEGER,
            circuit_short_name TEXT,
            country_code TEXT,
            country_key INTEGER,
            country_name TEXT,
            date_start TEXT,
            gmt_offset TEXT,
            location TEXT,
            meeting_key INTEGER PRIMARY KEY,
            meeting_name TEXT,
            meeting_official_name TEXT,
            year INTEGER
        )
    ''',
    
    'sessions': '''
        CREATE TABLE IF NOT EXISTS sessions (
            circuit_key INTEGER,
            circuit_short_name TEXT,
            country_code TEXT,
            country_key INTEGER,
            country_name TEXT,
            date_end TEXT,
            date_start TEXT,
            gmt_offset TEXT,
            location TEXT,
            meeting_key INTEGER,
            session_key INTEGER PRIMARY KEY,
            session_name TEXT,
            session_type TEXT,
            year INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key)
        )
    ''',
    
    'drivers': '''
        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            broadcast_name TEXT,
            country_code TEXT,
            driver_number INTEGER,
            first_name TEXT,
            full_name TEXT,
            headshot_url TEXT,
            last_name TEXT,
            meeting_key INTEGER,
            name_acronym TEXT,
            session_key INTEGER,
            team_colour TEXT,
            team_name TEXT,
            year INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'intervals': '''
        CREATE TABLE IF NOT EXISTS intervals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            driver_number INTEGER,
            gap_to_leader REAL,
            interval REAL,
            meeting_key INTEGER,
            session_key INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'laps': '''
        CREATE TABLE IF NOT EXISTS laps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_start TEXT,
            driver_number INTEGER,
            duration_sector_1 REAL,
            duration_sector_2 REAL,
            duration_sector_3 REAL,
            i1_speed INTEGER,
            i2_speed INTEGER,
            is_pit_out_lap BOOLEAN,
            lap_duration REAL,
            lap_number INTEGER,
            meeting_key INTEGER,
            segments_sector_1 TEXT,
            segments_sector_2 TEXT,
            segments_sector_3 TEXT,
            session_key INTEGER,
            st_speed INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'pit': '''
        CREATE TABLE IF NOT EXISTS pit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            driver_number INTEGER,
            lap_number INTEGER,
            meeting_key INTEGER,
            pit_duration REAL,
            session_key INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'position': '''
        CREATE TABLE IF NOT EXISTS position (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            driver_number INTEGER,
            meeting_key INTEGER,
            position INTEGER,
            session_key INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'race_control': '''
        CREATE TABLE IF NOT EXISTS race_control (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT,
            date TEXT,
            driver_number INTEGER,
            flag TEXT,
            lap_number INTEGER,
            meeting_key INTEGER,
            message TEXT,
            scope TEXT,
            sector INTEGER,
            session_key INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'stints': '''
        CREATE TABLE IF NOT EXISTS stints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            compound TEXT,
            driver_number INTEGER,
            lap_end INTEGER,
            lap_start INTEGER,
            meeting_key INTEGER,
            session_key INTEGER,
            stint_number INTEGER,
            tyre_age_at_start INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'team_radio': '''
        CREATE TABLE IF NOT EXISTS team_radio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            driver_number INTEGER,
            meeting_key INTEGER,
            recording_url TEXT,
            session_key INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',

    'weather': '''
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            air_temperature REAL,
            date TEXT,
            humidity INTEGER,
            meeting_key INTEGER,
            pressure REAL,
            rainfall INTEGER,
            session_key INTEGER,
            track_temperature REAL,
            wind_direction INTEGER,
            wind_speed REAL,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'car_data': '''
        CREATE TABLE IF NOT EXISTS car_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brake INTEGER,
            date TEXT,
            driver_number INTEGER,
            drs INTEGER,
            meeting_key INTEGER,
            n_gear INTEGER,
            rpm INTEGER,
            session_key INTEGER,
            speed INTEGER,
            throttle INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    ''',
    
    'location': '''
        CREATE TABLE IF NOT EXISTS location (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            driver_number INTEGER,
            meeting_key INTEGER,
            session_key INTEGER,
            x INTEGER,
            y INTEGER,
            z INTEGER,
            FOREIGN KEY (meeting_key) REFERENCES meetings (meeting_key),
            FOREIGN KEY (session_key) REFERENCES sessions (session_key)
        )
    '''
}
