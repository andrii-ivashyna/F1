# config.py
import os
import sqlite3
import sys
from datetime import datetime
import time

# --- Application Configuration ---
DB_FILE = 'data/formula.db'
API_BASE_URL = 'https://api.openf1.org/v1'
YEAR = 2025 # Fetch data from this year

# --- Terminal Logging Styling ---
class Style:
    """ANSI escape codes for terminal colors."""
    RESET, BOLD, UNDERLINE = '\033[0m', '\033[1m', '\033[4m'
    BLACK, RED, GREEN, YELLOW = '\033[90m', '\033[91m', '\033[92m', '\033[93m'
    BLUE, MAGENTA, CYAN, WHITE = '\033[94m', '\033[95m', '\033[96m', '\033[97m'
    
    @staticmethod
    def url(url_string):
        """Formats a URL for cleaner log output."""
        return f".../{url_string.split('/')[-1]}" if isinstance(url_string, str) and '/' in url_string else url_string

LOG_STYLES = {
    'INFO': Style.CYAN, 'SUCCESS': Style.GREEN, 'WARNING': Style.YELLOW, 'ERROR': Style.RED,
    'HEADING': f'{Style.BOLD}{Style.WHITE}', 'SUBHEADING': f'{Style.BOLD}{Style.MAGENTA}'
}

def format_log_data(data_dict):
    """Formats a dictionary of data for log output."""
    if not data_dict: return ""
    return " " + Style.MAGENTA + str(data_dict) + Style.RESET

def log(message, msg_type='INFO', indent=0, data=None):
    """Prints a styled log message to the console."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    style = LOG_STYLES.get(msg_type, Style.RESET)
    
    prefix = ""
    if indent > 0: # For any indent, use a simple pointer or space, not the double pointer
        prefix = "  " * indent + "▸ "

    if msg_type == 'HEADING':
        print()
        full_message = f"{style}--- {message} ---{Style.RESET}"
    elif msg_type == 'SUBHEADING':
        full_message = f"{style}--- {message} ---{Style.RESET}"
    else:
        full_message = f"{style}{timestamp} {prefix}{message}{format_log_data(data)}{Style.RESET}"
    
    print(full_message)
    sys.stdout.flush()

def show_progress_bar(current, total, prefix_text='', length=40, fill='█', start_time=None):
    """
    Displays a dynamic progress bar in the console.
    :param current: Current iteration.
    :param total: Total iterations.
    :param prefix_text: The descriptive text for the progress bar (e.g., "API Countries").
    :param length: Character length of the bar.
    :param fill: Bar fill character.
    :param start_time: Optional, time.time() when the operation started, to display elapsed time.
    """
    percent = ("{0:.1f}").format(100 * (current / float(total)))
    filled_length = int(length * current // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    
    time_str = ""
    if start_time is not None:
        elapsed = time.time() - start_time
        time_str = f" {Style.YELLOW}({elapsed:.1f}s){Style.RESET}"

    # Pad prefix_text to a consistent length, then add "   " before the bar
    # Max length for "Formula1.com Circuits" is 22 chars. Let's pad to 22 chars for consistent alignment, then add 3 spaces.
    padded_label = f"{prefix_text:<22}   "

    sys.stdout.write(f"\r{Style.CYAN}{padded_label}|{bar}| {percent}%{Style.RESET}{time_str}")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write(f"\r{Style.CYAN}{padded_label}|{bar}| {percent}%{Style.RESET}{time_str}\n")
        sys.stdout.flush()

# --- Database Setup Function ---
def create_database():
    """Creates the database and tables using the schema defined below."""
    log("Initializing database", 'HEADING')
    
    os.makedirs('data', exist_ok=True)
    if not os.path.exists(DB_FILE):
        log("Created 'data' directory.", 'SUCCESS', indent=1)

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        tables = [row[0] for row in cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()]
        if tables:
            log(f"Dropping {len(tables)} existing tables...", 'INFO', indent=1)
            cursor.executescript(';'.join(f"DROP TABLE IF EXISTS {t}" for t in tables))

        log(f"Creating {SCHEMA.count('CREATE TABLE')} new tables...", 'INFO', indent=1)
        cursor.executescript(SCHEMA)
        
    log("Database and tables created successfully.", 'SUCCESS')

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
    air_temperature REAL,
    track_temperature REAL,
    humidity SMALLINT,
    air_pressure SMALLINT,
    wind_direction SMALLINT,
    wind_speed REAL,
    is_raining BOOLEAN,
    date TEXT,
    session_fk SMALLINT,
    FOREIGN KEY (session_fk) REFERENCES session(session_key)
);
"""
