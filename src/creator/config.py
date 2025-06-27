# config.py

import os
import sqlite3
import sys
from datetime import datetime

# --- Application Configuration ---
DB_FILE = 'data/formula.db'
API_BASE_URL = 'https://api.openf1.org/v1'
YEARS = [2025]

# --- Terminal Logging Styling ---
class Style:
    """ANSI escape codes for terminal colors."""
    RESET, BOLD, UNDERLINE = '\033[0m', '\033[1m', '\033[4m'
    BLACK, RED, GREEN, YELLOW = '\033[30m', '\033[31m', '\033[32m', '\033[33m'
    BLUE, MAGENTA, CYAN, WHITE = '\033[34m', '\033[35m', '\033[36m', '\033[37m'
    
    @staticmethod
    def url(url_string):
        """Formats a URL for cleaner log output."""
        return f".../{url_string.split('/')[-1]}" if isinstance(url_string, str) and '/' in url_string else url_string

LOG_STYLES = {
    'INFO': Style.CYAN, 'SUCCESS': Style.GREEN, 'WARNING': Style.YELLOW, 'ERROR': Style.RED,
    'DATA': Style.MAGENTA, 'HEADING': f'{Style.BOLD}{Style.UNDERLINE}{Style.WHITE}',
    'SUBHEADING': f'{Style.BOLD}{Style.BLUE}'
}

def format_log_data(data_dict):
    """Formats a dictionary of data for beautiful logging."""
    parts = []
    for key, value in data_dict.items():
        if value is None:
            continue
        formatted_value = f"'{Style.url(value)}'" if isinstance(value, str) and value.startswith('http') else f"'{value}'"
        parts.append(f"{Style.BOLD}{key}={Style.RESET}{Style.WHITE}{formatted_value}{Style.RESET}")
    return f"{Style.WHITE}[ {f'{Style.RESET}, '.join(parts)}{Style.WHITE} ]{Style.RESET}"

def log(message, level='INFO', data=None, indent=0):
    """Prints a styled and structured message with a timestamp."""
    timestamp = datetime.now().strftime('%H:%M:%S')
    color = LOG_STYLES.get(level.upper(), Style.WHITE)
    indent_space = "  " * indent
    
    if level.upper() in ['HEADING', 'SUBHEADING']:
        print(f"\n{indent_space}{color}--- {message} ---{Style.RESET}")
        return

    log_message = f"{Style.YELLOW}{timestamp}{Style.RESET} {color}{'▸' * (indent + 1):<3} {message}{Style.RESET}"
    if data:
        log_message += f" {format_log_data(data)}"
        
    sys.stdout.write(f'\r{log_message}\n')
    sys.stdout.flush()

def end_log():
    """Prints a final newline for clean exit."""
    print()

# --- Database Setup Function ---
def create_database():
    """Creates the database and tables using the schema defined below."""
    log("Initializing database", 'HEADING')
    
    os.makedirs('data', exist_ok=True)
    if not os.path.exists(DB_FILE):
        log("Created 'data' directory.", 'SUCCESS', indent=1)

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        # Get user tables only (exclude system tables)
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
"""
