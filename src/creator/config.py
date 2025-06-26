# config.py

import os
import sqlite3
import sys
from datetime import datetime

# --- Application Configuration ---
DB_FILE = 'data/formula.db'
API_BASE_URL = 'https://api.openf1.org/v1'
YEARS = [2025] # Fetch data for these years

# --- Terminal Logging Styling ---
class Style:
    """ANSI escape codes for terminal colors."""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    @staticmethod
    def url(url_string):
        """Formats a URL for cleaner log output."""
        if not isinstance(url_string, str) or '/' not in url_string:
            return url_string
        return f".../{url_string.split('/')[-1]}"

LOG_STYLES = {
    'INFO': Style.CYAN,
    'SUCCESS': Style.GREEN,
    'WARNING': Style.YELLOW,
    'ERROR': Style.RED,
    'DATA': Style.MAGENTA,
    'HEADING': f'{Style.BOLD}{Style.UNDERLINE}{Style.WHITE}',
    'SUBHEADING': f'{Style.BOLD}{Style.BLUE}'
}

def format_log_data(data_dict):
    """Formats a dictionary of data for beautiful logging."""
    parts = []
    for key, value in data_dict.items():
        if value is None:
            continue
        # Shorten URLs in the log output if they are values in the data dict
        if isinstance(value, str) and value.startswith('http'):
            formatted_value = f"'{Style.url(value)}'"
        else:
            formatted_value = f"'{value}'"
        
        parts.append(f"{Style.BOLD}{key}={Style.RESET}{Style.WHITE}{formatted_value}{Style.RESET}")
    return f"{Style.WHITE}[ {f'{Style.RESET}, '.join(parts)}{Style.WHITE} ]{Style.RESET}"

def log(message, level='INFO', data=None, indent=0):
    """
    Prints a styled and structured message with a timestamp.
    - message: The log message.
    - level: INFO, SUCCESS, WARNING, ERROR, DATA, HEADING, SUBHEADING.
    - data: An optional dictionary to be pretty-printed.
    - indent: Indentation level.
    """
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
    if not os.path.exists('data'):
        os.makedirs('data')
        log("Created 'data' directory.", 'SUCCESS', indent=1)

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    log(f"Dropping {len(TABLES_TO_DROP)} existing tables...", 'INFO', indent=1)
    for table_name in TABLES_TO_DROP:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    log(f"Creating {len(CREATE_TABLE_STATEMENTS)} new tables...", 'INFO', indent=1)
    for create_statement in CREATE_TABLE_STATEMENTS:
        cursor.execute(create_statement)
    conn.commit()
    conn.close()
    log("Database and tables created successfully.", 'SUCCESS')

# --- Database Structure (Schema) ---
TABLES_TO_DROP = [
    "session_driver", "meeting_driver", "driver", "team",
    "session", "meeting", "circuit", "country",
]
CREATE_TABLE_STATEMENTS = [
    "CREATE TABLE country (country_code CHAR(3) PRIMARY KEY, country_name VARCHAR(50) UNIQUE);",
    """CREATE TABLE circuit (
        circuit_key SMALLINT PRIMARY KEY, circuit_name VARCHAR(30), circuit_official_name VARCHAR(100),
        location VARCHAR(50), type TEXT CHECK(type IN ('street', 'race')),
        direction TEXT CHECK(direction IN ('clockwise', 'anti-clockwise', 'both')),
        length_km FLOAT, laps SMALLINT, turns SMALLINT, gmt_offset VARCHAR(6),
        country_fk CHAR(3), map_image_url VARCHAR(255),
        FOREIGN KEY (country_fk) REFERENCES country(country_code));""",
    """CREATE TABLE meeting (
        meeting_key SMALLINT PRIMARY KEY, meeting_name VARCHAR(50), meeting_official_name VARCHAR(100),
        date_start DATETIME, circuit_fk SMALLINT,
        FOREIGN KEY (circuit_fk) REFERENCES circuit(circuit_key));""",
    """CREATE TABLE session (
        session_key SMALLINT PRIMARY KEY, session_name VARCHAR(30), session_type VARCHAR(30),
        date_start DATETIME, date_end DATETIME, meeting_fk SMALLINT,
        FOREIGN KEY (meeting_fk) REFERENCES meeting(meeting_key));""",
    """CREATE TABLE team (
        team_id INTEGER PRIMARY KEY AUTOINCREMENT, team_name VARCHAR(30) UNIQUE, team_official_name VARCHAR(100),
        power_unit VARCHAR(30), chassis VARCHAR(30), country_fk CHAR(3),
        logo_image_url VARCHAR(255), car_image_url VARCHAR(255),
        FOREIGN KEY (country_fk) REFERENCES country(country_code));""",
    """CREATE TABLE driver (
        driver_code CHAR(3) PRIMARY KEY, driver_name VARCHAR(60), driver_number SMALLINT, country_fk CHAR(3),
        driver_image_url VARCHAR(255), number_image_url VARCHAR(255),
        FOREIGN KEY (country_fk) REFERENCES country(country_code));""",
    """CREATE TABLE meeting_driver (
        meeting_fk SMALLINT, driver_fk CHAR(3), team_fk INTEGER, driver_number SMALLINT,
        PRIMARY KEY (meeting_fk, driver_fk), FOREIGN KEY (meeting_fk) REFERENCES meeting(meeting_key),
        FOREIGN KEY (driver_fk) REFERENCES driver(driver_code), FOREIGN KEY (team_fk) REFERENCES team(team_id));""",
    """CREATE TABLE session_driver (
        session_fk SMALLINT, driver_fk CHAR(3), PRIMARY KEY (session_fk, driver_fk),
        FOREIGN KEY (session_fk) REFERENCES session(session_key), FOREIGN KEY (driver_fk) REFERENCES driver(driver_code));"""
]
