# config.py

import os
import sqlite3
from datetime import datetime

# --- Application Configuration ---
DB_FILE = 'data/formula.db'
API_BASE_URL = 'https://api.openf1.org/v1'
YEARS = [2025] # Fetch data for these years

# --- Logging Helper ---
def log(message):
    """Prints a message with a timestamp."""
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

# --- Database Setup Function ---
def create_database():
    """Creates the database and tables using the schema defined below."""
    log("Initializing database...")
    if not os.path.exists('data'):
        os.makedirs('data')

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    log("Dropping existing tables...")
    for table_name in TABLES_TO_DROP:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    log("Creating tables with the new structure...")
    for create_statement in CREATE_TABLE_STATEMENTS:
        cursor.execute(create_statement)
    conn.commit()
    conn.close()
    log("Database and tables created successfully.")

# --- Database Structure ---
TABLES_TO_DROP = [
    "session_driver",
    "meeting_driver",
    "driver",
    "team",
    "session",
    "meeting",
    "circuit",
    "country",
]

CREATE_TABLE_STATEMENTS = [
    """
    CREATE TABLE country (
        country_code CHAR(3) PRIMARY KEY,
        country_name VARCHAR(50) UNIQUE
    );
    """,
    """
    CREATE TABLE circuit (
        circuit_key SMALLINT PRIMARY KEY,
        circuit_name VARCHAR(30),
        circuit_official_name VARCHAR(100),
        location VARCHAR(50),
        type TEXT CHECK(type IN ('street', 'race')),
        direction TEXT CHECK(direction IN ('clockwise', 'anti-clockwise', 'both')),
        length_km FLOAT,
        laps SMALLINT,
        turns SMALLINT,
        gmt_offset VARCHAR(6),
        country_fk CHAR(3),
        circuit_map_url VARCHAR(255),
        FOREIGN KEY (country_fk) REFERENCES country(country_code)
    );
    """,
    """
    CREATE TABLE meeting (
        meeting_key SMALLINT PRIMARY KEY,
        meeting_name VARCHAR(50),
        meeting_official_name VARCHAR(100),
        date_start DATETIME,
        circuit_fk SMALLINT,
        FOREIGN KEY (circuit_fk) REFERENCES circuit(circuit_key)
    );
    """,
    """
    CREATE TABLE session (
        session_key SMALLINT PRIMARY KEY,
        session_name VARCHAR(30),
        session_type VARCHAR(30),
        date_start DATETIME,
        date_end DATETIME,
        meeting_fk SMALLINT,
        FOREIGN KEY (meeting_fk) REFERENCES meeting(meeting_key)
    );
    """,
    """
    CREATE TABLE team (
        team_id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_name VARCHAR(30) UNIQUE,
        team_official_name VARCHAR(100),
        power_unit VARCHAR(30),
        chassis VARCHAR(30),
        country_fk CHAR(3),
        team_logo_url VARCHAR(255),
        team_car_url VARCHAR(255),
        FOREIGN KEY (country_fk) REFERENCES country(country_code)
    );
    """,
    """
    CREATE TABLE driver (
        driver_code CHAR(3) PRIMARY KEY,
        driver_name VARCHAR(60),
        driver_number SMALLINT,
        country_fk CHAR(3),
        FOREIGN KEY (country_fk) REFERENCES country(country_code)
    );
    """,
    """
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
    """,
    """
    CREATE TABLE session_driver (
        session_fk SMALLINT,
        driver_fk CHAR(3),
        PRIMARY KEY (session_fk, driver_fk),
        FOREIGN KEY (session_fk) REFERENCES session(session_key),
        FOREIGN KEY (driver_fk) REFERENCES driver(driver_code)
    );
    """
]
