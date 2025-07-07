# stint_analyzer.py
"""
Analyzes stint data for a specific Formula 1 meeting
and prints a detailed report to the terminal.

This script connects to the pre-populated SQLite database and performs the following:
1.  Finds the specified Grand Prix meeting (default: Monaco).
2.  Retrieves all sessions for that meeting.
3.  For each session, fetches the combined stint data, which now includes pit stop details.
4.  Groups the data by driver.
5.  For each driver, it prints:
    - The total number of stints and pit stops.
    - A detailed list of each stint (number, compound, laps, tyre age).
    - Pit stop duration if one preceded the stint.
"""

import sqlite3
import sys
import os
from collections import defaultdict

# Adjust the Python path to allow imports from the parent 'src' directory.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    # Now that the path is adjusted, we can import from the sibling directory.
    from creator.config import DB_FILE, Style, log
except ImportError:
    # Define fallback styles and logger if the import fails.
    print("Warning: Could not import from 'creator.config'. Using fallback for styling and logging.")
    DB_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'formula.db')
    class Style:
        RESET, BOLD, UNDERLINE = '', '', ''
        BLACK, RED, GREEN, YELLOW = '', '', '', ''
        BLUE, MAGENTA, CYAN, WHITE = '', '', '', ''
    def log(message, msg_type='INFO', **kwargs):
        print(f"[{msg_type}] {message}")


def get_meeting_key(cursor, meeting_name="Monaco Grand Prix"):
    """Retrieves the meeting_key for a given meeting name."""
    log(f"Searching for meeting: '{meeting_name}'", 'INFO')
    cursor.execute("SELECT meeting_key FROM meeting WHERE meeting_name = ?", (meeting_name,))
    result = cursor.fetchone()
    if result:
        log(f"Found meeting_key: {result[0]}", 'SUCCESS')
        return result[0]
    log(f"Meeting '{meeting_name}' not found in the database.", 'ERROR')
    return None

def get_sessions_for_meeting(cursor, meeting_key):
    """Retrieves all sessions for a given meeting_key."""
    cursor.execute(
        "SELECT session_key, session_name, session_type FROM session WHERE meeting_fk = ? ORDER BY timestamp_utc",
        (meeting_key,)
    )
    sessions = cursor.fetchall()
    log(f"Found {len(sessions)} sessions for this meeting.", 'INFO')
    return sessions

def analyze_stints(cursor, session_key, session_name, session_type):
    """Analyzes and prints the stint data for a given session."""
    log(f"Analyzing Session: {session_name} ({session_type})", 'HEADING')

    # Fetch all stint data for the session, including the new pit_duration_s column.
    cursor.execute("""
        SELECT
            driver_fk,
            stint_num,
            tyre_compound,
            lap_num_start,
            lap_num_end,
            tyre_age_laps,
            pit_duration_s
        FROM stint
        WHERE session_fk = ?
        ORDER BY driver_fk, stint_num
    """, (session_key,))

    stints_raw = cursor.fetchall()
    if not stints_raw:
        log("No stint data found for this session.", 'WARNING')
        return

    # Group stints and pit stop info by driver directly from the query result.
    driver_stints = defaultdict(list)
    pit_stop_summary = defaultdict(lambda: {'count': 0, 'laps': []})

    for stint_data in stints_raw:
        driver_fk, stint_num, compound, lap_start, lap_end, tyre_age, pit_duration = stint_data
        
        driver_stints[driver_fk].append({
            'stint_num': stint_num,
            'compound': compound,
            'lap_start': lap_start,
            'lap_end': lap_end,
            'tyre_age': tyre_age,
            'pit_duration': pit_duration
        })

        # If pit_duration exists, it means a pit stop occurred before this stint.
        if pit_duration is not None:
            pit_lap = lap_start - 1
            pit_stop_summary[driver_fk]['count'] += 1
            if pit_lap not in pit_stop_summary[driver_fk]['laps']:
                pit_stop_summary[driver_fk]['laps'].append(pit_lap)

    # Process and print the analysis for each driver.
    for driver_code, stints_list in sorted(driver_stints.items()):
        pit_data = pit_stop_summary.get(driver_code, {'count': 0, 'laps': []})
        pit_count = pit_data['count']
        pit_laps = ", ".join(map(str, sorted(pit_data['laps'])))
        
        pit_info_str = f" (Pit Stops: {pit_count}"
        if pit_laps:
            pit_info_str += f" on laps {pit_laps})"
        else:
            pit_info_str += ")"

        print(f"\n{Style.BOLD}{Style.MAGENTA}Driver: {driver_code}{pit_info_str}{Style.RESET}")
        print(f"  Total Stints: {len(stints_list)}")

        for stint in stints_list:
            lap_end_display = stint['lap_end'] if stint['lap_end'] is not None else 'N/A'
            laps_display = f"{stint['lap_start']}-{lap_end_display}"
            
            pit_display = ""
            if stint['pit_duration'] is not None:
                pit_duration_str = f"{stint['pit_duration']:.3f}s"
                pit_display = f" {Style.CYAN}[PIT: {pit_duration_str} before stint]{Style.RESET}"

            compound_display = "N/A" if not stint['compound'] else stint['compound']
            print(
                f"  - Stint {stint['stint_num']:<2} | "
                f"Compound: {compound_display:<12} | "
                f"Laps: {laps_display:<6} | "
                f"Tyre Age: {stint['tyre_age']:<2} laps"
                f"{pit_display}"
            )

def main():
    """Main function to run the stint analysis."""
    if not os.path.exists(DB_FILE):
        log(f"Database file not found at '{DB_FILE}'.", 'ERROR')
        log("Please run the main data population script first.", 'INFO')
        sys.exit(1)

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        meeting_name = "Canadian Grand Prix"
        meeting_key = get_meeting_key(cursor, meeting_name)
        if meeting_key is None:
            sys.exit(1)

        sessions = get_sessions_for_meeting(cursor, meeting_key)
        for session_key, session_name, session_type in sessions:
            analyze_stints(cursor, session_key, session_name, session_type)
            print("-" * 80)

    except sqlite3.Error as e:
        log(f"Database error: {e}", 'ERROR')
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            log("Analysis complete. Database connection closed.", 'SUCCESS')

if __name__ == '__main__':
    main()
