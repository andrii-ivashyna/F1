# stint_analyzer.py
"""
Analyzes stint data for a specific Formula 1 meeting
and prints a detailed report to the terminal.

This script connects the pre-populated SQLite database and performs the following:
1.  Finds the specified Grand Prix meeting (default: Monaco).
2.  Retrieves all sessions for that meeting.
3.  For each session, fetches and joins stint and pit data.
4.  Groups the data by driver.
5.  For each driver, it prints:
    - The total number of stints.
    - A detailed list of each stint (number, compound, laps, tyre age).
    - Pit stop information (lap and duration) if the stint starts on a pit lap.
"""

import sqlite3
import sys
import os
from collections import defaultdict

# Adjust the Python path to allow imports from the parent 'src' directory.
# This assumes the script is in 'src/analysis/' and config is in 'src/creator/'.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    # Now that the path is adjusted, we can import from the sibling directory.
    from creator.config import DB_FILE, Style, log
except ImportError:
    # Define fallback styles and logger if the import fails for any reason
    print("Warning: Could not import from 'creator.config'. Using fallback for styling and logging.")
    # Adjust DB_FILE path based on the new structure assumption
    DB_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'formula.db')
    class Style:
        RESET, BOLD, UNDERLINE = '', '', ''
        BLACK, RED, GREEN, YELLOW = '', '', '', ''
        BLUE, MAGENTA, CYAN, WHITE = '', '', '', ''
    def log(message, msg_type='INFO', **kwargs):
        print(f"[{msg_type}] {message}")


def get_meeting_key(cursor, meeting_name="Monaco Grand Prix"):
    """
    Retrieves the meeting_key for a given meeting name.
    """
    log(f"Searching for meeting: '{meeting_name}'", 'INFO')
    cursor.execute("SELECT meeting_key FROM meeting WHERE meeting_name = ?", (meeting_name,))
    result = cursor.fetchone()
    if result:
        log(f"Found meeting_key: {result[0]}", 'SUCCESS')
        return result[0]
    else:
        log(f"Meeting '{meeting_name}' not found in the database.", 'ERROR')
        return None

def get_sessions_for_meeting(cursor, meeting_key):
    """
    Retrieves all sessions for a given meeting_key.
    """
    cursor.execute(
        "SELECT session_key, session_name, session_type FROM session WHERE meeting_fk = ? ORDER BY timestamp_utc",
        (meeting_key,)
    )
    sessions = cursor.fetchall()
    log(f"Found {len(sessions)} sessions for this meeting.", 'INFO')
    return sessions

def analyze_stints(cursor, session_key, session_name, session_type):
    """
    Analyzes and prints the stint and associated pit data for a given session.
    """
    log(f"Analyzing Session: {session_name} ({session_type})", 'HEADING')

    # Fetch all stints for the session.
    # We will fetch pit stops separately and match them in Python to handle the
    # "pit will present a start of a stint, not end" requirement cleanly
    # and avoid potential duplicate joins if pit data isn't perfectly one-to-one with stint starts.
    cursor.execute("""
        SELECT
            driver_fk,
            stint_num,
            tyre_compound,
            lap_num_start,
            lap_num_end,
            tyre_age_laps
        FROM stint
        WHERE session_fk = ?
        ORDER BY driver_fk, stint_num
    """, (session_key,))

    stints_raw = cursor.fetchall()

    if not stints_raw:
        log("No stint data found for this session.", 'WARNING')
        return

    # Fetch all pit stops for the current session, keyed by (driver_fk, lap_num) for easy lookup
    cursor.execute("""
        SELECT
            driver_fk,
            lap_num,
            duration_s
        FROM pit
        WHERE session_fk = ?
        ORDER BY driver_fk, lap_num
    """, (session_key,))
    
    # Using a defaultdict of lists to handle cases where multiple pit stops might
    # somehow be recorded for the same driver on the same lap, though typically it's one.
    pit_stops_by_lap = defaultdict(list)
    pit_stop_summary = defaultdict(lambda: {'count': 0, 'laps': []})

    for driver_fk, lap_num, duration_s in cursor.fetchall():
        pit_stops_by_lap[(driver_fk, lap_num)].append({'duration': duration_s})
        pit_stop_summary[driver_fk]['count'] += 1
        # Only add the lap to the summary list if it's not already there for this driver,
        # to prevent duplicates in the driver summary line.
        if lap_num not in pit_stop_summary[driver_fk]['laps']:
            pit_stop_summary[driver_fk]['laps'].append(lap_num)

    # Group stints by driver and attach pit info
    driver_stints = defaultdict(list)
    for stint in stints_raw:
        driver_fk, stint_num, compound, lap_start, lap_end, tyre_age = stint
        
        pit_info_for_stint = None
        # Check if this stint starts on a lap where a pit stop occurred
        matching_pits = pit_stops_by_lap.get((driver_fk, lap_start))
        if matching_pits:
            # If there are multiple pit stops on the same lap (unlikely for a single car normally),
            # we'll just take the first one or you can define specific logic.
            # For simplicity, taking the first one found.
            pit_info_for_stint = matching_pits[0] 

        driver_stints[driver_fk].append({
            'stint_num': stint_num,
            'compound': compound,
            'lap_start': lap_start,
            'lap_end': lap_end,
            'tyre_age': tyre_age,
            'pit_info': pit_info_for_stint # Attach the pit info directly to the stint
        })

    # Process and print the analysis for each driver
    for driver_code, stints_list in sorted(driver_stints.items()):
        pit_data = pit_stop_summary.get(driver_code, {'count': 0, 'laps': []})
        pit_count = pit_data['count']
        # Sort pit laps for consistent display
        pit_laps = ", ".join(map(str, sorted(pit_data['laps'])))
        
        pit_info_str = f" (Pit Stops: {pit_count}"
        if pit_laps:
            pit_info_str += f" ({pit_laps})"
        pit_info_str += ")"

        print(f"\n{Style.BOLD}{Style.MAGENTA}Driver: {driver_code}{pit_info_str}{Style.RESET}")
        print(f"  Total Stints: {len(stints_list)}")

        for stint_data in stints_list:
            stint_num = stint_data['stint_num']
            compound = stint_data['compound']
            lap_start = stint_data['lap_start']
            lap_end = stint_data['lap_end']
            tyre_age = stint_data['tyre_age']
            pit_info = stint_data['pit_info']
            
            display_lap_end = lap_end if lap_end is not None else lap_start # Fallback for lap_end
            laps_display = f"{lap_start}-{display_lap_end}"
            
            pit_display = ""
            if pit_info:
                # Handle potential NULL values for pit duration
                pit_duration_str = f"{pit_info['duration']:.3f}s" if pit_info['duration'] is not None else "N/A"
                pit_lap_str = str(pit_info['lap_num']) if 'lap_num' in pit_info and pit_info['lap_num'] is not None else "N/A"
                # The pit_info_for_stint dictionary already holds 'lap_num' from the pit_stops_by_lap key.
                pit_display = f" {Style.CYAN}[PIT: {pit_duration_str} on LAP: {lap_start}]{Style.RESET}"

            compound_display = "N/A" if not compound else (compound[:5] if len(compound) > 10 else compound)
            print(
                f"  - Stint {stint_num:<2} | "
                f"Compound: {compound_display:<7} | "
                f"Laps: {laps_display:<7} | "
                f"Tyre Age: {tyre_age:<2} laps | "
                f"{pit_display}"
            )


def main():
    """
    Main function to run the stint analysis.
    """
    if not os.path.exists(DB_FILE):
        log(f"Database file not found at '{DB_FILE}'.", 'ERROR')
        log("Please run the main data population script first.", 'INFO')
        sys.exit(1)

    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # --- Start Analysis ---
        meeting_name = "British Grand Prix"
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
