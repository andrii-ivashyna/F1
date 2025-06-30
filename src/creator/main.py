# main.py
"""
Main application runner for the Formula 1 data fetching and enrichment process.
Handles command-line arguments and orchestrates the data pipeline.
"""

import sys
import time
from datetime import datetime
import manager_parse
from config import log, show_completion_summary
from manager_db import create_database
from manager_api import populate_database

# --- IMPORTANT ---
# Before running, please install the required libraries:
# pip install requests beautifulsoup4 pandas pycountry lxml

def main():
    """
    Main function to run the full data fetching and enrichment process.
    Provides command-line arguments to run specific parts of the process.
    """
    try:
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            log(f"Running command: {command}", 'HEADING')
            
            if command == 'api':
                create_database()
                populate_database()
            elif command == 'wiki':
                manager_parse.parse_circuit_wiki()
                manager_parse.parse_team_wiki()
            elif command == 'f1':
                manager_parse.parse_circuit_f1()
                manager_parse.parse_team_f1()
                manager_parse.parse_driver_f1()
            elif command == 'circuit':
                manager_parse.run_circuit_parsers()
            elif command == 'team':
                manager_parse.run_team_parsers()
            elif command == 'driver':
                manager_parse.run_driver_parsers()
            elif command == 'circuit-wiki':
                manager_parse.parse_circuit_wiki()
            elif command == 'circuit-f1':
                manager_parse.parse_circuit_f1()
            elif command == 'team-wiki':
                manager_parse.parse_team_wiki()
            elif command == 'team-f1':
                manager_parse.parse_team_f1()
            elif command == 'driver-f1':
                manager_parse.parse_driver_f1()
            else:
                log(f"Unknown command: {command}", 'ERROR')
                log("See --help for available commands.", 'INFO')
                sys.exit(1)
        else:
            # Default: run full process
            start_time, start_datetime = time.time(), datetime.now()
            
            log("Start Data Fetching and Enrichment Process", 'HEADING')
            create_database()
            populate_database()
            manager_parse.run_all_parsers()
            show_completion_summary(start_time, start_datetime)
        
    except Exception as e:
        log(f"An unexpected error occurred during the process", 'ERROR', data={'error': str(e)})
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
