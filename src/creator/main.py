# main.py
"""
Main application runner for the Formula 1 data fetching and enrichment process
Handles command-line arguments and orchestrates the data pipeline
"""

# --- DEPENDENCIES ---
# Before running, please install the required libraries:
# pip install requests beautifulsoup4 pandas pycountry lxml

import sys
from datetime import datetime
import manager_parse
from config import log, show_completion_summary
from manager_db import create_database
from manager_api import populate_database

def main():
    """
    Main function to run the full data fetching and enrichment process
    Provides command-line arguments to run specific parts of the process
    """
    commands = {
        'api': [create_database, populate_database],
        'wiki': [manager_parse.parse_circuit_wiki, manager_parse.parse_team_wiki],
        'f1': [manager_parse.parse_circuit_f1, manager_parse.parse_team_f1, manager_parse.parse_driver_f1],
        'circuit': [manager_parse.run_circuit_parsers],
        'team': [manager_parse.run_team_parsers],
        'driver': [manager_parse.run_driver_parsers],
        'circuit-wiki': [manager_parse.parse_circuit_wiki],
        'circuit-f1': [manager_parse.parse_circuit_f1],
        'team-wiki': [manager_parse.parse_team_wiki],
        'team-f1': [manager_parse.parse_team_f1],
        'driver-f1': [manager_parse.parse_driver_f1],
    }

    try:
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            log(f"Running command: {command}", 'HEADING')

            if command in commands:
                for func in commands[command]:
                    func()
            else:
                log(f"Unknown command: {command}", 'ERROR')
                log("See --help for available commands.", 'INFO')
                sys.exit(1)
        else:
            # Default: run full process
            start_datetime = datetime.now()
            
            log("Start Data Fetching and Enrichment Process", 'HEADING')
            create_database()
            populate_database()
            manager_parse.run_all_parsers()
            show_completion_summary(start_datetime)
        
    except Exception as e:
        log(f"An unexpected error occurred during the process", 'ERROR', data={'error': str(e)})
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
