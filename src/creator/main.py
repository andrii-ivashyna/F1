# main.py

import sys
import time
from datetime import datetime
import config
import fetch_api
import parse_circuit
import parse_team
import parse_driver
from config import log, Style

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
                config.create_database()
                fetch_api.populate_database()
            elif command == 'wiki':
                parse_circuit.parse_circuit_wiki()
                parse_team.parse_team_wiki()
            elif command == 'f1':
                parse_circuit.parse_circuit_f1()
                parse_team.parse_team_f1()
                parse_driver.parse_driver_f1()
            elif command == 'circuit':
                parse_circuit.run_circuit_parsers()
            elif command == 'team':
                parse_team.run_team_parsers()
            elif command == 'driver':
                parse_driver.run_driver_parsers()
            elif command == 'circuit-wiki':
                parse_circuit.parse_circuit_wiki()
            elif command == 'circuit-f1':
                parse_circuit.parse_circuit_f1()
            elif command == 'team-wiki':
                parse_team.parse_team_wiki()
            elif command == 'team-f1':
                parse_team.parse_team_f1()
            elif command == 'driver-f1':
                parse_driver.parse_driver_f1()
            else:
                log(f"Unknown command: {command}", 'ERROR')
                log("See --help for available commands.", 'INFO')
                sys.exit(1)
        else:
            # Default: run full process
            start_time = time.time()
            start_datetime = datetime.now()
            
            log("Start Data Fetching and Enrichment Process", 'HEADING')
            config.create_database()
            fetch_api.populate_database()
            
            log("Parsing Data from External Sources", 'SUBHEADING')
            parse_circuit.run_circuit_parsers()
            parse_team.run_team_parsers()
            parse_driver.run_driver_parsers()

            # Process completion summary
            end_time = time.time()
            end_datetime = datetime.now()
            duration = end_time - start_time
            
            print()
            log(f"Process completed successfully!", 'SUCCESS')
            log(f"Start time: {Style.yellow(start_datetime.strftime('%H:%M:%S'))}", 'SUCCESS')
            log(f"End time: {Style.yellow(end_datetime.strftime('%H:%M:%S'))}", 'SUCCESS')
            log(f"Total duration: {Style.yellow(f'{duration:.1f}s')}", 'SUCCESS')
            print()
        
    except Exception as e:
        log(f"An unexpected error occurred during the process", 'ERROR', data={'error': str(e)})
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
